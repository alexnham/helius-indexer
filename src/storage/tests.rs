use super::*;
use std::sync::{Arc, Barrier};

#[test]
fn concurrent_parent_child_conflict_is_rejected_before_both_anchors_commit() {
    let db = pg::TestDatabase::new();
    Store::open(&db.0).unwrap();
    db.0.test_client().batch_execute("CREATE FUNCTION delay_anchor() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_sleep(0.2); RETURN NEW; END$$; CREATE TRIGGER delay_anchor BEFORE INSERT ON historical_block_identity FOR EACH ROW EXECUTE FUNCTION delay_anchor();").unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let handles:Vec<_>=(0..2).map(|index| {
        let database=db.0.clone();let barrier=Arc::clone(&barrier);
        std::thread::spawn(move || {
            let mut store=Store::open(&database).unwrap();
            let slot=10+index;
            let block=serde_json::json!({"blockhash":bs58::encode([slot as u8;32]).into_string(),"parentSlot":slot-1,
                "previousBlockhash":bs58::encode([if index==1 {99}else{9};32]).into_string(),"blockTime":1});
            barrier.wait();
            store.observe_block_identity(slot,&block,&[])
        })
    }).collect();
    barrier.wait();
    let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(outcomes.iter().filter(|r| r.is_ok()).count(), 1);
    assert!(
        outcomes
            .iter()
            .any(|r| r.as_ref().is_err_and(|e| e.0 == "block_parent_conflict"))
    );
    let mut sql = db.0.test_client();
    let inconsistent:i64=sql.query_one("SELECT COUNT(*) FROM historical_block_identity c JOIN historical_block_identity p ON c.parent_slot=p.slot WHERE c.previous_blockhash<>p.blockhash",&[]).unwrap().get(0);
    assert_eq!(inconsistent, 0);
}
