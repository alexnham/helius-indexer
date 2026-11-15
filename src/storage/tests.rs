use super::pg::{MIGRATIONS, migrate};
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

type Columns = Vec<(String, Option<i64>, Option<String>, Option<String>)>;

fn fixture() -> serde_json::Value {
    serde_json::from_slice(include_bytes!("../../tests/fixtures/mainnet-transfer.json")).unwrap()
}

fn decoded(value: &serde_json::Value) -> (Vec<u8>, crate::model::Capture, Vec<Transfer>) {
    let raw = serde_json::to_vec(value).unwrap();
    let capture = crate::decoder::parse_capture(&raw).unwrap();
    let rows = crate::decoder::decode(&capture).unwrap();
    (raw, capture, rows)
}

/// Block time and amount columns as SQL sees them; amounts are exact text.
fn columns(db: &Database) -> Columns {
    let mut rows: Columns = db
        .test_client()
        .query(
            "SELECT transfer_id,block_time,amount_raw::text,fee_raw::text FROM transfers",
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    rows.sort();
    rows
}

fn expected_columns(rows: &[Transfer]) -> Columns {
    let mut out: Columns = rows
        .iter()
        .map(|t| {
            (
                t.transfer_id.clone(),
                t.block_time,
                t.amount_raw.map(|n| n.to_string()),
                t.fee_raw.map(|n| n.to_string()),
            )
        })
        .collect();
    out.sort();
    out
}

#[test]
fn migration_0008_backfills_block_time_and_exact_amounts_on_existing_rows() {
    let db = pg::TestDatabase::new();
    let mut sql = db.sql();
    migrate(&mut sql, &pg::MIGRATIONS[..5]).unwrap();
    let (_, _, rows) = decoded(&fixture());
    let mut known = rows[0].clone();
    known.block_time = Some(1_794_430_637);
    known.amount_raw = Some(u64::MAX);
    known.fee_raw = Some(0);
    let mut unknown = known.clone();
    unknown.transfer_id.push_str(":unknown");
    unknown.block_time = None;
    unknown.amount_raw = None;
    unknown.fee_raw = None;
    // Version 5 rows have no new columns; the partition is created as the writer would.
    let index = known.slot as i64 / 432_000;
    sql.batch_execute(&format!(
        "CREATE TABLE transfers_p{index} PARTITION OF transfers FOR VALUES FROM ({}) TO ({})",
        index * 432_000,
        (index + 1) * 432_000
    ))
    .unwrap();
    for row in [&known, &unknown] {
        sql.execute(
            "INSERT INTO transfers(transfer_id,signature,slot,outer_index,inner_index,mint,record_json) VALUES($1,$2,$3,$4,$5,$6,$7)",
            &[
                &row.transfer_id,
                &row.signature,
                &(row.slot as i64),
                &(row.instruction_index as i32),
                &row.inner_instruction_index.map_or(-1, |n| n as i32),
                &row.mint,
                &serde_json::to_string(row).unwrap(),
            ],
        )
        .unwrap();
    }
    assert_eq!(
        migrate(&mut sql, MIGRATIONS).unwrap(),
        MIGRATIONS.len() as i32
    );
    assert_eq!(
        columns(&db.0),
        expected_columns(&[known.clone(), unknown.clone()])
    );
    assert_eq!(
        Store::query(&db.0, None, 100).unwrap(),
        vec![known, unknown]
    );
}

#[test]
fn exact_amounts_round_trip_and_hidden_values_stay_null() {
    let db = pg::TestDatabase::new();
    let (raw, capture, mut rows) = decoded(&fixture());
    rows[0].amount_raw = Some(u64::MAX);
    rows[0].fee_raw = Some(u64::MAX);
    // A confidential transfer has no visible amount or fee; NULL, never zero.
    rows[1].confidential = true;
    rows[1].amount_raw = None;
    rows[1].fee_raw = None;
    Store::open(&db.0)
        .unwrap()
        .persist(&capture, &raw, &Ok(rows.clone()))
        .unwrap();
    assert_eq!(Store::query(&db.0, None, 100).unwrap(), rows);
    assert_eq!(columns(&db.0), expected_columns(&rows));
    let hidden =
        db.0.test_client()
            .query_one(
                "SELECT amount_raw::text,fee_raw::text FROM transfers WHERE transfer_id=$1",
                &[&rows[1].transfer_id],
            )
            .map(|r| (r.get::<_, Option<String>>(0), r.get::<_, Option<String>>(1)))
            .unwrap();
    assert_eq!(hidden, (None, None));
}

#[test]
fn enrichment_sets_block_time_column_and_repeats_stay_deduplicated() {
    let db = pg::TestDatabase::new();
    let mut sparse = fixture();
    sparse["result"]["blockTime"] = serde_json::Value::Null;
    let (raw, capture, rows) = decoded(&sparse);
    Store::open(&db.0)
        .unwrap()
        .persist(&capture, &raw, &Ok(rows.clone()))
        .unwrap();
    assert_eq!(columns(&db.0), expected_columns(&rows));
    assert!(columns(&db.0).iter().all(|c| c.1.is_none()));

    let (raw, capture, full) = decoded(&fixture());
    for _ in 0..2 {
        // Enrichment rewrites the stored record and its column; a repeat inserts nothing.
        assert_eq!(
            Store::open(&db.0)
                .unwrap()
                .persist(&capture, &raw, &Ok(full.clone()))
                .unwrap(),
            0
        );
    }
    assert_eq!(full.len(), 2);
    assert!(full.iter().all(|t| t.block_time == Some(1_794_430_637)));
    assert_eq!(columns(&db.0), expected_columns(&full));
    assert_eq!(Store::query(&db.0, None, 100).unwrap(), full);
}
