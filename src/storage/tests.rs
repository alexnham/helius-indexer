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

#[test]
fn sampled_dataset_is_stable_on_replay_and_repair_and_retains_inputs() {
    let db = pg::TestDatabase::new();
    let path = &db.0;
    let fixture: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../tests/fixtures/mainnet-transfer.json"))
            .unwrap();
    let capture = crate::decoder::parse_capture(&serde_json::to_vec(&fixture).unwrap()).unwrap();
    let mint = crate::decoder::decode(&capture).unwrap()[0].mint.clone();
    assert_eq!(
        Store::init_sample(path, &mint, 10).unwrap()["numerator"],
        10
    );
    assert!(Store::init_sample(path, &mint, 10).is_ok());
    assert!(Store::init_sample(path, &mint, 20).is_err());
    let policy = Sampling {
        mint: Some(mint.clone()),
        percent: 10,
    };
    let mut selected = Vec::new();
    let mut inputs = Vec::new();
    for index in (0..40u8).rev() {
        let mut value = fixture.clone();
        value["result"]["transaction"]["signatures"][0] =
            bs58::encode([index; 64]).into_string().into();
        let raw = serde_json::to_vec(&value).unwrap();
        let capture = crate::decoder::parse_capture(&raw).unwrap();
        let decoded = crate::decoder::decode(&capture).unwrap();
        selected.extend(decoded.iter().filter(|row| policy.accepts(row)).cloned());
        inputs.push((capture, raw, decoded));
    }
    assert!(!selected.is_empty());
    assert!(selected.len() < inputs.len() * 2);
    for (capture, raw, decoded) in &inputs {
        let mut store = Store::open(path).unwrap();
        store.persist(capture, raw, &Ok(decoded.clone())).unwrap();
        assert_eq!(
            store.persist(capture, raw, &Ok(decoded.clone())).unwrap(),
            0
        );
    }
    let rows = Store::query(path, Some(&mint), 1000).unwrap();
    let mut ids: Vec<_> = rows.iter().map(|r| &r.transfer_id).collect();
    let mut expected: Vec<_> = selected.iter().map(|r| &r.transfer_id).collect();
    ids.sort();
    expected.sort();
    assert_eq!(ids, expected);
    assert_eq!(
        Store::summary(path, &mint).unwrap()["sampling"]["policy"],
        "sha256-transfer-id-v1"
    );
    assert_eq!(
        Store::prune(path, 1).unwrap_err().0,
        "sample_prune_not_supported"
    );
    let mut sql = path.test_client();
    assert_eq!(
        sql.query_one("SELECT COUNT(*) FROM captures", &[])
            .unwrap()
            .get::<_, i64>(0),
        40
    );
    // Seed an earlier revision's errors: a missing, a changed and an unselected row.
    sql.execute(
        "UPDATE captures SET decoder_version='token-transfers-v1'",
        &[],
    )
    .unwrap();
    sql.execute(
        "DELETE FROM transfers WHERE transfer_id=$1",
        &[&selected[0].transfer_id],
    )
    .unwrap();
    sql.execute("UPDATE transfers SET record_json=jsonb_set(record_json::jsonb,'{amount_raw}',to_jsonb('999'::text))::text WHERE transfer_id=$1", &[&selected[1].transfer_id]).unwrap();
    let excluded = inputs
        .iter()
        .flat_map(|(_, _, decoded)| decoded)
        .find(|row| !policy.accepts(row))
        .unwrap();
    sql.execute(
        "INSERT INTO transfers VALUES($1,$2,$3,$4,$5,$6,$7)",
        &[
            &excluded.transfer_id,
            &excluded.signature,
            &(excluded.slot as i64),
            &(excluded.instruction_index as i32),
            &excluded.inner_instruction_index.map_or(-1, |n| n as i32),
            &excluded.mint,
            &serde_json::to_string(excluded).unwrap(),
        ],
    )
    .unwrap();
    for (capture, _, _) in &inputs {
        Store::open(path)
            .unwrap()
            .correct(&capture.result.transaction.signatures[0])
            .unwrap();
    }
    let mut repaired = Store::query(path, Some(&mint), 1000).unwrap();
    repaired.sort_by(|a, b| a.transfer_id.cmp(&b.transfer_id));
    selected.sort_by(|a, b| a.transfer_id.cmp(&b.transfer_id));
    assert_eq!(repaired, selected);
    assert!(Store::init_sample(path, &bs58::encode([5u8; 32]).into_string(), 10).is_err());
}

#[test]
fn sampled_activation_rejects_existing_exact_inputs() {
    let db = pg::TestDatabase::new();
    let path = &db.0;
    let raw = include_bytes!("../../tests/fixtures/mainnet-transfer.json");
    let capture = crate::decoder::parse_capture(raw).unwrap();
    let decoded = crate::decoder::decode(&capture);
    let mint = decoded.as_ref().unwrap()[0].mint.clone();
    Store::open(path)
        .unwrap()
        .persist(&capture, raw, &decoded)
        .unwrap();
    assert_eq!(
        Store::init_sample(path, &mint, 10).unwrap_err().0,
        "sample_session_not_empty"
    );
    assert_eq!(Store::sampling(path).unwrap()["policy"], "exact");
}

#[test]
fn activation_waits_for_inflight_input_and_rejects_exact_dataset() {
    let db = pg::TestDatabase::new();
    let path = &db.0;
    Store::open(path).unwrap();
    let mut writer = path.test_client();
    let mut tx = writer.transaction().unwrap();
    tx.execute(
        "INSERT INTO captures VALUES('test',1,'{}',$1,'decoded','test')",
        &[&b"{}".as_slice()],
    )
    .unwrap();
    let path2 = path.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        sender.send(()).unwrap();
        Store::init_sample(&path2, &bs58::encode([3u8; 32]).into_string(), 10)
    });
    receiver.recv().unwrap();
    // Commit only once activation is blocked on the writer's policy lock.
    let mut observer = path.test_client();
    while observer.query_one("SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%dataset_policy%FOR UPDATE%'", &[]).unwrap().get::<_, i64>(0) == 0 {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    tx.commit().unwrap();
    assert_eq!(
        handle.join().unwrap().unwrap_err().0,
        "sample_session_not_empty"
    );
    assert_eq!(Store::sampling(path).unwrap()["policy"], "exact");
}

#[test]
fn sample_percent_sets_the_rate_keeps_all_at_100_and_nests_smaller_samples() {
    let raw = include_bytes!("../../tests/fixtures/mainnet-transfer.json");
    let template =
        crate::decoder::decode(&crate::decoder::parse_capture(raw).unwrap()).unwrap()[0].clone();
    let rows: Vec<_> = (0..20_000)
        .map(|n| Transfer {
            transfer_id: format!("{}:{n}", template.transfer_id),
            ..template.clone()
        })
        .collect();
    let policy = |percent| Sampling {
        mint: Some(template.mint.clone()),
        percent,
    };
    for percent in [1, 10, 50, 100] {
        let kept = rows.iter().filter(|r| policy(percent).accepts(r)).count();
        let expected = rows.len() * percent as usize / 100;
        assert!(kept.abs_diff(expected) <= 400, "{percent}%: kept {kept}");
    }
    assert!(rows.iter().all(|r| policy(100).accepts(r)));
    assert!(
        rows.iter()
            .filter(|r| policy(10).accepts(r))
            .all(|r| policy(50).accepts(r))
    );
    let other_mint = Transfer {
        mint: "other".into(),
        ..template.clone()
    };
    assert!(!policy(100).accepts(&other_mint));
}
