use super::pg::{MIGRATIONS, migrate};
use super::window;
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

// Rolling-window reads against real PostgreSQL with a fixed clock.
const WINDOW_MINT: &str = "WindowMint1111111111111111111111111111111";
const NOW: i64 = 1_800_000_000;
const CUTOFF: i64 = NOW - window::RETENTION_SECONDS;

fn windowed_transfer(
    signature: &str,
    slot: u64,
    outer: usize,
    inner: Option<usize>,
    block_time: Option<i64>,
    amount: Option<u64>,
    fee: Option<u64>,
) -> Transfer {
    Transfer {
        transfer_id: format!("{signature}:{outer}:{inner:?}"),
        signature: signature.into(),
        slot,
        block_time,
        instruction_index: outer,
        inner_instruction_index: inner,
        mint: WINDOW_MINT.into(),
        token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into(),
        source_token_account: "source".into(),
        destination_token_account: "destination".into(),
        source_owner: None,
        destination_owner: None,
        amount_raw: amount,
        decimals: Some(6),
        fee_raw: fee,
        confidential: false,
        decoder_version: DECODER_VERSION.into(),
    }
}

/// Inserts rows as the write path stores them: record JSON plus the SQL columns.
fn insert_rows(db: &Database, rows: &[Transfer]) {
    Store::open(db)
        .unwrap()
        .ensure_partitions(rows.iter().map(|t| t.slot))
        .unwrap();
    let mut client = db.test_client();
    for t in rows {
        client
            .execute(
                "INSERT INTO transfers(transfer_id,signature,slot,outer_index,inner_index,mint,record_json,block_time,amount_raw,fee_raw)
                 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::text::numeric,$10::text::numeric)",
                &[
                    &t.transfer_id,
                    &t.signature,
                    &(t.slot as i64),
                    &(t.instruction_index as i32),
                    &(t.inner_instruction_index.map_or(-1, |n| n as i32)),
                    &t.mint,
                    &serde_json::to_string(t).unwrap(),
                    &t.block_time,
                    &t.amount_raw.map(|n| n.to_string()),
                    &t.fee_raw.map(|n| n.to_string()),
                ],
            )
            .unwrap();
    }
}

fn insert_history(db: &Database, slots: &[(i64, &str)]) {
    let mut client = db.test_client();
    for &(slot, status) in slots {
        client
            .execute(
                "INSERT INTO historical_slots(mint,decoder,slot,status,error_class,first_pending,attempts,block_time)
                 VALUES($1,$2,$3,$4,NULL,0,0,NULL)",
                &[&WINDOW_MINT, &DECODER_VERSION, &slot, &status],
            )
            .unwrap();
    }
}

fn rolling() -> window::Window {
    window::Window::resolve(NOW, None, None).unwrap()
}

fn listed_ids(page: &serde_json::Value) -> Vec<String> {
    page["transfers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["transfer_id"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn window_resolution_is_computed_from_the_injected_clock_and_bounded() {
    let rolling = window::Window::resolve(NOW, None, None).unwrap();
    assert_eq!((rolling.from, rolling.to), (CUTOFF, NOW));
    let short = window::Window::resolve(NOW, Some(60), None).unwrap();
    assert_eq!((short.from, short.to), (NOW - 60, NOW));
    let explicit = window::Window::resolve(NOW, None, Some((CUTOFF, NOW))).unwrap();
    assert_eq!(explicit.from, CUTOFF);
    for (seconds, range) in [
        (Some(0), None),
        (Some(window::RETENTION_SECONDS + 1), None),
        (Some(60), Some((1, 2))),
        (None, Some((5, 5))),
        (None, Some((CUTOFF - 1, NOW))),
    ] {
        assert!(window::Window::resolve(NOW, seconds, range).is_err());
    }
}

#[test]
fn rolling_cutoff_includes_exact_boundary_and_excludes_earlier_rows() {
    let db = pg::TestDatabase::new();
    insert_rows(
        &db.0,
        &[
            windowed_transfer("before", 10, 0, None, Some(CUTOFF - 1), Some(1), Some(0)),
            windowed_transfer("at-cutoff", 11, 0, None, Some(CUTOFF), Some(2), Some(0)),
            windowed_transfer("inside", 12, 0, None, Some(NOW - 1), Some(3), Some(0)),
        ],
    );
    let page = window::listing(&db.0, Some(WINDOW_MINT), rolling(), false, 1000, None).unwrap();
    assert_eq!(page["cutoff"], CUTOFF);
    assert_eq!(page["now"], NOW);
    assert_eq!(
        page["window"],
        serde_json::json!({"from": CUTOFF, "to": NOW})
    );
    assert_eq!(page["null_block_time"], 0);
    let signatures: Vec<&str> = page["transfers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["signature"].as_str().unwrap())
        .collect();
    assert_eq!(signatures, ["at-cutoff", "inside"]);
    let summary = window::window_summary(&db.0, WINDOW_MINT, rolling()).unwrap();
    assert_eq!(summary["transfer_count"], 2);
    assert_eq!(summary["known_volume_raw"], "5");
}

#[test]
fn null_block_time_is_counted_never_listed_or_summed_in_a_window() {
    let db = pg::TestDatabase::new();
    insert_rows(
        &db.0,
        &[
            windowed_transfer("timed", 20, 0, None, Some(NOW - 5), Some(7), Some(1)),
            windowed_transfer("untimed-amount", 21, 0, None, None, Some(u64::MAX), None),
            windowed_transfer("untimed-empty", 22, 0, None, None, None, None),
        ],
    );
    let window = rolling();
    let page = window::listing(&db.0, Some(WINDOW_MINT), window, true, 1000, None).unwrap();
    assert_eq!(listed_ids(&page), ["timed:0:None"]);
    assert_eq!(page["null_block_time"], 2);
    let summary = window::window_summary(&db.0, WINDOW_MINT, window).unwrap();
    assert_eq!(summary["transfer_count"], 1);
    assert_eq!(summary["known_volume_raw"], "7");
    assert_eq!(summary["known_fees_raw"], "1");
    assert_eq!(summary["null_block_time"], 2);
    assert_eq!(summary["aggregate_scope"], "known_block_time_in_window");
    // All-stored diagnostics keep unknown-time rows, but NULL stays unknown, never zero.
    let stored = Store::summary(&db.0, WINDOW_MINT).unwrap();
    assert_eq!(stored["transfer_count"], 3);
    assert_eq!(stored["unknown_amount_count"], 1);
    assert_eq!(stored["unknown_fee_count"], 2);
    assert_eq!(
        stored["known_volume_raw"],
        (u128::from(u64::MAX) + 7).to_string()
    );
}

#[test]
fn exact_sums_exceed_u64_and_hidden_values_stay_unknown() {
    let db = pg::TestDatabase::new();
    let mut confidential = windowed_transfer("hidden", 40, 0, None, Some(NOW - 2), None, None);
    confidential.confidential = true;
    insert_rows(
        &db.0,
        &[
            windowed_transfer(
                "max-a",
                41,
                0,
                None,
                Some(NOW - 4),
                Some(u64::MAX),
                Some(u64::MAX),
            ),
            windowed_transfer(
                "max-b",
                42,
                0,
                None,
                Some(NOW - 3),
                Some(u64::MAX),
                Some(u64::MAX),
            ),
            windowed_transfer("max-c", 43, 0, None, Some(NOW - 1), Some(u64::MAX), None),
            confidential,
        ],
    );
    let summary = window::window_summary(&db.0, WINDOW_MINT, rolling()).unwrap();
    assert_eq!(summary["transfer_count"], 4);
    assert_eq!(
        summary["known_volume_raw"],
        (u128::from(u64::MAX) * 3).to_string()
    );
    assert_eq!(
        summary["known_fees_raw"],
        (u128::from(u64::MAX) * 2).to_string()
    );
    assert_eq!(summary["unknown_amount_count"], 1);
    assert_eq!(summary["unknown_fee_count"], 2);
    assert_eq!(summary["confidential_transfer_count"], 1);
}

#[test]
fn pages_are_keyset_ordered_across_ties_and_stable_across_inserts() {
    let db = pg::TestDatabase::new();
    let block_time = Some(NOW - 10);
    insert_rows(
        &db.0,
        &[
            windowed_transfer("tie", 30, 0, None, block_time, Some(1), None),
            windowed_transfer("tie", 30, 1, Some(0), block_time, Some(1), None),
            windowed_transfer("tie", 30, 1, Some(1), block_time, Some(1), None),
            windowed_transfer("tie", 30, 1, Some(2), block_time, Some(1), None),
            windowed_transfer("tie", 30, 2, None, block_time, Some(1), None),
            windowed_transfer("other", 31, 0, None, block_time, Some(1), None),
        ],
    );
    let window = rolling();
    let all = |recent| {
        listed_ids(&window::listing(&db.0, Some(WINDOW_MINT), window, recent, 1000, None).unwrap())
    };
    let ascending = all(false);
    assert_eq!(
        ascending,
        [
            "tie:0:None",
            "tie:1:Some(0)",
            "tie:1:Some(1)",
            "tie:1:Some(2)",
            "tie:2:None",
            "other:0:None"
        ]
    );
    let mut descending = all(true);
    descending.reverse();
    assert_eq!(descending, ascending);

    // Walk two rows at a time; a page cursor must survive inserts before and after it.
    let first = window::listing(&db.0, Some(WINDOW_MINT), window, false, 2, None).unwrap();
    assert_eq!(listed_ids(&first), ["tie:0:None", "tie:1:Some(0)"]);
    let cursor = first["next_cursor"].as_str().unwrap().to_owned();
    insert_rows(
        &db.0,
        &[
            windowed_transfer("early", 5, 0, None, block_time, Some(1), None),
            windowed_transfer("tie", 30, 1, Some(3), block_time, Some(1), None),
            windowed_transfer("late", 40, 0, None, block_time, Some(1), None),
        ],
    );
    let mut continued = Vec::new();
    let mut next = Some(cursor);
    while let Some(cursor) = next {
        let page =
            window::listing(&db.0, Some(WINDOW_MINT), window, false, 2, Some(&cursor)).unwrap();
        continued.extend(listed_ids(&page));
        next = page["next_cursor"].as_str().map(str::to_owned);
    }
    assert_eq!(
        continued,
        [
            "tie:1:Some(1)",
            "tie:1:Some(2)",
            "tie:1:Some(3)",
            "tie:2:None",
            "other:0:None",
            "late:0:None"
        ]
    );
    assert!(window::listing(&db.0, Some(WINDOW_MINT), window, false, 1, Some("zz")).is_err());
    assert!(window::listing(&db.0, Some(WINDOW_MINT), window, false, 0, None).is_err());
    assert!(window::listing(&db.0, Some(WINDOW_MINT), window, false, 1001, None).is_err());
}

#[test]
fn coverage_reports_complete_partial_no_evidence_and_unknown_windows() {
    let db = pg::TestDatabase::new();
    let block_time = Some(NOW - 10);
    insert_rows(
        &db.0,
        &[
            windowed_transfer("a", 100, 0, None, block_time, Some(1), None),
            windowed_transfer("b", 103, 0, None, block_time, Some(1), None),
        ],
    );
    let window = rolling();
    let coverage =
        |mint| window::listing(&db.0, mint, window, false, 1000, None).unwrap()["coverage"].clone();

    // No historical evidence for the transfer span: unknown, not complete.
    let none = coverage(Some(WINDOW_MINT));
    assert_eq!(none["status"], "unknown");
    assert_eq!(none["reason"], "no_coverage_evidence");
    assert_eq!(
        none["slot_range"],
        serde_json::json!({"from": 100, "to": 103})
    );

    insert_history(
        &db.0,
        &[
            (100, "complete"),
            (101, "pending"),
            (102, "complete"),
            (103, "failed"),
        ],
    );
    let partial = coverage(Some(WINDOW_MINT));
    assert_eq!(partial["status"], "partial");
    assert_eq!(
        partial["covered_ranges"],
        serde_json::json!([{"from": 100, "to": 100}, {"from": 102, "to": 102}])
    );
    assert_eq!(
        partial["missing_ranges"],
        serde_json::json!([{"from": 101, "to": 101}, {"from": 103, "to": 103}])
    );
    assert_eq!(partial["slot_status_counts"]["complete"], 2);
    assert_eq!(partial["slot_status_counts"]["pending"], 1);
    assert_eq!(partial["slot_status_counts"]["failed"], 1);
    assert_eq!(partial["missing_range_count"], 2);
    assert_eq!(partial["missing_ranges_truncated"], false);

    db.0.test_client()
        .execute(
            "UPDATE historical_slots SET status='complete' WHERE slot IN (101,103)",
            &[],
        )
        .unwrap();
    let complete = coverage(Some(WINDOW_MINT));
    assert_eq!(complete["status"], "complete");
    assert_eq!(complete["missing_range_count"], 0);
    assert_eq!(
        complete["covered_ranges"],
        serde_json::json!([{"from": 100, "to": 103}])
    );
    assert_eq!(complete["slot_status_counts"]["unknown"], 0);

    // Mint-less listings have no per-mint coverage; a window without known times is unknown.
    assert_eq!(coverage(None)["reason"], "mint_required");
    let empty = window::listing(
        &db.0,
        Some(WINDOW_MINT),
        window::Window::resolve(NOW, Some(5), None).unwrap(),
        false,
        1000,
        None,
    )
    .unwrap();
    assert_eq!(empty["coverage"]["status"], "unknown");
    assert_eq!(
        empty["coverage"]["reason"],
        "no_known_time_transfers_in_window"
    );
}

#[test]
fn coverage_gap_list_is_bounded_while_counts_stay_exact() {
    let db = pg::TestDatabase::new();
    let block_time = Some(NOW - 10);
    insert_rows(
        &db.0,
        &[
            windowed_transfer("start", 1000, 0, None, block_time, Some(1), None),
            windowed_transfer("end", 1300, 0, None, block_time, Some(1), None),
        ],
    );
    // Even slots complete, odd slots pending: 150 single-slot gaps.
    let slots: Vec<(i64, &str)> = (1000..=1300)
        .map(|slot| (slot, if slot % 2 == 0 { "complete" } else { "pending" }))
        .collect();
    insert_history(&db.0, &slots);
    let page = window::listing(&db.0, Some(WINDOW_MINT), rolling(), false, 1000, None).unwrap();
    let coverage = &page["coverage"];
    assert_eq!(coverage["missing_range_count"], 150);
    assert_eq!(coverage["missing_ranges"].as_array().unwrap().len(), 100);
    assert_eq!(coverage["missing_ranges_truncated"], true);
    assert_eq!(coverage["covered_range_count"], 151);
}
