#[cfg(feature = "archive")]
pub(crate) mod bridge;
pub(crate) mod history;
pub(crate) mod live;
pub mod pg;
pub(crate) mod repair;
use crate::{
    Error, Result,
    decoder::DECODER_VERSION,
    model::{Capture, Transfer},
    reconcile,
};
pub use pg::Database;
use postgres::{Client, GenericClient, Transaction};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};

/// Transfers are range-partitioned by slot; one partition spans one epoch of slots.
const PARTITION_SLOTS: i64 = 432_000;

pub struct Store {
    db: Client,
    /// Partitions this connection has confirmed; partitions are never dropped.
    partitions: HashSet<i64>,
}

fn write_failed(_: postgres::Error) -> Error {
    Error("storage_write_failed")
}

fn read_failed(_: postgres::Error) -> Error {
    Error("storage_read_failed")
}

impl Store {
    pub fn open(db: &Database) -> Result<Self> {
        Ok(Self {
            db: db.connect()?,
            partitions: HashSet::new(),
        })
    }

    /// Creates and migrates the session schema, reporting the schema version.
    pub fn migrate(db: &Database) -> Result<serde_json::Value> {
        db.connect()?;
        Ok(
            serde_json::json!({"session":db.session(),"schema_version":pg::MIGRATIONS.last().map_or(0, |m| m.0)}),
        )
    }

    /// Creates missing transfer partitions outside the caller's write transaction, so
    /// concurrent writers serialize only on the first creation of a partition.
    pub(crate) fn ensure_partitions(&mut self, slots: impl IntoIterator<Item = u64>) -> Result<()> {
        for slot in slots {
            let index =
                i64::try_from(slot).map_err(|_| Error("unsupported_slot"))? / PARTITION_SLOTS;
            if self.partitions.contains(&index) {
                continue;
            }
            let mut tx = self.db.transaction().map_err(write_failed)?;
            tx.execute(
                "SELECT pg_advisory_xact_lock(731194600, $1::int)",
                &[&(index as i32)],
            )
            .map_err(write_failed)?;
            tx.batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS transfers_p{index} PARTITION OF transfers FOR VALUES FROM ({}) TO ({})",
                index * PARTITION_SLOTS,
                (index + 1) * PARTITION_SLOTS
            ))
            .map_err(write_failed)?;
            tx.commit().map_err(|_| Error("storage_commit_failed"))?;
            self.partitions.insert(index);
        }
        Ok(())
    }

    /// Stores a complete transaction's output atomically. Replay of identical input is a no-op.
    ///
    /// READ COMMITTED: a first observation inserts optimistically without reading; a repeat
    /// waits on the existing capture's row lock, so concurrent duplicates reconcile in turn.
    pub fn persist(
        &mut self,
        capture: &Capture,
        raw: &[u8],
        decoded: &Result<Vec<Transfer>>,
    ) -> Result<usize> {
        let signature = capture
            .result
            .transaction
            .signatures
            .first()
            .ok_or(Error("missing_signature"))?;
        let value: serde_json::Value =
            serde_json::from_slice(raw).map_err(|_| Error("invalid_json"))?;
        let result_json =
            serde_json::to_string(&value["result"]).map_err(|_| Error("invalid_json"))?;
        let digest = Sha256::digest(result_json.as_bytes()).to_vec();
        let slot = i64::try_from(capture.result.slot).map_err(|_| Error("unsupported_slot"))?;
        // Every transfer of a capture carries the capture's slot, including repaired ones.
        self.ensure_partitions([capture.result.slot])?;
        let status = match decoded {
            Ok(_) if !capture.result.meta.err.is_null() => "failed_transaction",
            Ok(_) => "decoded",
            Err(e) => e.0,
        };
        let mut transaction = self.db.transaction().map_err(write_failed)?;
        let first = transaction
            .execute(
                "INSERT INTO captures(signature,slot,result_json,capture_json,status,decoder_version) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(signature) DO NOTHING",
                &[signature, &slot, &result_json, &raw, &status, &DECODER_VERSION],
            )
            .map_err(write_failed)?
            == 1;
        let observe = "INSERT INTO capture_inputs(signature,source,result_digest,slot,result_json,capture_json,status) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(signature,source,result_digest) DO NOTHING";
        let receipt = "UPDATE capture_inputs SET status=$1 WHERE signature=$2 AND source=$3 AND result_digest=$4";
        if first {
            transaction
                .execute(
                    observe,
                    &[
                        signature,
                        &capture.source,
                        &digest,
                        &slot,
                        &result_json,
                        &raw,
                        &status,
                    ],
                )
                .map_err(write_failed)?;
            transaction
                .execute(receipt, &[&status, signature, &capture.source, &digest])
                .map_err(write_failed)?;
            let inserted = insert_transfers(&mut transaction, signature, decoded.as_ref().ok())?;
            transaction
                .commit()
                .map_err(|_| Error("storage_commit_failed"))?;
            return Ok(inserted);
        }
        transaction
            .execute(
                observe,
                &[
                    signature,
                    &capture.source,
                    &digest,
                    &slot,
                    &result_json,
                    &raw,
                    &"received",
                ],
            )
            .map_err(write_failed)?;
        let row = transaction
            .query_one(
                "SELECT result_json,decoder_version,status FROM captures WHERE signature=$1 FOR UPDATE",
                &[signature],
            )
            .map_err(read_failed)?;
        let (prior, version, prior_status): (String, String, String) =
            (row.get(0), row.get(1), row.get(2));
        let prior_value: serde_json::Value =
            serde_json::from_str(&prior).map_err(|_| Error("stored_record_invalid"))?;
        let mut repaired = None;
        let mut outcome = if version != DECODER_VERSION {
            Err(Error("decoder_revision_conflict"))
        } else {
            reconcile::compatible_results(&prior_value, &value["result"])
        };
        let mut reference = None;
        if outcome.is_ok() {
            outcome = (|| {
                let mut combined = reconcile::reference(&prior_value, &value["result"])?;
                if decoded.is_err() && !prior_value["meta"]["logMessages"].is_null() {
                    combined["meta"]["logMessages"] = prior_value["meta"]["logMessages"].clone();
                }
                let mut envelope = value.clone();
                envelope["result"] = combined.clone();
                let raw =
                    serde_json::to_vec(&envelope).map_err(|_| Error("serialization_failed"))?;
                let combined_capture = crate::decoder::parse_capture(&raw)?;
                if decoded.is_ok() {
                    repaired = Some(crate::decoder::decode(&combined_capture)?);
                }
                reference = Some(
                    serde_json::to_string(&combined).map_err(|_| Error("serialization_failed"))?,
                );
                Ok(())
            })();
        }
        let mut updates = Vec::new();
        if outcome.is_ok()
            && prior_status == "decoded"
            && let Some(incoming) = repaired.as_ref()
        {
            let old = transaction
                .query(
                    "SELECT transfer_id,record_json FROM transfers WHERE signature=$1",
                    &[signature],
                )
                .map_err(read_failed)?
                .iter()
                .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
                .collect::<BTreeMap<_, _>>();
            if old.len() != incoming.len() {
                outcome = Err(Error("capture_conflict"));
            }
            for new in incoming {
                let Some(old) = old.get(&new.transfer_id) else {
                    outcome = Err(Error("capture_conflict"));
                    break;
                };
                let old: Transfer =
                    serde_json::from_str(old).map_err(|_| Error("stored_record_invalid"))?;
                match reconcile::transfer(&old, new) {
                    Ok(merged) => updates.push(merged),
                    Err(e) => {
                        outcome = Err(e);
                        break;
                    }
                }
            }
        }
        let receipt_status = outcome.as_ref().map_or_else(|e| e.0, |_| status);
        transaction
            .execute(
                receipt,
                &[&receipt_status, signature, &capture.source, &digest],
            )
            .map_err(write_failed)?;
        if let Err(error) = outcome {
            // Quarantine is durable, but existing logical query results remain unchanged.
            transaction
                .commit()
                .map_err(|_| Error("storage_commit_failed"))?;
            return Err(error);
        }
        transaction
            .execute(
                "UPDATE captures SET result_json=$1 WHERE signature=$2",
                &[&reference, signature],
            )
            .map_err(write_failed)?;
        if prior_status == "decoded" || prior_status == "failed_transaction" {
            for merged in updates {
                let text =
                    serde_json::to_string(&merged).map_err(|_| Error("serialization_failed"))?;
                transaction
                    .execute(
                        "UPDATE transfers SET record_json=$1 WHERE signature=$2 AND transfer_id=$3",
                        &[&text, signature, &merged.transfer_id],
                    )
                    .map_err(write_failed)?;
            }
            transaction
                .commit()
                .map_err(|_| Error("storage_commit_failed"))?;
            return Ok(0);
        }
        // A same-revision unresolved observation may become decodable after metadata arrives.
        if decoded.is_err() {
            transaction
                .commit()
                .map_err(|_| Error("storage_commit_failed"))?;
            return Ok(0);
        }
        transaction
            .execute(
                "UPDATE captures SET capture_json=$1,status=$2 WHERE signature=$3",
                &[&raw, &status, signature],
            )
            .map_err(write_failed)?;
        let inserted = insert_transfers(
            &mut transaction,
            signature,
            repaired.as_ref().or_else(|| decoded.as_ref().ok()),
        )?;
        transaction
            .commit()
            .map_err(|_| Error("storage_commit_failed"))?;
        Ok(inserted)
    }

    pub fn summary(db: &Database, mint: &str) -> Result<serde_json::Value> {
        let mut client = db.connect()?;
        let rows = client
            .query(
                "SELECT record_json FROM transfers t WHERE mint=$1 AND NOT EXISTS(SELECT 1 FROM decoder_repairs r WHERE r.signature=t.signature AND r.status='unresolved')",
                &[&mint],
            )
            .map_err(read_failed)?;
        let (mut count, mut volume, mut fees, mut unknown_fees) = (0u64, 0u128, 0u128, 0u64);
        let mut other_decoder_transfers = 0u64;
        for row in rows {
            let transfer: Transfer = serde_json::from_str(row.get::<_, &str>(0))
                .map_err(|_| Error("stored_record_invalid"))?;
            if transfer.decoder_version != DECODER_VERSION {
                other_decoder_transfers = other_decoder_transfers
                    .checked_add(1)
                    .ok_or(Error("aggregate_overflow"))?;
            }
            count = count.checked_add(1).ok_or(Error("aggregate_overflow"))?;
            volume = volume
                .checked_add(u128::from(transfer.amount_raw))
                .ok_or(Error("aggregate_overflow"))?;
            if let Some(fee) = transfer.fee_raw {
                fees = fees
                    .checked_add(u128::from(fee))
                    .ok_or(Error("aggregate_overflow"))?;
            } else {
                unknown_fees = unknown_fees
                    .checked_add(1)
                    .ok_or(Error("aggregate_overflow"))?;
            }
        }
        Ok(
            serde_json::json!({"mint":mint,"transfer_count":count,"target_decoder":DECODER_VERSION,"other_decoder_transfer_count":other_decoder_transfers,"known_volume_raw":volume.to_string(),"known_fees_raw":fees.to_string(),"unknown_fee_count":unknown_fees,"coverage":"unknown","window":"all_stored"}),
        )
    }

    /// Runtime-test retention: keeps the newest `keep` transfers and drops inputs and
    /// progress for older slots, so pruned slots report unknown rather than complete.
    /// Partitions are kept; time-window retention by dropping partitions belongs to #10.
    pub fn prune(db: &Database, keep: usize) -> Result<serde_json::Value> {
        if !(1..=1_000_000).contains(&keep) {
            return Err(Error("invalid_retention_limit"));
        }
        let mut client = db.connect()?;
        let mut transaction = client.transaction().map_err(write_failed)?;
        // Serializes concurrent prunes; writers are not blocked.
        transaction
            .execute("SELECT pg_advisory_xact_lock(7311946003)", &[])
            .map_err(write_failed)?;
        let transfers = transaction
            .execute(
                "DELETE FROM transfers WHERE (slot,transfer_id) NOT IN (SELECT slot,transfer_id FROM transfers
             ORDER BY slot DESC, signature DESC, outer_index DESC, inner_index DESC LIMIT $1)",
                &[&(keep as i64)],
            )
            .map_err(write_failed)?;
        let cutoff: Option<i64> = transaction
            .query_one("SELECT MIN(slot) FROM transfers", &[])
            .map_err(read_failed)?
            .get(0);
        let mut inputs = 0;
        if let Some(cutoff) = cutoff {
            for table in ["captures", "capture_inputs"] {
                inputs += transaction
                    .execute(
                        &format!(
                            "DELETE FROM {table} c WHERE slot < $1 AND NOT EXISTS(SELECT 1 FROM transfers t WHERE t.signature=c.signature)"
                        ),
                        &[&cutoff],
                    )
                    .map_err(write_failed)?;
            }
            for table in [
                "decoder_repairs",
                "live_raw",
                "historical_slots",
                "historical_inputs",
                "historical_source_slots",
                "historical_block_identity",
                "bridge_overlap_proofs",
            ] {
                // A count cap can split its oldest retained slot. Its completion
                // proofs must disappear atomically with the removed transfers.
                let proof = matches!(
                    table,
                    "historical_slots" | "historical_source_slots" | "bridge_overlap_proofs"
                );
                let comparison = if proof && transfers > 0 { "<=" } else { "<" };
                inputs += transaction
                    .execute(
                        &format!("DELETE FROM {table} WHERE slot {comparison} $1"),
                        &[&cutoff],
                    )
                    .map_err(write_failed)?;
            }
        }
        transaction.commit().map_err(write_failed)?;
        Ok(
            serde_json::json!({"kept_limit":keep,"removed_transfers":transfers,"removed_input_rows":inputs,
            "oldest_retained_slot":cutoff,"coverage":"unknown"}),
        )
    }

    /// Read-only export for the independent Python cross-check: retained block responses
    /// (at most `blocks` observations, 64 MiB) and this mint's stored rows in their slot range.
    pub fn comparison_input(db: &Database, mint: &str, blocks: i64) -> Result<serde_json::Value> {
        let mut client = db.connect()?;
        let mut tx = client
            .build_transaction()
            .isolation_level(postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(read_failed)?;
        let mut size = 0;
        let mut retained = Vec::new();
        for row in tx
            .query(
                "SELECT slot,response FROM historical_inputs ORDER BY slot,attempt LIMIT $1",
                &[&blocks],
            )
            .map_err(read_failed)?
        {
            let response: Vec<u8> = row.get(1);
            size += response.len();
            if size > 64 * 1024 * 1024 {
                return Err(Error("comparison_input_limit"));
            }
            let response: serde_json::Value =
                serde_json::from_slice(&response).map_err(|_| Error("stored_record_invalid"))?;
            retained.push(serde_json::json!({"slot":row.get::<_, i64>(0),"response":response}));
        }
        let slots = retained.iter().filter_map(|b| b["slot"].as_i64());
        let (low, high) = (slots.clone().min(), slots.max());
        let transfers = match (low, high) {
            (Some(low), Some(high)) => tx
                .query(
                    "SELECT record_json FROM transfers WHERE mint=$1 AND slot BETWEEN $2 AND $3 LIMIT 100001",
                    &[&mint, &low, &high],
                )
                .map_err(read_failed)?
                .iter()
                .map(|r| serde_json::from_str(r.get::<_, &str>(0)).map_err(|_| Error("stored_record_invalid")))
                .collect::<Result<Vec<serde_json::Value>>>()?,
            _ => Vec::new(),
        };
        tx.commit().map_err(read_failed)?;
        Ok(serde_json::json!({"blocks":retained,"transfers":transfers}))
    }

    pub fn query(db: &Database, mint: Option<&str>, limit: usize) -> Result<Vec<Transfer>> {
        Self::query_ordered(db, mint, limit, false)
    }

    pub fn query_recent(db: &Database, mint: Option<&str>, limit: usize) -> Result<Vec<Transfer>> {
        Self::query_ordered(db, mint, limit, true)
    }

    fn query_ordered(
        db: &Database,
        mint: Option<&str>,
        limit: usize,
        recent: bool,
    ) -> Result<Vec<Transfer>> {
        if !(1..=1000).contains(&limit) {
            return Err(Error("invalid_query_limit"));
        }
        let order = if recent { "DESC" } else { "ASC" };
        let mut client = db.connect()?;
        let rows = client
            .query(
                &format!(
                    "SELECT record_json FROM transfers t
            WHERE ($1::text IS NULL OR mint=$1)
            AND NOT EXISTS(SELECT 1 FROM decoder_repairs r WHERE r.signature=t.signature AND r.status='unresolved')
            ORDER BY slot {order}, signature {order}, outer_index {order}, inner_index {order} LIMIT $2"
                ),
                &[&mint, &(limit as i64)],
            )
            .map_err(read_failed)?;
        rows.iter()
            .map(|row| {
                serde_json::from_str(row.get::<_, &str>(0))
                    .map_err(|_| Error("stored_record_invalid"))
            })
            .collect()
    }

    pub(crate) fn observe_block_identity(
        &mut self,
        slot: u64,
        block: &serde_json::Value,
        transactions: &[serde_json::Value],
    ) -> Result<()> {
        let mut tx = self.db.transaction().map_err(write_failed)?;
        observe_block_identity_scan(&mut tx, slot, block, transactions)?;
        tx.commit().map_err(|_| Error("storage_commit_failed"))
    }
}

/// One multi-row statement per transaction; existing identities are left unchanged.
/// The inner index is -1 for outer instructions; an unknown fee stays null in record_json.
pub(crate) fn insert_transfers(
    tx: &mut Transaction<'_>,
    signature: &str,
    transfers: Option<&Vec<Transfer>>,
) -> Result<usize> {
    let Some(transfers) = transfers.filter(|t| !t.is_empty()) else {
        return Ok(0);
    };
    let (mut ids, mut slots, mut outer, mut inner, mut mints, mut records) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    for transfer in transfers {
        ids.push(transfer.transfer_id.as_str());
        slots.push(i64::try_from(transfer.slot).map_err(|_| Error("unsupported_slot"))?);
        outer.push(
            i32::try_from(transfer.instruction_index).map_err(|_| Error("unsupported_index"))?,
        );
        inner.push(match transfer.inner_instruction_index {
            Some(n) => i32::try_from(n).map_err(|_| Error("unsupported_index"))?,
            None => -1,
        });
        mints.push(transfer.mint.as_str());
        records.push(serde_json::to_string(transfer).map_err(|_| Error("serialization_failed"))?);
    }
    let inserted = tx
        .execute(
            "INSERT INTO transfers(transfer_id,signature,slot,outer_index,inner_index,mint,record_json)
             SELECT id,$2,slot,outer_index,inner_index,mint,record FROM unnest($1::text[],$3::bigint[],$4::int[],$5::int[],$6::text[],$7::text[])
             AS u(id,slot,outer_index,inner_index,mint,record)
             ON CONFLICT(slot,transfer_id) DO NOTHING",
            &[&ids, &signature, &slots, &outer, &inner, &mints, &records],
        )
        .map_err(write_failed)?;
    Ok(inserted as usize)
}

fn observe_block_identity_scan(
    tx: &mut Transaction<'_>,
    slot: u64,
    block: &serde_json::Value,
    transactions: &[serde_json::Value],
) -> Result<()> {
    let slot_key = slot as i64;
    let known: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM historical_block_identity WHERE slot=$1)",
            &[&slot_key],
        )
        .map_err(read_failed)?
        .get(0);
    if !known {
        let mut total = 0;
        for index in 0..33i64 {
            let raw: Option<Option<Vec<u8>>> = tx
                .query_opt(
                    "SELECT CASE WHEN octet_length(response)<=67108864 THEN response END FROM historical_inputs WHERE slot=$1 ORDER BY received_at,attempt LIMIT 1 OFFSET $2",
                    &[&slot_key, &index],
                )
                .map_err(read_failed)?
                .map(|r| r.get(0));
            let Some(raw) = raw else {
                break;
            };
            let raw = raw.ok_or(Error("block_identity_observation_limit"))?;
            total += raw.len();
            if index == 32
                || total > 128 * 1024 * 1024
                || raw.len() > crate::backfill::MAX_BLOCK_BYTES
            {
                return Err(Error("block_identity_observation_limit"));
            }
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(&raw) else {
                continue;
            };
            if value["jsonrpc"] != "2.0" || value["id"] != 1 || value.get("error").is_some() {
                continue;
            }
            let prior = &value["result"];
            let Some(txs) = prior["transactions"].as_array() else {
                continue;
            };
            if txs.len() > 100_000
                || !prior["parentSlot"].as_u64().is_some_and(|p| p < slot)
                || !prior
                    .get("blockTime")
                    .is_some_and(|v| v.is_null() || v.is_i64())
            {
                continue;
            }
            if ["blockhash", "previousBlockhash"].iter().any(|field| {
                prior[*field].as_str().is_none_or(|s| {
                    s.len() > 44 || bs58::decode(s).into_vec().map_or(true, |b| b.len() != 32)
                })
            }) {
                continue;
            }
            observe_block_identity_inner(tx, slot, prior, txs)?;
        }
    }
    observe_block_identity_inner(tx, slot, block, transactions)
}

fn observe_block_identity_inner(
    tx: &mut impl GenericClient,
    slot: u64,
    block: &serde_json::Value,
    transactions: &[serde_json::Value],
) -> Result<()> {
    let mut ids = Vec::with_capacity(transactions.len());
    let mut seen = std::collections::HashSet::new();
    for transaction in transactions {
        let signatures = transaction["transaction"]["signatures"]
            .as_array()
            .filter(|s| !s.is_empty())
            .ok_or(Error("missing_signature"))?;
        for signature in signatures {
            let signature = signature
                .as_str()
                .filter(|s| s.len() <= 100)
                .ok_or(Error("invalid_base58_identifier"))?;
            if bs58::decode(signature)
                .into_vec()
                .map_or(true, |s| s.len() != 64)
            {
                return Err(Error("invalid_base58_identifier"));
            }
        }
        if !seen.insert(&signatures[0]) {
            return Err(Error("duplicate_block_transaction"));
        }
        ids.push(signatures);
    }
    let digest =
        Sha256::digest(serde_json::to_vec(&ids).map_err(|_| Error("serialization_failed"))?)
            .to_vec();
    let hash = block["blockhash"]
        .as_str()
        .ok_or(Error("invalid_block_hash"))?;
    let previous = block["previousBlockhash"]
        .as_str()
        .ok_or(Error("invalid_block_hash"))?;
    let parent = block["parentSlot"]
        .as_i64()
        .ok_or(Error("missing_block_parent"))?;
    let time = block["blockTime"].as_i64();
    let slot_key = slot as i64;
    let parent_hash: Option<String> = tx
        .query_opt(
            "SELECT blockhash FROM historical_block_identity WHERE slot=$1",
            &[&parent],
        )
        .map_err(read_failed)?
        .map(|r| r.get(0));
    let conflicting_child: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM historical_block_identity WHERE parent_slot=$1 AND previous_blockhash<>$2)",
            &[&slot_key, &hash],
        )
        .map_err(read_failed)?
        .get(0);
    if parent_hash.as_deref().is_some_and(|h| h != previous) || conflicting_child {
        return Err(Error("block_parent_conflict"));
    }
    let prior = tx
        .query_opt(
            "SELECT blockhash,parent_slot,previous_blockhash,transaction_ids_hash,block_time FROM historical_block_identity WHERE slot=$1 FOR UPDATE",
            &[&slot_key],
        )
        .map_err(read_failed)?;
    if let Some(row) = prior {
        let (old_hash, old_parent, old_previous, old_ids, old_time): (
            String,
            i64,
            String,
            Vec<u8>,
            Option<i64>,
        ) = (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
        if old_hash != hash
            || old_parent != parent
            || old_previous != previous
            || old_ids != digest
            || matches!((old_time,time),(Some(a),Some(b)) if a!=b)
        {
            return Err(Error("block_identity_conflict"));
        }
        tx.execute(
            "UPDATE historical_block_identity SET block_time=COALESCE(block_time,$2) WHERE slot=$1",
            &[&slot_key, &time],
        )
        .map_err(write_failed)?;
    } else {
        tx.execute(
            "INSERT INTO historical_block_identity VALUES($1,$2,$3,$4,$5,$6)",
            &[&slot_key, &hash, &parent, &previous, &digest, &time],
        )
        .map_err(write_failed)?;
    }
    Ok(())
}
