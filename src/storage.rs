use crate::{
    Error, Result,
    decoder::DECODER_VERSION,
    model::{Capture, Transfer},
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use std::{path::Path, time::Duration};

pub struct Store(Connection);

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path).map_err(|_| Error("storage_open_failed"))?;
        db.busy_timeout(Duration::from_secs(5))
            .map_err(|_| Error("storage_configuration_failed"))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS captures (
                signature TEXT PRIMARY KEY, result_json TEXT NOT NULL,
                capture_json BLOB NOT NULL, status TEXT NOT NULL, decoder_version TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS transfers (
                transfer_id TEXT PRIMARY KEY, signature TEXT NOT NULL,
                slot INTEGER NOT NULL, outer_index INTEGER NOT NULL,
                inner_index INTEGER NOT NULL, mint TEXT NOT NULL, record_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS transfers_mint ON transfers(mint);",
        )
        .map_err(|_| Error("storage_configuration_failed"))?;
        Ok(Self(db))
    }

    /// Stores a complete transaction's output atomically. Replay of identical input is a no-op.
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
        let transaction = self
            .0
            .transaction()
            .map_err(|_| Error("storage_write_failed"))?;
        let prior: Option<(String, String)> = transaction
            .query_row(
                "SELECT result_json, decoder_version FROM captures WHERE signature=?1",
                [signature],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|_| Error("storage_read_failed"))?;
        if let Some((prior, version)) = prior {
            if prior != result_json || version != DECODER_VERSION {
                return Err(Error("capture_conflict"));
            }
            return Ok(0);
        }
        let status = match decoded {
            Ok(_) if !capture.result.meta.err.is_null() => "failed_transaction",
            Ok(_) => "decoded",
            Err(e) => e.0,
        };
        transaction
            .execute(
                "INSERT INTO captures VALUES (?1,?2,?3,?4,?5)",
                params![signature, result_json, raw, status, DECODER_VERSION],
            )
            .map_err(|_| Error("storage_write_failed"))?;
        let mut inserted = 0;
        if let Ok(transfers) = decoded {
            for transfer in transfers {
                let json =
                    serde_json::to_string(transfer).map_err(|_| Error("serialization_failed"))?;
                transaction
                    .execute(
                        "INSERT INTO transfers VALUES (?1,?2,?3,?4,?5,?6,?7)",
                        params![
                            transfer.transfer_id,
                            signature,
                            transfer.slot as i64,
                            transfer.instruction_index as i64,
                            transfer.inner_instruction_index.map_or(-1, |n| n as i64),
                            transfer.mint,
                            json,
                        ],
                    )
                    .map_err(|_| Error("storage_write_failed"))?;
                inserted += 1;
            }
        }
        transaction
            .commit()
            .map_err(|_| Error("storage_commit_failed"))?;
        Ok(inserted)
    }

    pub fn query(path: &Path, mint: Option<&str>, limit: usize) -> Result<Vec<Transfer>> {
        if !(1..=1000).contains(&limit) {
            return Err(Error("invalid_query_limit"));
        }
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|_| Error("storage_open_failed"))?;
        let mut stmt = db
            .prepare(
                "SELECT record_json FROM transfers
            WHERE (?1 IS NULL OR mint=?1)
            ORDER BY slot, signature, outer_index, inner_index LIMIT ?2",
            )
            .map_err(|_| Error("storage_read_failed"))?;
        let rows = stmt
            .query_map(params![mint, limit as i64], |r| r.get::<_, String>(0))
            .map_err(|_| Error("storage_read_failed"))?;
        rows.map(|row| {
            let text = row.map_err(|_| Error("storage_read_failed"))?;
            serde_json::from_str(&text).map_err(|_| Error("stored_record_invalid"))
        })
        .collect()
    }
}
