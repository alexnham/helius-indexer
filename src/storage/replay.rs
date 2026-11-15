//! Frozen replay scope and durable allowance reservations. No provider calls here.
use super::{Database, live::now, read_failed, write_failed};
use crate::{Error, Result, decoder::DECODER_VERSION};
use postgres::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const SECONDS_PER_GRANT: i64 = 60;
pub const MESSAGES_PER_GRANT: i64 = 10_000;
pub const BYTES_PER_GRANT: i64 = 64 * 1024 * 1024;
pub const CONNECTIONS_PER_GRANT: i64 = 3;
/// Archive batches per archive grant; at most 64 grants, so 256 batches per job.
pub const ARCHIVE_BATCHES_PER_GRANT: i64 = 4;
/// Ceilings one archive batch runs under: requests, response bytes, seconds.
pub const ARCHIVE_BATCH_LIMITS: (i64, i64, i64) = (32, 128 * 1024 * 1024, 120);

pub(crate) fn valid_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Error("invalid_replay_job_id"));
    }
    Ok(())
}

pub struct JobLog(Client);

pub struct Scope {
    pub mint: String,
    pub start: u64,
    pub end: u64,
}

/// Capture and history source name of Old Faithful input.
pub const ARCHIVE_SOURCE: &str = "old-faithful-jetstreamer";
/// Capture source name of LaserStream input.
pub const REPLAY_SOURCE: &str = "helius-laserstream";

/// One reconciliation outcome for an overlap slot.
pub struct Overlap {
    pub outcome: &'static str,
    pub reason: Option<&'static str>,
    pub replay_inputs: usize,
    pub archive_mint_inputs: usize,
}

/// Routed (start, end, kind) segments of one revision.
pub type RouteSegments = Vec<(u64, u64, String)>;

/// One measurement of where each slot of a job may come from.
pub struct Route {
    pub replay_first: Option<u64>,
    pub reference: u64,
    pub archive_probed: bool,
    pub archive_last: Option<u64>,
    pub archive_error: Option<&'static str>,
    pub segments: Vec<(u64, u64, &'static str)>,
}

pub struct Pending {
    pub signature: Vec<u8>,
    pub hash: Vec<u8>,
    pub raw_id: i64,
}

impl JobLog {
    pub fn scope(&mut self, id: &str) -> Result<Scope> {
        valid_id(id)?;
        let row = self
            .0
            .query_opt(
                "SELECT mint,start_slot,end_slot,decoder FROM replay_jobs WHERE id=$1",
                &[&id],
            )
            .map_err(read_failed)?
            .ok_or(Error("replay_job_missing"))?;
        if row.get::<_, String>(3) != DECODER_VERSION {
            return Err(Error("replay_decoder_mismatch"));
        }
        Ok(Scope {
            mint: row.get(0),
            start: row.get::<_, i64>(1) as u64,
            end: row.get::<_, i64>(2) as u64,
        })
    }

    /// Holds job ownership until this log connection closes. Pending input is
    /// bounded and rotated by a durable fairness cursor, never a progress cursor.
    pub fn pending(&mut self, id: &str) -> Result<Vec<Pending>> {
        self.scope(id)?;
        let locked:bool=self.0.query_one("SELECT pg_try_advisory_lock(hashtextextended(current_schema() || ':replay:' || $1,0))", &[&id]).map_err(write_failed)?.get(0);
        if !locked {
            return Err(Error("replay_job_busy"));
        }
        let rows=self.0.query("SELECT r.signature,r.input_hash,r.raw_id FROM replay_receipts r WHERE r.job=$1 AND r.status<>'decoded' ORDER BY (r.signature<=COALESCE((SELECT retry_cursor FROM replay_jobs WHERE id=$1),'\\x'::bytea)),r.signature LIMIT 100", &[&id]).map_err(read_failed)?;
        Ok(rows
            .into_iter()
            .map(|r| Pending {
                signature: r.get(0),
                hash: r.get(1),
                raw_id: r.get(2),
            })
            .collect())
    }

    pub fn raw(&mut self, raw_id: i64) -> Result<Option<(Vec<u8>, String)>> {
        Ok(self.0.query_opt("SELECT protobuf,proto_schema FROM live_raw WHERE id=$1 AND octet_length(protobuf)<=16777216", &[&raw_id]).map_err(read_failed)?.map(|r|(r.get(0),r.get(1))))
    }

    pub fn retry_cursor(&mut self, id: &str, signature: &[u8]) -> Result<()> {
        let mut tx = self.0.transaction().map_err(write_failed)?;
        tx.execute(
            "UPDATE replay_jobs SET retry_cursor=$1 WHERE id=$2",
            &[&signature, &id],
        )
        .map_err(write_failed)?;
        tx.execute("UPDATE replay_receipts SET status='pending',processed_at=NULL,processing_attempts=processing_attempts+1 WHERE job=$1 AND signature=$2 AND status<>'decoded'", &[&id,&signature]).map_err(write_failed)?;
        tx.commit().map_err(write_failed)
    }

    pub fn retry_acknowledge(
        &mut self,
        id: &str,
        raw_id: i64,
        result: &Result<usize>,
    ) -> Result<()> {
        let status = result.as_ref().map_or_else(|e| e.0, |_| "decoded");
        self.0
            .execute(
                "UPDATE replay_receipts SET status=$1,processed_at=CASE WHEN $1='decoded' THEN EXTRACT(EPOCH FROM now())::bigint ELSE NULL END WHERE job=$2 AND raw_id=$3",
                &[&status, &id, &raw_id],
            )
            .map_err(write_failed)?;
        Ok(())
    }
    pub fn open(database: &Database) -> Result<Self> {
        Ok(Self(database.connect()?))
    }

    /// Idempotent creation never moves the original requested range or decoder.
    pub fn create(&mut self, id: &str, mint: &str, start: u64, end: u64) -> Result<()> {
        valid_id(id)?;
        crate::backfill::scope(mint, start, end)?;
        let mut tx = self.0.transaction().map_err(write_failed)?;
        tx.execute("INSERT INTO replay_jobs(id,mint,start_slot,end_slot,decoder,created_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING",
            &[&id,&mint,&(start as i64),&(end as i64),&DECODER_VERSION,&now()]).map_err(write_failed)?;
        let r = tx
            .query_one(
                "SELECT mint,start_slot,end_slot,decoder FROM replay_jobs WHERE id=$1",
                &[&id],
            )
            .map_err(read_failed)?;
        if r.get::<_, String>(0) != mint
            || r.get::<_, i64>(1) != start as i64
            || r.get::<_, i64>(2) != end as i64
        {
            return Err(Error("replay_scope_conflict"));
        }
        if r.get::<_, String>(3) != DECODER_VERSION {
            return Err(Error("replay_decoder_mismatch"));
        }
        tx.commit().map_err(write_failed)
    }

    pub fn status(&mut self, id: &str) -> Result<Value> {
        valid_id(id)?;
        let mut tx = self
            .0
            .build_transaction()
            .read_only(true)
            .isolation_level(postgres::IsolationLevel::RepeatableRead)
            .start()
            .map_err(read_failed)?;
        let r = tx.query_opt("SELECT mint,start_slot,end_slot,decoder,created_at,grants,seconds_charged,connections_used,messages_used,bytes_used FROM replay_jobs WHERE id=$1", &[&id]).map_err(read_failed)?.ok_or(Error("replay_job_missing"))?;
        let receipts=tx.query_one("SELECT COUNT(*),COUNT(*) FILTER (WHERE r.status='decoded'),COUNT(*) FILTER (WHERE r.status<>'decoded'),COUNT(l.id) FROM replay_receipts r LEFT JOIN live_raw l ON l.id=r.raw_id WHERE r.job=$1", &[&id]).map_err(read_failed)?;
        let attempts:Vec<Value>=tx.query("SELECT id,started_at,reserved_seconds,status,first_available,reference_slot,observed_at,end_observed FROM replay_attempts WHERE job=$1 ORDER BY id DESC LIMIT 20", &[&id]).map_err(read_failed)?.into_iter().map(|a|{
            let first:Option<i64>=a.get(4);let observed:Option<i64>=a.get(6);
            json!({"attempt":a.get::<_,i64>(0),"started_at":a.get::<_,i64>(1),"reserved_seconds":a.get::<_,i64>(2),"status":a.get::<_,String>(3),"first_available":first,"reference_slot":a.get::<_,Option<i64>>(5),"observed_at":observed,"availability_age_seconds":observed.and_then(|t|now().checked_sub(t)).filter(|n|*n>=0),"replay_margin_slots":first.map(|n|r.get::<_,i64>(1)-n),"end_observed":a.get::<_,bool>(7)})
        }).collect();
        let failures:Vec<Value>=tx.query("SELECT status,COUNT(*) FROM replay_receipts WHERE job=$1 AND status NOT IN ('pending','decoded') GROUP BY status ORDER BY status LIMIT 16", &[&id]).map_err(read_failed)?.into_iter().map(|r|json!({"error_class":r.get::<_,String>(0),"inputs":r.get::<_,i64>(1)})).collect();
        let oldest:Option<i64>=tx.query_one("SELECT MIN(first_received) FROM replay_receipts WHERE job=$1 AND status<>'decoded'", &[&id]).map_err(read_failed)?.get(0);
        let retained:i64=tx.query_one("SELECT COUNT(*) FROM transfers t WHERE mint=$1 AND slot BETWEEN $2 AND $3 AND NOT EXISTS(SELECT 1 FROM decoder_repairs r WHERE r.signature=t.signature AND r.status='unresolved')", &[&r.get::<_,String>(0),&r.get::<_,i64>(1),&r.get::<_,i64>(2)]).map_err(read_failed)?.get(0);
        let routing = match tx.query_opt("SELECT revision,observed_at,replay_first,reference_slot,archive_probed,archive_last,archive_error FROM replay_routes WHERE job=$1 ORDER BY revision DESC LIMIT 1", &[&id]).map_err(read_failed)? {
            None => Value::Null,
            Some(route) => {
                let revision: i64 = route.get(0);
                let segments: Vec<Value> = tx.query("SELECT start_slot,end_slot,kind FROM replay_route_segments WHERE job=$1 AND revision=$2 ORDER BY start_slot", &[&id,&revision]).map_err(read_failed)?.into_iter()
                    .map(|s| json!({"start":s.get::<_,i64>(0),"end":s.get::<_,i64>(1),"kind":s.get::<_,String>(2)})).collect();
                let observed: i64 = route.get(1);
                json!({"revision":revision,"observed_at":observed,"measurement_age_seconds":now().checked_sub(observed).filter(|n|*n>=0),
                    "replay_first_available":route.get::<_,Option<i64>>(2),"reference_slot":route.get::<_,i64>(3),
                    "archive":{"probed":route.get::<_,bool>(4),"last_available":route.get::<_,Option<i64>>(5),"error_class":route.get::<_,Option<String>>(6)},
                    "segments":segments,"state":"routed_not_processed"})
            }
        };
        let archive_row = tx
            .query_one(
                "SELECT archive_grants,archive_batches_charged FROM replay_jobs WHERE id=$1",
                &[&id],
            )
            .map_err(read_failed)?;
        let archive_attempts: Vec<Value> = tx.query("SELECT id,route_revision,start_slot,end_slot,started_at,status,request_limit,byte_limit,second_limit,requests,response_bytes,cached_chunks FROM archive_attempts WHERE job=$1 ORDER BY id DESC LIMIT 20", &[&id]).map_err(read_failed)?.into_iter()
            .map(|a| json!({"attempt":a.get::<_,i64>(0),"route_revision":a.get::<_,i64>(1),"start":a.get::<_,i64>(2),"end":a.get::<_,i64>(3),"started_at":a.get::<_,i64>(4),"status":a.get::<_,String>(5),
                "limits":{"requests":a.get::<_,i64>(6),"response_bytes":a.get::<_,i64>(7),"seconds":a.get::<_,i64>(8)},
                "observed":{"requests":a.get::<_,Option<i64>>(9),"response_bytes":a.get::<_,Option<i64>>(10),"cached_chunks":a.get::<_,Option<i64>>(11)}})).collect();
        let archive_by_status: serde_json::Map<String, Value> = tx.query("SELECT status,COUNT(*),COALESCE(SUM(response_bytes),0)::BIGINT FROM archive_attempts WHERE job=$1 GROUP BY status ORDER BY status LIMIT 16", &[&id]).map_err(read_failed)?.into_iter()
            .map(|r| (r.get::<_,String>(0), json!({"attempts":r.get::<_,i64>(1),"response_bytes":r.get::<_,i64>(2)}))).collect();
        let (archive_grants, archive_charged): (i64, i64) =
            (archive_row.get(0), archive_row.get(1));
        let archive = json!({"grants":archive_grants,"batches_charged":archive_charged,"batches_remaining":archive_grants*ARCHIVE_BATCHES_PER_GRANT-archive_charged,"by_status":archive_by_status,"attempts":archive_attempts});
        tx.commit().map_err(read_failed)?;
        let (grants, seconds, connections, messages, bytes): (i64, i64, i64, i64, i64) =
            (r.get(5), r.get(6), r.get(7), r.get(8), r.get(9));
        let exhausted = seconds >= grants * SECONDS_PER_GRANT
            || connections >= grants * CONNECTIONS_PER_GRANT
            || messages >= grants * MESSAGES_PER_GRANT
            || bytes >= grants * BYTES_PER_GRANT;
        let state = if r.get::<_, String>(3) != DECODER_VERSION {
            "decoder_superseded"
        } else if exhausted {
            "budget_exhausted"
        } else {
            "unverified"
        };
        Ok(
            json!({"job":id,"state":state,"pending_age_seconds":now().checked_sub(r.get::<_,i64>(4)).filter(|n|*n>=0),"oldest_unresolved_age_seconds":if receipts.get::<_,i64>(2)==0 {Some(0)}else{oldest.and_then(|t|now().checked_sub(t)).filter(|n|*n>=0)},"failures":failures,"retained":{"transfer_count":retained,"coverage":"unknown"},"mint":r.get::<_,String>(0),"start":r.get::<_,i64>(1),"end":r.get::<_,i64>(2),"decoder_version":r.get::<_,String>(3),"created_at":r.get::<_,i64>(4),
            "coverage":"unknown","verified_boundary":null,"resume_from":r.get::<_,i64>(1),"pending_interval":{"start":r.get::<_,i64>(1),"end":r.get::<_,i64>(2),"kind":"unverified"},"attempts":attempts,"routing":routing,"archive":archive,"reason":"filtered_replay_has_no_validated_slot_completion_evidence",
            "receipts":{"unique_inputs":receipts.get::<_,i64>(0),"decoded":receipts.get::<_,i64>(1),"unresolved":receipts.get::<_,i64>(2),"raw_still_retained":receipts.get::<_,i64>(3)},
            "budget":{"grants":grants,"seconds_charged":seconds,"seconds_remaining":grants*SECONDS_PER_GRANT-seconds,"connections_used":connections,"connections_remaining":grants*CONNECTIONS_PER_GRANT-connections,"messages_used":messages,"messages_remaining":grants*MESSAGES_PER_GRANT-messages,"bytes_used":bytes,"bytes_remaining":grants*BYTES_PER_GRANT-bytes}}),
        )
    }

    /// Append the next routing revision; earlier revisions and their segments stay.
    pub fn record_route(&mut self, id: &str, route: &Route) -> Result<i64> {
        valid_id(id)?;
        let slot = |n: u64| i64::try_from(n).map_err(|_| Error("unsupported_slot"));
        let opt = |n: Option<u64>| n.map(slot).transpose();
        let mut tx = self.0.transaction().map_err(write_failed)?;
        // Serialize revisions per job; the row lock lasts until commit.
        tx.query_opt("SELECT 1 FROM replay_jobs WHERE id=$1 FOR UPDATE", &[&id])
            .map_err(write_failed)?
            .ok_or(Error("replay_job_missing"))?;
        let revision: i64 = tx
            .query_one(
                "SELECT COALESCE(MAX(revision),0)+1 FROM replay_routes WHERE job=$1",
                &[&id],
            )
            .map_err(read_failed)?
            .get(0);
        tx.execute(
            "INSERT INTO replay_routes VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
            &[
                &id,
                &revision,
                &now(),
                &opt(route.replay_first)?,
                &slot(route.reference)?,
                &route.archive_probed,
                &opt(route.archive_last)?,
                &route.archive_error,
            ],
        )
        .map_err(write_failed)?;
        for (start, end, kind) in &route.segments {
            tx.execute(
                "INSERT INTO replay_route_segments VALUES($1,$2,$3,$4,$5)",
                &[&id, &revision, &slot(*start)?, &slot(*end)?, kind],
            )
            .map_err(write_failed)?;
        }
        tx.commit().map_err(write_failed)?;
        Ok(revision)
    }

    /// Latest routing revision and its segments, if the job was ever routed.
    pub fn latest_route(&mut self, id: &str) -> Result<Option<(i64, RouteSegments)>> {
        valid_id(id)?;
        let Some(revision) = self
            .0
            .query_one(
                "SELECT MAX(revision) FROM replay_routes WHERE job=$1",
                &[&id],
            )
            .map_err(read_failed)?
            .get::<_, Option<i64>>(0)
        else {
            return Ok(None);
        };
        let segments = self.0.query("SELECT start_slot,end_slot,kind FROM replay_route_segments WHERE job=$1 AND revision=$2 ORDER BY start_slot", &[&id, &revision])
            .map_err(read_failed)?
            .into_iter()
            .map(|r| (r.get::<_, i64>(0) as u64, r.get::<_, i64>(1) as u64, r.get(2)))
            .collect();
        Ok(Some((revision, segments)))
    }

    /// Explicit archive refill; replay grants never replenish archive work.
    pub fn archive_grant(&mut self, id: &str) -> Result<()> {
        valid_id(id)?;
        let n = self
            .0
            .execute(
                "UPDATE replay_jobs SET archive_grants=archive_grants+1 WHERE id=$1 AND archive_grants<64 AND decoder=$2",
                &[&id, &DECODER_VERSION],
            )
            .map_err(write_failed)?;
        if n != 1 {
            return Err(Error("archive_grant_rejected"));
        }
        Ok(())
    }

    /// Charge one whole archive batch before any cache or network work. The
    /// connection holding the advisory lock owns the attempt until it closes.
    pub fn archive_reserve(
        database: &Database,
        id: &str,
        revision: i64,
        start: u64,
        end: u64,
    ) -> Result<ArchiveReservation> {
        valid_id(id)?;
        let mut client = database.connect()?;
        let locked: bool = client.query_one("SELECT pg_try_advisory_lock(hashtextextended(current_schema() || ':archive:' || $1,0))", &[&id]).map_err(write_failed)?.get(0);
        if !locked {
            return Err(Error("archive_job_busy"));
        }
        let mut tx = client.transaction().map_err(write_failed)?;
        let n = tx.execute("UPDATE replay_jobs SET archive_batches_charged=archive_batches_charged+1 WHERE id=$1 AND decoder=$2 AND archive_batches_charged<archive_grants*$3", &[&id, &DECODER_VERSION, &ARCHIVE_BATCHES_PER_GRANT]).map_err(write_failed)?;
        if n != 1 {
            return Err(Error("archive_budget_exhausted"));
        }
        tx.execute(
            "UPDATE archive_attempts SET status='interrupted' WHERE job=$1 AND status='running'",
            &[&id],
        )
        .map_err(write_failed)?;
        let (requests, bytes, seconds) = ARCHIVE_BATCH_LIMITS;
        let attempt: i64 = tx.query_one("INSERT INTO archive_attempts(job,route_revision,start_slot,end_slot,started_at,status,request_limit,byte_limit,second_limit) VALUES($1,$2,$3,$4,$5,'running',$6,$7,$8) RETURNING id",
            &[&id, &revision, &(start as i64), &(end as i64), &now(), &requests, &bytes, &seconds]).map_err(write_failed)?.get(0);
        tx.commit().map_err(write_failed)?;
        Ok(ArchiveReservation { client, attempt })
    }

    /// Replay receipts in one slot: (base58 signature, receipt status).
    pub fn slot_receipts(&mut self, id: &str, slot: u64) -> Result<Vec<(String, String)>> {
        Ok(self
            .0
            .query(
                "SELECT signature,status FROM replay_receipts WHERE job=$1 AND slot=$2 ORDER BY signature",
                &[&id, &(slot as i64)],
            )
            .map_err(read_failed)?
            .into_iter()
            .map(|r| {
                (
                    bs58::encode(r.get::<_, Vec<u8>>(0)).into_string(),
                    r.get(1),
                )
            })
            .collect())
    }

    /// Retained per-source captures in one slot: signature -> (capture bytes, receipt status).
    pub fn slot_captures(
        &mut self,
        slot: u64,
        source: &str,
    ) -> Result<std::collections::BTreeMap<String, (Vec<u8>, String)>> {
        Ok(self
            .0
            .query(
                "SELECT signature,capture_json,status FROM capture_inputs WHERE slot=$1 AND source=$2 AND octet_length(capture_json)<=16777216 ORDER BY signature,result_digest",
                &[&(slot as i64), &source],
            )
            .map_err(read_failed)?
            .into_iter()
            .map(|r| (r.get(0), (r.get(1), r.get(2))))
            .collect())
    }

    /// Decoder revision of the stored output for each signature that has one.
    pub fn output_decoders(&mut self, signatures: &[String]) -> Result<Vec<String>> {
        Ok(self
            .0
            .query(
                "SELECT DISTINCT decoder_version FROM captures WHERE signature = ANY($1)",
                &[&signatures],
            )
            .map_err(read_failed)?
            .into_iter()
            .map(|r| r.get(0))
            .collect())
    }

    /// Archive full-block evidence still retained: slot complete or skipped for the
    /// current decoder, completed by the archive source, with its raw input kept.
    pub fn archive_slot_verified(&mut self, mint: &str, slot: u64) -> Result<bool> {
        Ok(self.0.query_one("SELECT EXISTS(SELECT 1 FROM historical_slots h JOIN historical_source_slots s ON s.mint=h.mint AND s.decoder=h.decoder AND s.slot=h.slot AND s.source_kind=$4 AND s.status IN ('complete','skipped') WHERE h.mint=$1 AND h.decoder=$2 AND h.slot=$3 AND h.status IN ('complete','skipped') AND EXISTS(SELECT 1 FROM historical_inputs i WHERE i.mint=h.mint AND i.decoder=h.decoder AND i.slot=h.slot AND i.source_kind=$4))",
            &[&mint, &DECODER_VERSION, &(slot as i64), &ARCHIVE_SOURCE]).map_err(read_failed)?.get(0))
    }

    pub fn record_overlap(
        &mut self,
        id: &str,
        revision: i64,
        slot: u64,
        outcome: &Overlap,
    ) -> Result<()> {
        let n = |v: usize| i64::try_from(v).map_err(|_| Error("stored_record_invalid"));
        self.0.execute("INSERT INTO replay_overlap_outcomes(job,route_revision,slot,decoder,outcome,reason,replay_inputs,archive_mint_inputs,observed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            &[&id, &revision, &(slot as i64), &DECODER_VERSION, &outcome.outcome, &outcome.reason, &n(outcome.replay_inputs)?, &n(outcome.archive_mint_inputs)?, &now()]).map_err(write_failed)?;
        Ok(())
    }

    /// Latest outcome per slot for this route and the current decoder revision.
    pub fn latest_overlaps(
        &mut self,
        id: &str,
        revision: i64,
    ) -> Result<std::collections::BTreeMap<u64, (String, Option<String>)>> {
        Ok(self.0.query("SELECT DISTINCT ON (slot) slot,outcome,reason FROM replay_overlap_outcomes WHERE job=$1 AND route_revision=$2 AND decoder=$3 ORDER BY slot,id DESC",
            &[&id, &revision, &DECODER_VERSION]).map_err(read_failed)?.into_iter()
            .map(|r| (r.get::<_, i64>(0) as u64, (r.get(1), r.get(2)))).collect())
    }

    /// Explicit refill; ordinary restart never replenishes a consumed allowance.
    pub fn grant(&mut self, id: &str) -> Result<()> {
        valid_id(id)?;
        let n = self
            .0
            .execute(
                "UPDATE replay_jobs SET grants=grants+1 WHERE id=$1 AND grants<64 AND decoder=$2",
                &[&id, &DECODER_VERSION],
            )
            .map_err(write_failed)?;
        if n != 1 {
            return Err(Error("replay_grant_rejected"));
        }
        Ok(())
    }

    /// Reserve time before opening a provider connection. Crashes retain the whole
    /// charge. The connection holding the advisory lock owns the attempt.
    pub fn reserve(database: &Database, id: &str, seconds: u64) -> Result<Reservation> {
        valid_id(id)?;
        if !(1..=SECONDS_PER_GRANT as u64).contains(&seconds) {
            return Err(Error("invalid_replay_bounds"));
        }
        let mut client = database.connect()?;
        let locked: bool = client.query_one("SELECT pg_try_advisory_lock(hashtextextended(current_schema() || ':replay:' || $1,0))", &[&id]).map_err(write_failed)?.get(0);
        if !locked {
            return Err(Error("replay_job_busy"));
        }
        let mut tx = client.transaction().map_err(write_failed)?;
        let n = tx.execute("UPDATE replay_jobs SET seconds_charged=seconds_charged+$2 WHERE id=$1 AND decoder=$3 AND seconds_charged+$2<=grants*60 AND connections_used<grants*3 AND messages_used<grants*10000 AND bytes_used<grants*67108864", &[&id,&(seconds as i64),&DECODER_VERSION]).map_err(write_failed)?;
        if n != 1 {
            return Err(Error("replay_budget_or_scope_rejected"));
        }
        tx.execute(
            "UPDATE replay_attempts SET status='interrupted' WHERE job=$1 AND status='running'",
            &[&id],
        )
        .map_err(write_failed)?;
        let attempt: i64 = tx.query_one("INSERT INTO replay_attempts(job,started_at,reserved_seconds,status) VALUES($1,$2,$3,'running') RETURNING id", &[&id,&now(),&(seconds as i64)]).map_err(write_failed)?.get(0);
        tx.commit().map_err(write_failed)?;
        Ok(Reservation {
            client,
            job: id.into(),
            attempt,
        })
    }
}

pub struct Reservation {
    client: Client,
    job: String,
    pub attempt: i64,
}

pub struct ArchiveReservation {
    client: Client,
    pub attempt: i64,
}
impl ArchiveReservation {
    /// Observed (requests, response bytes, cached chunks); dropping releases the lock.
    pub fn finish(mut self, status: &str, observed: (u64, u64, u64)) -> Result<()> {
        let n = |v: u64| i64::try_from(v).map_err(|_| Error("stored_record_invalid"));
        self.client.execute("UPDATE archive_attempts SET status=$1,requests=$2,response_bytes=$3,cached_chunks=$4 WHERE id=$5",
            &[&status, &n(observed.0)?, &n(observed.1)?, &n(observed.2)?, &self.attempt]).map_err(write_failed)?;
        Ok(())
    }
}

pub enum Retained {
    BeforeStart,
    PastEnd,
    AlreadyDecoded,
    Pending(i64),
}

impl Reservation {
    pub fn availability(
        &mut self,
        scope: &Scope,
        first: Option<u64>,
        reference: u64,
    ) -> Result<()> {
        if first.is_some_and(|n| n > i64::MAX as u64) || reference > i64::MAX as u64 {
            return Err(Error("unsupported_slot"));
        }
        self.client.execute("UPDATE replay_attempts SET first_available=$1,reference_slot=$2,observed_at=$3 WHERE id=$4", &[&first.map(|n|n as i64),&(reference as i64),&now(),&self.attempt]).map_err(write_failed)?;
        let first = first.ok_or(Error("replay_info_unavailable"))?;
        if scope.start < first {
            return Err(Error("outside_replay_window"));
        }
        if scope.end > reference {
            return Err(Error("replay_end_not_finalized"));
        }
        Ok(())
    }

    pub fn allowance(&mut self) -> Result<()> {
        let allowed:bool=self.client.query_one("SELECT messages_used<grants*10000 AND bytes_used<grants*67108864 FROM replay_jobs WHERE id=$1", &[&self.job]).map_err(read_failed)?.get(0);
        if !allowed {
            return Err(Error("replay_budget_exhausted"));
        }
        Ok(())
    }
    /// The ingestion boundary supplies validated wire metadata. Raw bytes and a
    /// pending receipt commit together before the shared decoder is invoked.
    #[cfg(test)]
    fn retain(&mut self, slot: u64, signature: &[u8], protobuf: &[u8]) -> Result<Retained> {
        self.retain_evidence(slot, signature, protobuf, None)
    }
    pub fn retain_evidence(
        &mut self,
        slot: u64,
        signature: &[u8],
        protobuf: &[u8],
        normalized_hash: Option<&[u8]>,
    ) -> Result<Retained> {
        if slot > i64::MAX as u64
            || signature.len() != 64
            || protobuf.len() > crate::decoder::MAX_CAPTURE_BYTES
            || normalized_hash.is_some_and(|h| h.len() != 32)
        {
            return Err(Error("invalid_replay_input"));
        }
        let hash = Sha256::digest(protobuf).to_vec();
        // Charge received work even if raw persistence fails immediately afterwards.
        let row=self.client.query_opt("UPDATE replay_jobs SET messages_used=messages_used+1,bytes_used=bytes_used+$2 WHERE id=$1 AND messages_used<grants*10000 AND bytes_used+$2<=grants*67108864 RETURNING start_slot,end_slot", &[&self.job,&(protobuf.len() as i64)]).map_err(write_failed)?.ok_or(Error("replay_budget_exhausted"))?;
        let (start, end): (i64, i64) = (row.get(0), row.get(1));
        let slot = slot as i64;
        let mut tx = self.client.transaction().map_err(write_failed)?;
        if slot < start {
            tx.commit().map_err(write_failed)?;
            return Ok(Retained::BeforeStart);
        }
        if slot > end {
            tx.execute(
                "UPDATE replay_attempts SET end_observed=TRUE WHERE id=$1",
                &[&self.attempt],
            )
            .map_err(write_failed)?;
            tx.commit().map_err(write_failed)?;
            return Ok(Retained::PastEnd);
        }
        let existing=tx.query_opt("SELECT input_hash,raw_id,status,normalized_hash,slot FROM replay_receipts WHERE job=$1 AND signature=$2", &[&self.job,&signature]).map_err(read_failed)?;
        if let Some(row) = existing.as_ref() {
            if row.get::<_, i64>(4) != slot {
                return Err(Error("replay_input_conflict"));
            }
            let same_wire = row.get::<_, Vec<u8>>(0) == hash;
            let existing_normalized: Option<Vec<u8>> = row.get(3);
            let same_evidence = same_wire
                || normalized_hash.is_some_and(|h| existing_normalized.as_deref() == Some(h));
            if same_evidence && row.get::<_, String>(2) == "decoded" {
                tx.commit().map_err(write_failed)?;
                return Ok(Retained::AlreadyDecoded);
            }
            let raw_id: i64 = row.get(1);
            if same_wire && tx
                .query_opt("SELECT protobuf FROM live_raw WHERE id=$1 AND octet_length(protobuf)<=16777216", &[&raw_id])
                .map_err(read_failed)?
                .is_some_and(|r| Sha256::digest(r.get::<_,Vec<u8>>(0)).to_vec()==hash)
            {
                tx.execute("UPDATE replay_receipts SET input_attempt=$1,status='pending',processed_at=NULL,processing_attempts=processing_attempts+1 WHERE job=$2 AND signature=$3", &[&self.attempt,&self.job,&signature]).map_err(write_failed)?;
                tx.commit().map_err(write_failed)?;
                return Ok(Retained::Pending(raw_id));
            }
        }
        let raw_id:i64=tx.query_one("INSERT INTO live_raw(slot,protobuf,status,received_at,ingestion_mode) VALUES($1,$2,'pending',$3,'replay') RETURNING id", &[&slot,&protobuf,&now()]).map_err(write_failed)?.get(0);
        tx.execute("INSERT INTO replay_receipts(job,signature,input_hash,slot,raw_id,status,first_received,input_attempt,normalized_hash) VALUES($1,$2,$3,$4,$5,'pending',$6,$7,$8) ON CONFLICT(job,signature) DO UPDATE SET raw_id=excluded.raw_id,input_hash=excluded.input_hash,normalized_hash=excluded.normalized_hash,status='pending',input_attempt=excluded.input_attempt,processing_attempts=replay_receipts.processing_attempts+1,processed_at=NULL", &[&self.job,&signature,&hash,&slot,&raw_id,&now(),&self.attempt,&normalized_hash]).map_err(write_failed)?;
        tx.commit().map_err(write_failed)?;
        Ok(Retained::Pending(raw_id))
    }

    /// Called only after the shared storage output acknowledgment. A crash before
    /// this write leaves a pending receipt and deterministic retryable input.
    pub fn acknowledge(&mut self, raw_id: i64, result: &Result<usize>) -> Result<()> {
        let status = result.as_ref().map_or_else(|e| e.0, |_| "decoded");
        let n = self
            .client
            .execute(
                "UPDATE replay_receipts SET status=$1,processed_at=CASE WHEN $1='decoded' THEN EXTRACT(EPOCH FROM now())::bigint ELSE NULL END WHERE job=$2 AND raw_id=$3",
                &[&status, &self.job, &raw_id],
            )
            .map_err(write_failed)?;
        if n != 1 {
            return Err(Error("replay_receipt_missing"));
        }
        Ok(())
    }

    /// Charge every connection, including failed attempts, before network I/O.
    pub fn connection(&mut self) -> Result<()> {
        let n = self.client.execute("UPDATE replay_jobs SET connections_used=connections_used+1 WHERE id=$1 AND connections_used<grants*3", &[&self.job]).map_err(write_failed)?;
        if n != 1 {
            return Err(Error("replay_budget_exhausted"));
        }
        Ok(())
    }

    /// All terminal outcomes release ownership by consuming this reservation.
    /// Unused time remains charged conservatively; no crash-dependent refunds.
    pub fn finish(mut self, status: &'static str) -> Result<()> {
        self.client
            .execute(
                "UPDATE replay_attempts SET status=$1 WHERE id=$2 AND status='running'",
                &[&status, &self.attempt],
            )
            .map_err(write_failed)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{live::USDC, storage::pg::TestDatabase};

    #[test]
    fn persisted_message_byte_and_connection_limits_fail_closed() {
        let db = TestDatabase::new();
        let mut log = JobLog::open(&db.0).unwrap();
        log.create("limited", USDC, 100, 149).unwrap();
        let mut attempt = JobLog::reserve(&db.0, "limited", 60).unwrap();
        for _ in 0..3 {
            attempt.connection().unwrap();
        }
        assert_eq!(
            attempt.connection().unwrap_err().0,
            "replay_budget_exhausted"
        );
        db.0.test_client()
            .execute(
                "UPDATE replay_jobs SET messages_used=9999,bytes_used=67108860 WHERE id='limited'",
                &[],
            )
            .unwrap();
        assert!(matches!(
            attempt.retain(100, &[1; 64], b"wire").unwrap(),
            Retained::Pending(_)
        ));
        assert!(matches!(
            attempt.retain(100, &[1; 64], b"wire"),
            Err(Error("replay_budget_exhausted"))
        ));
        let status = log.status("limited").unwrap();
        assert_eq!(status["budget"]["messages_remaining"], 0);
        assert_eq!(status["budget"]["bytes_remaining"], 0);
        assert_eq!(status["receipts"]["unique_inputs"], 1);
    }

    #[test]
    fn raw_and_receipt_are_atomic_and_receipts_survive_pruning() {
        let db = TestDatabase::new();
        let mut log = JobLog::open(&db.0).unwrap();
        log.create("fifty", USDC, 100, 149).unwrap();
        let mut attempt = JobLog::reserve(&db.0, "fifty", 60).unwrap();
        db.0.test_client().batch_execute("CREATE FUNCTION fail_receipt() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER fail_receipt BEFORE INSERT ON replay_receipts FOR EACH ROW EXECUTE FUNCTION fail_receipt();").unwrap();
        assert!(attempt.retain(100, &[1; 64], b"wire").is_err());
        let mut sql = db.0.test_client();
        assert_eq!(
            sql.query_one("SELECT COUNT(*) FROM live_raw", &[])
                .unwrap()
                .get::<_, i64>(0),
            0
        );
        assert_eq!(log.status("fifty").unwrap()["budget"]["messages_used"], 1);
        sql.batch_execute("DROP TRIGGER fail_receipt ON replay_receipts")
            .unwrap();
        let Retained::Pending(raw) = attempt.retain(100, &[1; 64], b"wire").unwrap() else {
            panic!("pending input missing")
        };
        assert!(
            matches!(attempt.retain(100,&[1;64],b"wire").unwrap(),Retained::Pending(id) if id==raw)
        );
        attempt.acknowledge(raw, &Ok(1)).unwrap();
        sql.execute("DELETE FROM live_raw", &[]).unwrap();
        assert!(matches!(
            attempt.retain(100, &[1; 64], b"wire").unwrap(),
            Retained::AlreadyDecoded
        ));
        assert!(matches!(
            attempt.retain(101, &[1; 64], b"wire"),
            Err(Error("replay_input_conflict"))
        ));
        assert!(matches!(
            attempt.retain(150, &[2; 64], b"later").unwrap(),
            Retained::PastEnd
        ));
        assert_eq!(
            sql.query_one("SELECT COUNT(*) FROM replay_receipts", &[])
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        assert_eq!(log.status("fifty").unwrap()["coverage"], "unknown");
        let receipts = log.status("fifty").unwrap()["receipts"].clone();
        assert_eq!(receipts["decoded"], 1);
        assert_eq!(receipts["raw_still_retained"], 0);
    }

    #[test]
    fn migration_preserves_existing_inputs_and_old_binary_refuses_new_schema() {
        let db = TestDatabase::new();
        let mut sql = db.sql();
        super::super::pg::migrate(&mut sql, &super::super::pg::MIGRATIONS[..1]).unwrap();
        sql.execute(
            "INSERT INTO live_raw(slot,protobuf,status,received_at) VALUES(100,$1,'pending',1)",
            &[&b"retained".as_slice()],
        )
        .unwrap();
        super::super::pg::migrate(&mut sql, super::super::pg::MIGRATIONS).unwrap();
        assert_eq!(
            sql.query_one("SELECT protobuf FROM live_raw", &[])
                .unwrap()
                .get::<_, Vec<u8>>(0),
            b"retained"
        );
        assert_eq!(
            super::super::pg::migrate(&mut sql, &super::super::pg::MIGRATIONS[..1])
                .unwrap_err()
                .0,
            "storage_schema_newer_than_binary"
        );
    }

    #[test]
    fn frozen_scope_and_consumed_budget_survive_restart() {
        let db = TestDatabase::new();
        let mut log = JobLog::open(&db.0).unwrap();
        log.create("fifty", USDC, 100, 149).unwrap();
        let before = log.status("fifty").unwrap();
        log.create("fifty", USDC, 100, 149).unwrap();
        let after = log.status("fifty").unwrap();
        for field in [
            "job",
            "mint",
            "start",
            "end",
            "decoder_version",
            "created_at",
            "budget",
            "receipts",
        ] {
            assert_eq!(before[field], after[field]);
        }
        assert_eq!(
            log.create("fifty", USDC, 101, 150).unwrap_err().0,
            "replay_scope_conflict"
        );
        let mut reserved = JobLog::reserve(&db.0, "fifty", 60).unwrap();
        reserved.connection().unwrap();
        assert!(matches!(
            JobLog::reserve(&db.0, "fifty", 1),
            Err(Error("replay_job_busy"))
        ));
        drop(reserved); // Process/connection death cannot refund allowance.
        assert!(matches!(
            JobLog::reserve(&db.0, "fifty", 1),
            Err(Error("replay_budget_or_scope_rejected"))
        ));
        drop(log);
        let mut restarted = JobLog::open(&db.0).unwrap();
        assert_eq!(
            restarted.status("fifty").unwrap()["budget"]["seconds_remaining"],
            0
        );
        restarted.grant("fifty").unwrap();
        JobLog::reserve(&db.0, "fifty", 60)
            .unwrap()
            .finish("time_limit")
            .unwrap();
        let status = restarted.status("fifty").unwrap();
        assert_eq!(status["start"], 100);
        assert_eq!(status["end"], 149);
        assert_eq!(status["coverage"], "unknown");
        let rows =
            db.0.test_client()
                .query("SELECT status FROM replay_attempts ORDER BY id", &[])
                .unwrap();
        assert_eq!(rows[0].get::<_, String>(0), "interrupted");
        assert_eq!(rows[1].get::<_, String>(0), "time_limit");
    }

    #[test]
    fn invalid_scope_and_decoder_revision_cannot_consume_work() {
        let db = TestDatabase::new();
        let mut log = JobLog::open(&db.0).unwrap();
        assert!(log.create("bad/id", USDC, 100, 149).is_err());
        assert!(log.create("bad", USDC, 149, 100).is_err());
        log.create("good", USDC, 100, 149).unwrap();
        db.0.test_client()
            .execute(
                "UPDATE replay_jobs SET decoder='superseded' WHERE id='good'",
                &[],
            )
            .unwrap();
        assert!(JobLog::reserve(&db.0, "good", 60).is_err());
        assert_eq!(log.status("good").unwrap()["budget"]["seconds_charged"], 0);
        assert!(log.grant("good").is_err());
    }
}
