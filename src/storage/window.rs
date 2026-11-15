//! Rolling-window reads: block-time cutoffs, keyset pages, coverage of the window's
//! transfer-bearing slot span, and SQL aggregates over known block times.

use super::{pg::Database, read_failed};
use crate::{Error, Result, decoder::DECODER_VERSION, model::Transfer};
use postgres::Client;
use serde_json::{Value, json};

/// The rolling window the API serves; rows older than this are never query-eligible.
pub const RETENTION_SECONDS: i64 = 30 * 24 * 60 * 60;
/// Covered and missing range lists are capped; their counts stay exact.
const RANGE_LIST_LIMIT: i64 = 100;
/// Rows whose transfer is hidden by an unresolved decoder repair are never reported.
const LIVE_ROWS: &str = "NOT EXISTS(SELECT 1 FROM decoder_repairs r WHERE r.signature=t.signature AND r.status='unresolved')";

/// A block-time window `[from, to)` resolved against `now`, the injected clock.
/// The cutoff is `from`; `to` is `now` for rolling windows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Window {
    pub now: i64,
    pub from: i64,
    pub to: i64,
}

impl Window {
    /// `seconds` ends at `now` and defaults to the 30-day window. An explicit range
    /// must be non-empty and may not start before the retained window.
    pub fn resolve(now: i64, seconds: Option<i64>, explicit: Option<(i64, i64)>) -> Result<Self> {
        let (from, to) = match (seconds, explicit) {
            (Some(_), Some(_)) => return Err(Error("invalid_window")),
            (None, Some((from, to))) if from < to => (from, to),
            (None, Some(_)) => return Err(Error("invalid_window")),
            (seconds, None) => {
                let seconds = seconds.unwrap_or(RETENTION_SECONDS);
                if !(1..=RETENTION_SECONDS).contains(&seconds) {
                    return Err(Error("invalid_window"));
                }
                (now - seconds, now)
            }
        };
        if from < now - RETENTION_SECONDS {
            return Err(Error("window_exceeds_retention"));
        }
        Ok(Self { now, from, to })
    }

    fn disclose(&self, value: &mut Value) {
        value["now"] = json!(self.now);
        value["cutoff"] = json!(self.from);
        value["window"] = json!({"from": self.from, "to": self.to});
    }
}

/// One keyset page of transfers in the window. `cursor` is the opaque `next_cursor` of an
/// earlier page; keys are (slot, signature, outer, inner), unique per transfer.
pub fn listing(
    db: &Database,
    mint: Option<&str>,
    window: Window,
    recent: bool,
    limit: usize,
    cursor: Option<&str>,
) -> Result<Value> {
    if !(1..=1000).contains(&limit) {
        return Err(Error("invalid_query_limit"));
    }
    let after = cursor.map(decode_cursor).transpose()?;
    let (slot, signature, outer, inner) = match &after {
        Some((slot, signature, outer, inner)) => (
            Some(*slot),
            Some(signature.as_str()),
            Some(*outer),
            Some(*inner),
        ),
        None => (None, None, None, None),
    };
    let (order, after_op) = if recent { ("DESC", "<") } else { ("ASC", ">") };
    let mut client = db.connect()?;
    let rows = client
        .query(
            &format!(
                "SELECT slot,signature,outer_index,inner_index,record_json FROM transfers t
                WHERE ($1::text IS NULL OR mint=$1) AND block_time>=$2 AND block_time<$3
                AND ($4::bigint IS NULL OR (slot,signature,outer_index,inner_index) {after_op} ($4::bigint,$5::text,$6::integer,$7::integer))
                AND {LIVE_ROWS}
                ORDER BY slot {order}, signature {order}, outer_index {order}, inner_index {order} LIMIT $8"
            ),
            &[
                &mint,
                &window.from,
                &window.to,
                &slot,
                &signature,
                &outer,
                &inner,
                &((limit + 1) as i64),
            ],
        )
        .map_err(read_failed)?;
    let has_more = rows.len() > limit;
    let shown = &rows[..rows.len().min(limit)];
    let next_cursor = shown
        .last()
        .filter(|_| has_more)
        .map(|r| encode_cursor(r.get(0), r.get(1), r.get(2), r.get(3)));
    let transfers = shown
        .iter()
        .map(|r| {
            serde_json::from_str::<Transfer>(r.get::<_, &str>(4))
                .map_err(|_| Error("stored_record_invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut out = json!({
        "limit": limit,
        "order": if recent { "newest_first" } else { "oldest_first" },
        "transfers": transfers,
        "next_cursor": next_cursor,
        "null_block_time": null_block_time(&mut client, mint)?,
        "coverage": coverage(&mut client, mint, window)?,
    });
    window.disclose(&mut out);
    Ok(out)
}

/// Known-time aggregates for one mint. `None` covers every stored row, including rows
/// with unknown block time; `Some` covers only rows whose block time lies in the window.
/// Sums are SQL NUMERIC, so they are exact and never overflow u64.
pub(crate) fn aggregate(
    client: &mut Client,
    mint: &str,
    time: Option<(i64, i64)>,
) -> Result<Value> {
    let (from, to) = time.map_or((None, None), |(from, to)| (Some(from), Some(to)));
    let row = client
        .query_one(
            &format!(
                "SELECT COUNT(*), COALESCE(SUM(amount_raw),0)::text, COALESCE(SUM(fee_raw),0)::text,
                COUNT(*) FILTER (WHERE amount_raw IS NULL), COUNT(*) FILTER (WHERE fee_raw IS NULL),
                COUNT(*) FILTER (WHERE j->>'confidential'='true'), COUNT(*) FILTER (WHERE j->>'decoder_version'<>$2)
                FROM (SELECT amount_raw, fee_raw, record_json::jsonb AS j FROM transfers t WHERE mint=$1
                AND ($3::bigint IS NULL OR block_time>=$3) AND ($4::bigint IS NULL OR block_time<$4) AND {LIVE_ROWS}) s"
            ),
            &[&mint, &DECODER_VERSION, &from, &to],
        )
        .map_err(read_failed)?;
    Ok(json!({
        "mint": mint,
        "transfer_count": row.get::<_, i64>(0),
        "target_decoder": DECODER_VERSION,
        "other_decoder_transfer_count": row.get::<_, i64>(6),
        "known_volume_raw": row.get::<_, String>(1),
        "known_fees_raw": row.get::<_, String>(2),
        "unknown_fee_count": row.get::<_, i64>(4),
        "unknown_amount_count": row.get::<_, i64>(3),
        "confidential_transfer_count": row.get::<_, i64>(5),
    }))
}

/// Window summary: known-time aggregates plus the window's coverage and null-time count.
pub fn window_summary(db: &Database, mint: &str, window: Window) -> Result<Value> {
    let mut client = db.connect()?;
    let mut out = aggregate(&mut client, mint, Some((window.from, window.to)))?;
    out["aggregate_scope"] = json!("known_block_time_in_window");
    out["null_block_time"] = json!(null_block_time(&mut client, Some(mint))?);
    out["coverage"] = coverage(&mut client, Some(mint), window)?;
    window.disclose(&mut out);
    Ok(out)
}

/// Stored rows with unknown block time. They have no time, so this is not window-bounded.
fn null_block_time(client: &mut Client, mint: Option<&str>) -> Result<i64> {
    Ok(client
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM transfers t WHERE ($1::text IS NULL OR mint=$1) AND block_time IS NULL AND {LIVE_ROWS}"
            ),
            &[&mint],
        )
        .map_err(read_failed)?
        .get(0))
}

/// Coverage of the slot span of the window's known-time transfers, from historical_slots
/// for this mint and decoder. No span, or no slot evidence, is `unknown`; never `observed empty`.
pub(crate) fn coverage(client: &mut Client, mint: Option<&str>, window: Window) -> Result<Value> {
    let Some(mint) = mint else {
        return Ok(
            json!({"status": "unknown", "reason": "mint_required", "decoder_version": DECODER_VERSION}),
        );
    };
    let span = client
        .query_one(
            &format!(
                "SELECT MIN(slot),MAX(slot) FROM transfers t WHERE mint=$1 AND block_time>=$2 AND block_time<$3 AND {LIVE_ROWS}"
            ),
            &[&mint, &window.from, &window.to],
        )
        .map_err(read_failed)?;
    let (low, high): (Option<i64>, Option<i64>) = (span.get(0), span.get(1));
    let (Some(low), Some(high)) = (low, high) else {
        return Ok(json!({
            "mint": mint, "decoder_version": DECODER_VERSION, "status": "unknown",
            "reason": "no_known_time_transfers_in_window", "slot_range": null,
            "slot_mapping": SLOT_MAPPING,
        }));
    };
    // Gaps-and-islands: consecutive complete/skipped slots form one covered run; the
    // gaps between runs (and at both edges, via sentinel rows) are missing. One statement.
    let row = client
        .query_one(
            "WITH scoped AS (
                SELECT slot,status FROM historical_slots WHERE mint=$1 AND decoder=$2 AND slot BETWEEN $3 AND $4
            ),
            covered AS (
                SELECT MIN(slot) AS lo, MAX(slot) AS hi FROM (
                    SELECT slot, slot-ROW_NUMBER() OVER (ORDER BY slot) AS run FROM scoped WHERE status IN ('complete','skipped')
                ) r GROUP BY run
            ),
            frame AS (
                SELECT lo,hi FROM covered
                UNION ALL SELECT $3::bigint-1, $3::bigint-1
                UNION ALL SELECT $4::bigint+1, $4::bigint+1
            ),
            gaps AS (
                SELECT gap_lo,gap_hi FROM (SELECT LAG(hi) OVER (ORDER BY lo)+1 AS gap_lo, lo-1 AS gap_hi FROM frame) g
                WHERE gap_lo<=gap_hi
            )
            SELECT
                (SELECT COALESCE(json_object_agg(status,n),'{}') FROM (SELECT status,COUNT(*) AS n FROM scoped GROUP BY status) s)::text,
                (SELECT COUNT(*) FROM covered)::bigint,
                (SELECT COUNT(*) FROM gaps)::bigint,
                (SELECT COALESCE(json_agg(json_build_object('from',lo,'to',hi) ORDER BY lo),'[]') FROM (SELECT lo,hi FROM covered ORDER BY lo LIMIT $5) c)::text,
                (SELECT COALESCE(json_agg(json_build_object('from',gap_lo,'to',gap_hi) ORDER BY gap_lo),'[]') FROM (SELECT gap_lo,gap_hi FROM gaps ORDER BY gap_lo LIMIT $5) g)::text,
                (SELECT COUNT(*) FROM scoped)::bigint",
            &[&mint, &DECODER_VERSION, &low, &high, &RANGE_LIST_LIMIT],
        )
        .map_err(read_failed)?;
    let parse = |text: &str| -> Result<Value> {
        serde_json::from_str(text).map_err(|_| Error("history_read_failed"))
    };
    let mut counts = parse(row.get(0))?;
    let covered_count: i64 = row.get(1);
    let missing_count: i64 = row.get(2);
    let covered = parse(row.get(3))?;
    let missing = parse(row.get(4))?;
    let evidence: i64 = row.get(5);
    let (status, reason) = if evidence == 0 {
        ("unknown", Some("no_coverage_evidence"))
    } else if missing_count == 0 {
        ("complete", None)
    } else {
        ("partial", None)
    };
    counts["unknown"] = json!(high - low + 1 - evidence);
    Ok(json!({
        "mint": mint,
        "decoder_version": DECODER_VERSION,
        "status": status,
        "reason": reason,
        "slot_range": {"from": low, "to": high},
        "slot_status_counts": counts,
        "covered_range_count": covered_count,
        "covered_ranges": covered,
        "missing_range_count": missing_count,
        "missing_ranges": missing,
        "missing_ranges_truncated": missing_count > RANGE_LIST_LIMIT,
        "slot_mapping": SLOT_MAPPING,
    }))
}

const SLOT_MAPPING: &str = "min..max slot of known-block-time transfers in the window; covers that span only, not the window edges";

/// Opaque cursor: hex of "slot:signature:outer:inner". Clients must not parse it.
fn encode_cursor(slot: i64, signature: &str, outer: i32, inner: i32) -> String {
    format!("{slot}:{signature}:{outer}:{inner}")
        .bytes()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn decode_cursor(cursor: &str) -> Result<(i64, String, i32, i32)> {
    let bytes = (0..cursor.len())
        .step_by(2)
        .map(|i| {
            cursor
                .get(i..i + 2)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        })
        .collect::<Option<Vec<u8>>>()
        .ok_or(Error("invalid_cursor"))?;
    let text = String::from_utf8(bytes).map_err(|_| Error("invalid_cursor"))?;
    let parts: Vec<&str> = text.split(':').collect();
    let &[slot, signature, outer, inner] = parts.as_slice() else {
        return Err(Error("invalid_cursor"));
    };
    Ok((
        slot.parse().map_err(|_| Error("invalid_cursor"))?,
        signature.to_owned(),
        outer.parse().map_err(|_| Error("invalid_cursor"))?,
        inner.parse().map_err(|_| Error("invalid_cursor"))?,
    ))
}
