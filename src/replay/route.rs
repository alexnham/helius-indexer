//! Measured source routing for a frozen job. A route says which source may serve
//! each slot; it is neither processing progress nor coverage.
use crate::{
    Error, Result, live,
    storage::{
        Database,
        replay::{JobLog, Route, valid_id},
    },
    telemetry::Telemetry,
};
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, Instant},
};

/// Split an inclusive range by measured availability. Archive serves slots up to
/// `archive_last`; replay serves `replay_first..=reference`. Unknown availability
/// (None) serves nothing, so it routes to `uncovered`, never to an empty range.
pub fn split(
    start: u64,
    end: u64,
    archive_last: Option<u64>,
    replay_first: Option<u64>,
    reference: u64,
) -> Vec<(u64, u64, &'static str)> {
    let kind = |slot: u64| {
        let archive = archive_last.is_some_and(|last| slot <= last);
        let replay = replay_first.is_some_and(|first| slot >= first) && slot <= reference;
        match (archive, replay) {
            (true, true) => "overlap",
            (true, false) => "archive",
            (false, true) => "replay",
            (false, false) => "uncovered",
        }
    };
    let mut cuts: Vec<u64> = [
        archive_last.and_then(|n| n.checked_add(1)),
        replay_first,
        reference.checked_add(1),
    ]
    .into_iter()
    .flatten()
    .filter(|&c| c > start && c <= end)
    .collect();
    cuts.sort_unstable();
    cuts.dedup();
    let mut segments = Vec::new();
    let mut from = start;
    for cut in cuts.into_iter().chain([end + 1]) {
        segments.push((from, cut - 1, kind(from)));
        from = cut;
    }
    segments
}

/// Measure replay first; probe the archive only when replay cannot serve the whole
/// job, so a recent-only job does no archive work.
pub(crate) fn measure_with(
    database: &Database,
    id: &str,
    replay: impl FnOnce() -> Result<(Option<u64>, u64)>,
    archive: impl FnOnce() -> Result<Option<u64>>,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    valid_id(id)?;
    let mut log = JobLog::open(database)?;
    let scope = log.scope(id)?;
    telemetry.set_context("replay_route", "laserstream", "historical");
    let (replay_first, reference) = replay().inspect_err(|e| {
        telemetry.emit("route_measured", "route", "failed", Some(e.0));
    })?;
    let replay_serves_all =
        replay_first.is_some_and(|f| f <= scope.start) && scope.end <= reference;
    let (archive_probed, archive_last, archive_error) = if replay_serves_all {
        (false, None, None)
    } else {
        match archive() {
            Ok(last) => (true, last, None),
            // An unavailable archive leaves its slots unknown; it does not fail routing.
            Err(e) => (true, None, Some(e.0)),
        }
    };
    let segments = split(
        scope.start,
        scope.end,
        archive_last,
        replay_first,
        reference,
    );
    let route = Route {
        replay_first,
        reference,
        archive_probed,
        archive_last,
        archive_error,
        segments,
    };
    let revision = log.record_route(id, &route)?;
    let uncovered = route.segments.iter().any(|s| s.2 == "uncovered");
    let mut event = telemetry.event(
        "route_measured",
        "route",
        if uncovered { "incomplete" } else { "success" },
        archive_error,
    );
    event["revision"] = json!(revision);
    event["archive_probed"] = json!(archive_probed);
    event["uncovered_segments"] =
        json!(route.segments.iter().filter(|s| s.2 == "uncovered").count());
    use std::io::Write;
    let _ = writeln!(std::io::stderr().lock(), "{event}");
    log.status(id)
}

/// Opt-in provider measurement. Without the archive feature the archive side is unknown.
pub fn measure(
    database: &Database,
    id: &str,
    cache: &Path,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    valid_id(id)?;
    let scope = JobLog::open(database)?.scope(id)?;
    live::validate_options(scope.start, 30)?;
    let replay = || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Error("runtime_failed"))?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut client = live::connect(&runtime, deadline, &live::Token::from_env()?)?;
        live::availability(&runtime, deadline, &mut client)
    };
    #[cfg(feature = "archive")]
    let archive = || {
        let mut context = Telemetry::default();
        let tip = crate::archive::tip(cache, &mut context)?;
        // Only an exact tip is a usable boundary; anything else stays unknown.
        if tip["tip_exact_within_probed_epochs"] != true {
            return Err(Error("archive_tip_unknown"));
        }
        Ok(tip["last_observed_archive_slot"].as_u64())
    };
    #[cfg(not(feature = "archive"))]
    let archive = || {
        let _ = cache;
        Err(Error("archive_feature_disabled"))
    };
    measure_with(database, id, replay, archive, telemetry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{live::USDC, storage::pg::TestDatabase};

    #[test]
    fn split_routes_by_measured_availability() {
        // Archive and replay windows overlap.
        assert_eq!(
            split(100, 149, Some(120), Some(110), 200),
            vec![
                (100, 109, "archive"),
                (110, 120, "overlap"),
                (121, 149, "replay")
            ]
        );
        // A gap between the windows is uncovered, not empty.
        assert_eq!(
            split(100, 149, Some(109), Some(130), 200),
            vec![
                (100, 109, "archive"),
                (110, 129, "uncovered"),
                (130, 149, "replay")
            ]
        );
        // Zero is a valid first available slot; None is unknown.
        assert_eq!(split(0, 9, None, Some(0), 9), vec![(0, 9, "replay")]);
        assert_eq!(split(0, 9, None, None, 9), vec![(0, 9, "uncovered")]);
        // Past the finalized reference replay cannot serve yet.
        assert_eq!(
            split(100, 149, None, Some(50), 139),
            vec![(100, 139, "replay"), (140, 149, "uncovered")]
        );
        assert_eq!(split(5, 5, Some(5), None, 0), vec![(5, 5, "archive")]);
        assert_eq!(
            split(0, u64::MAX - 1, Some(u64::MAX), None, u64::MAX),
            vec![(0, u64::MAX - 1, "archive")]
        );
    }

    #[test]
    fn recent_job_skips_the_archive_and_measurements_append_revisions() {
        let db = TestDatabase::new();
        super::super::create(&db.0, "recent", USDC, 100, 149).unwrap();
        let mut telemetry = Telemetry::default();
        let report = measure_with(
            &db.0,
            "recent",
            || Ok((Some(90), 500)),
            || panic!("recent-only job must not probe the archive"),
            &mut telemetry,
        )
        .unwrap();
        let routing = &report["routing"];
        assert_eq!(routing["revision"], 1);
        assert_eq!(routing["archive"]["probed"], false);
        assert_eq!(routing["state"], "routed_not_processed");
        assert_eq!(
            routing["segments"],
            json!([{"start":100,"end":149,"kind":"replay"}])
        );
        assert_eq!(report["coverage"], "unknown");

        // Replay window moved past the start: the archive is probed and a gap appears.
        let report = measure_with(
            &db.0,
            "recent",
            || Ok((Some(130), 600)),
            || Ok(Some(109)),
            &mut telemetry,
        )
        .unwrap();
        let routing = &report["routing"];
        assert_eq!(routing["revision"], 2);
        assert_eq!(routing["archive"]["last_available"], 109);
        assert_eq!(
            routing["segments"],
            json!([{"start":100,"end":109,"kind":"archive"},{"start":110,"end":129,"kind":"uncovered"},{"start":130,"end":149,"kind":"replay"}])
        );

        // A failed archive probe is recorded as unknown, not as an empty range.
        let report = measure_with(
            &db.0,
            "recent",
            || Ok((None, 600)),
            || Err(Error("provider_unavailable")),
            &mut telemetry,
        )
        .unwrap();
        let routing = &report["routing"];
        assert_eq!(routing["revision"], 3);
        assert!(routing["replay_first_available"].is_null());
        assert!(routing["archive"]["last_available"].is_null());
        assert_eq!(routing["archive"]["error_class"], "provider_unavailable");
        assert_eq!(
            routing["segments"],
            json!([{"start":100,"end":149,"kind":"uncovered"}])
        );

        // Earlier revisions are retained.
        let kept: i64 = db
            .sql()
            .query_one(
                "SELECT COUNT(*) FROM replay_route_segments WHERE job='recent'",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(kept, 5);
    }

    #[test]
    fn failed_replay_measurement_records_nothing() {
        let db = TestDatabase::new();
        super::super::create(&db.0, "recent", USDC, 100, 149).unwrap();
        let error = measure_with(
            &db.0,
            "recent",
            || Err(Error("replay_info_unavailable")),
            || panic!("not reached"),
            &mut Telemetry::default(),
        )
        .unwrap_err();
        assert_eq!(error, Error("replay_info_unavailable"));
        assert!(super::super::status(&db.0, "recent").unwrap()["routing"].is_null());
        assert!(matches!(
            measure_with(
                &db.0,
                "missing",
                || Ok((Some(0), 1)),
                || Ok(None),
                &mut Telemetry::default()
            ),
            Err(Error("replay_job_missing"))
        ));
    }
}
