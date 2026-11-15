"""Bounded mainnet time probes for an archive plan; these are not coverage evidence."""
import json
import time
from urllib.request import HTTPRedirectHandler, Request, build_opener


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Rpc:
    def __init__(self, endpoint):
        if not isinstance(endpoint, str) or not endpoint.startswith("https://"):
            raise ValueError("rpc_https_required")
        self.endpoint = endpoint
        self.deadline = time.monotonic() + 120
        self.requests = 0
        self.opener = build_opener(_NoRedirect())

    def __call__(self, method, params):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0 or self.requests >= 128:
            raise ValueError("time_probe_budget_exhausted")
        self.requests += 1
        request = Request(self.endpoint, json.dumps({"jsonrpc": "2.0", "id": 1,
                          "method": method, "params": params}).encode(),
                          {"Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=min(10, remaining)) as response:
                raw = response.read(4097)
            if len(raw) > 4096:
                raise ValueError("oversized")
            result = json.loads(raw)
        except Exception:
            raise ValueError("time_probe_request_failed") from None
        if not isinstance(result, dict) or result.get("jsonrpc") != "2.0" or result.get("id") != 1:
            raise ValueError("invalid_time_probe_response")
        if "error" in result:
            # A skipped slot is not coverage evidence. Probe a nearby position only.
            if method == "getBlockTime" and isinstance(result["error"], dict) and result["error"].get("code") in (-32007, -32009):
                return None
            code = result["error"].get("code") if isinstance(result["error"], dict) else None
            suffix = str(code) if type(code) is int else "unknown"
            raise ValueError("time_probe_rpc_error_" + method + "_" + suffix)
        if "result" not in result:
            raise ValueError("invalid_time_probe_response")
        return result["result"]


def discover_window(days, endpoint=None, *, rpc=None, now=None):
    """Start a full epoch before a known-time epoch older than the requested cutoff.

    Block timestamps are estimates and need not be strictly monotonic. This
    conservative probe selects work; only scanned block times and coverage can
    describe the resulting dataset. No slots-per-second approximation is used.
    """
    if type(days) is not int or not 1 <= days <= 30:
        raise ValueError("invalid_history_days")
    now = int(time.time()) if now is None else now
    if type(now) is not int or now <= days * 86400:
        raise ValueError("invalid_history_clock")
    rpc = rpc or Rpc(endpoint or "https://api.mainnet-beta.solana.com")
    if rpc("getGenesisHash", []) != "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d":
        raise ValueError("mainnet_required")
    reference = rpc("getSlot", [{"commitment": "finalized"}])
    schedule = rpc("getEpochSchedule", [])
    size = schedule.get("slotsPerEpoch") if isinstance(schedule, dict) else None
    if type(reference) is not int or not 0 <= reference <= 2**63 - 1 or type(size) is not int or not 1 <= size <= 1_000_000:
        raise ValueError("invalid_time_probe_reference")
    cutoff = now - days * 86400
    observations = []

    def known_time(first, last):
        for slot in range(first, min(first + 8, last + 1)):
            value = rpc("getBlockTime", [slot])
            if value is not None and (type(value) is not int or value < 0 or value > now + 600):
                raise ValueError("invalid_block_time")
            observations.append({"slot": slot, "block_time": value})
            if value is not None:
                return value
        raise ValueError("block_time_unavailable")

    # Validate reference time independently rather than deriving it from slots.
    reference_time = known_time(max(0, reference - 7), reference)
    if reference_time < cutoff:
        raise ValueError("finalized_reference_older_than_window")
    epoch = reference // size
    for _ in range(64):
        first = epoch * size
        timestamp = known_time(first, reference)
        if timestamp <= cutoff:
            start = max(0, first - size)
            if reference - start >= 16_000_000:
                raise ValueError("history_exceeds_bridge_span")
            return {"days": days, "cutoff_unix_seconds": cutoff,
                    "measured_at_unix_seconds": now, "reference_slot": reference,
                    "reference_block_time": reference_time, "start_slot": start,
                    "observations": observations,
                    "boundary_status": "conservative_time_probe_not_verified_coverage"}
        if epoch == 0:
            break
        epoch -= 1
    raise ValueError("historical_time_boundary_unavailable")
