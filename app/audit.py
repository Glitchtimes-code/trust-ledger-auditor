"""Audit rules: schema, integrity (hash), hash chain, ordering, time, range, trust score.

Expected device message (the JSON placed in the `message` field of iota-messages-api):
{
  "device_id": "fridge-01",
  "seq": 12,                      # strictly increasing per device
  "timestamp": 1790000000,        # unix seconds
  "metrics": {"temperature_c": 4.2, ...},
  "prev_hash": "<hash of previous message, or 64 zeros for the first one>",
  "hash": "<sha256 over canonical JSON of this message without the hash field>"
}
"""
import hashlib
import json
import time

from . import config

GENESIS = "0" * 64
REQUIRED = ("device_id", "seq", "timestamp", "metrics", "prev_hash", "hash")

# code -> (severity, penalty, explanation)
RULES = {
    "SCHEMA_INVALID": ("invalid", 25, "Payload does not match the expected schema"),
    "HASH_MISMATCH": ("tampered", 30, "Message content does not match its own hash (modified after signing)"),
    "CHAIN_BROKEN": ("tampered", 20, "prev_hash does not match the previous message of this device"),
    "SEQ_REPLAY": ("tampered", 15, "seq is not higher than the last accepted message (replay / reorder)"),
    "TS_REGRESSION": ("tampered", 15, "timestamp is earlier than the previous message of this device"),
    "CONTENT_MISMATCH": ("tampered", 30, "Content stored on the ledger differs from what the publisher sent"),
    "SEQ_GAP": ("suspicious", 10, "One or more messages are missing before this one"),
    "TS_FUTURE": ("suspicious", 10, "timestamp is in the future"),
    "OUT_OF_RANGE": ("suspicious", 10, "Metric outside the plausible range"),
}
RECOVERY = 2  # trust gained for every fully verified message


def canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def compute_hash(msg: dict) -> str:
    body = {k: v for k, v in msg.items() if k != "hash"}
    return hashlib.sha256(canonical(body)).hexdigest()


def _check(code: str, ok: bool, detail: str = "") -> dict:
    sev, pen, expl = RULES[code]
    return {"code": code, "ok": ok, "severity": sev, "penalty": 0 if ok else pen,
            "detail": detail or (expl if not ok else "OK")}


def validate_schema(msg) -> str | None:
    if not isinstance(msg, dict):
        return "message is not a JSON object"
    missing = [k for k in REQUIRED if k not in msg]
    if missing:
        return "missing fields: " + ", ".join(missing)
    if not isinstance(msg["device_id"], str) or not msg["device_id"]:
        return "device_id must be a non-empty string"
    if not isinstance(msg["seq"], int) or isinstance(msg["seq"], bool) or msg["seq"] < 0:
        return "seq must be a non-negative integer"
    if not isinstance(msg["timestamp"], (int, float)) or isinstance(msg["timestamp"], bool):
        return "timestamp must be a number (unix seconds)"
    if not isinstance(msg["metrics"], dict):
        return "metrics must be an object"
    if not isinstance(msg["prev_hash"], str) or not isinstance(msg["hash"], str):
        return "prev_hash and hash must be strings"
    return None


def audit_message(msg, state: dict | None, now: float | None = None, sent: dict | None = None):
    """Return (status, checks, new_state, penalty).

    state = {"last_seq", "last_hash", "last_ts"} of the device or None if unseen.
    """
    now = now if now is not None else time.time()
    checks: list[dict] = []

    err = validate_schema(msg)
    if err:
        checks.append(_check("SCHEMA_INVALID", False, err))
        return "INVALID", checks, state, checks[0]["penalty"]
    checks.append(_check("SCHEMA_INVALID", True, "Schema valid"))

    # Cross-check: what the publisher sent (via the Messages API) vs. what the ledger holds
    if sent is not None:
        same = canonical(sent.get("message")) == canonical(msg) and sent.get("tag") == sent.get("ledger_tag")
        checks.append(_check("CONTENT_MISMATCH", same,
                             "Content on the ledger is identical to what the publisher sent" if same
                             else "Ledger content/tag differs from the content/tag the publisher sent"))

    # Integrity
    expected = compute_hash(msg)
    checks.append(_check("HASH_MISMATCH", expected == msg["hash"],
                         "Hash matches content" if expected == msg["hash"]
                         else f"claimed {msg['hash'][:16]}… but content hashes to {expected[:16]}…"))

    # Ordering + chain
    if state is None:
        checks.append(_check("SEQ_REPLAY", True, "First message seen for this device (chain start)"))
    else:
        last_seq = state["last_seq"]
        if msg["seq"] <= last_seq:
            checks.append(_check("SEQ_REPLAY", False, f"seq {msg['seq']} <= last accepted seq {last_seq}"))
        else:
            checks.append(_check("SEQ_REPLAY", True, f"seq {msg['seq']} > {last_seq}"))
            if msg["seq"] > last_seq + 1:
                checks.append(_check("SEQ_GAP", False, f"expected seq {last_seq + 1}, got {msg['seq']}"))
            else:
                ok = msg["prev_hash"] == state["last_hash"]
                checks.append(_check("CHAIN_BROKEN", ok,
                                     "prev_hash links to previous message" if ok
                                     else f"prev_hash {msg['prev_hash'][:16]}… != previous hash {str(state['last_hash'])[:16]}…"))

    # Time
    ts = float(msg["timestamp"])
    if state is not None and ts < state["last_ts"] and msg["seq"] > state["last_seq"]:
        checks.append(_check("TS_REGRESSION", False, f"timestamp {int(ts)} < previous {int(state['last_ts'])}"))
    else:
        checks.append(_check("TS_REGRESSION", True, "Timestamp not earlier than previous"))
    if ts > now + config.MAX_FUTURE_SKEW_S:
        checks.append(_check("TS_FUTURE", False, f"timestamp is {int(ts - now)}s in the future"))

    # Range
    temp = msg["metrics"].get("temperature_c")
    if isinstance(temp, (int, float)) and not isinstance(temp, bool):
        ok = config.TEMP_MIN <= temp <= config.TEMP_MAX
        checks.append(_check("OUT_OF_RANGE", ok,
                             f"temperature_c {temp} within [{config.TEMP_MIN}, {config.TEMP_MAX}]" if ok
                             else f"temperature_c {temp} outside [{config.TEMP_MIN}, {config.TEMP_MAX}]"))

    failed = [c for c in checks if not c["ok"]]
    sevs = {c["severity"] for c in failed}
    status = ("TAMPERED" if "tampered" in sevs else "SUSPICIOUS" if "suspicious" in sevs else "VERIFIED")
    penalty = min(40, sum(c["penalty"] for c in failed))

    new_state = state
    if state is None or msg["seq"] > state["last_seq"]:
        new_state = {"last_seq": msg["seq"], "last_hash": msg["hash"], "last_ts": max(ts, state["last_ts"]) if state else ts}
    return status, checks, new_state, penalty


def next_score(score: float, status: str, penalty: int) -> float:
    if status == "VERIFIED":
        return min(100.0, score + RECOVERY)
    return max(0.0, score - penalty)
