"""Ingest a block: fetch from ledger -> audit -> persist -> broadcast."""
import asyncio
import time

import httpx

from . import audit, store
from .sources import hornet

_lock = asyncio.Lock()
_subscribers: set[asyncio.Queue] = set()


def subscribe() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue(maxsize=500)
    _subscribers.add(q)
    return q


def unsubscribe(q: asyncio.Queue) -> None:
    _subscribers.discard(q)


def _publish(event: dict) -> None:
    for q in list(_subscribers):
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            pass


async def ingest(client: httpx.AsyncClient, block_id: str, sent: dict | None = None) -> dict | None:
    """Returns the stored block (with `already_indexed`), or None if not found / not tagged data.

    `sent` = {"tag", "message"} as the publisher handed it to the Messages API; when given, it is
    compared with what the ledger holds (CONTENT_MISMATCH check)."""
    async with _lock:
        existing = store.get_block(block_id)
        if existing:
            existing["already_indexed"] = True
            return existing

        fetched = await hornet.fetch_block(client, block_id)
        if fetched is None:
            return None
        if fetched["payload_type"] != 5:
            return {"block_id": block_id, "status": "IGNORED", "already_indexed": False,
                    "note": f"Block has payload type {fetched['payload_type']}, not tagged data"}

        msg = fetched["message"]
        device_id = msg.get("device_id") if isinstance(msg, dict) and isinstance(msg.get("device_id"), str) else None
        dev = store.get_device(device_id) if device_id else None
        state = ({"last_seq": dev["last_seq"], "last_hash": dev["last_hash"], "last_ts": dev["last_ts"]}
                 if dev and dev["last_seq"] is not None else None)

        if fetched["json_error"]:
            status, checks, new_state, penalty = ("INVALID",
                [audit._check("SCHEMA_INVALID", False, "Payload is not valid JSON: " + fetched["json_error"])], state, 25)
        else:
            sent_ctx = {**sent, "ledger_tag": fetched["tag"]} if sent else None
            status, checks, new_state, penalty = audit.audit_message(msg, state, sent=sent_ctx)

        device_row = None
        score_after = None
        if device_id:
            prev = dev or {"score": 100.0, "total": 0, "verified": 0, "flagged": 0}
            score_after = audit.next_score(prev["score"], status, penalty)
            ns = new_state or state or {}
            device_row = {"device_id": device_id, "score": score_after,
                          "last_seq": ns.get("last_seq"), "last_hash": ns.get("last_hash"), "last_ts": ns.get("last_ts"),
                          "total": prev["total"] + 1,
                          "verified": prev["verified"] + (1 if status == "VERIFIED" else 0),
                          "flagged": prev["flagged"] + (0 if status == "VERIFIED" else 1)}

        block = {**fetched, "status": status, "checks": checks, "penalty": penalty, "score_after": score_after,
                 "device_id": device_id, "ingested_at": time.time(),
                 "seq": msg.get("seq") if isinstance(msg, dict) and isinstance(msg.get("seq"), int) else None,
                 "msg_ts": msg.get("timestamp") if isinstance(msg, dict) and isinstance(msg.get("timestamp"), (int, float)) else None}
        store.save_result(block, device_row)
        stored = store.get_block(block_id)
        stored["already_indexed"] = False
        if not (stored.get("block_state") or "").startswith("confirmed"):
            asyncio.create_task(_watch_state(client, block_id))
        _publish({"type": "block", "block": stored, "device": store.get_device(device_id) if device_id else None})
        return stored


async def _watch_state(client: httpx.AsyncClient, block_id: str) -> None:
    """A block is usually read before the coordinator references it. Poll its metadata until it is
    confirmed (or ~90 s passed) and push the new state to the dashboard."""
    last = None
    for _ in range(60):
        await asyncio.sleep(1.5)
        state, solid = await hornet.fetch_state(client, block_id)
        if state and state != last:
            last = state
            store.update_block_state(block_id, state, solid)
            _publish({"type": "state", "block_id": block_id, "block_state": state, "solid": solid})
        if state and state.startswith("confirmed"):
            return


async def verify_on_ledger(client: httpx.AsyncClient, block_id: str) -> dict:
    """Re-read the block from the node and compare with what we indexed (traceability proof)."""
    stored = store.get_block(block_id)
    fetched = await hornet.fetch_block(client, block_id)
    if fetched is None:
        return {"block_id": block_id, "on_ledger": False, "indexed": stored is not None,
                "match": False, "detail": "Node does not know this block"}
    same = bool(stored) and stored.get("ledger_fingerprint") == fetched.get("ledger_fingerprint")
    if stored and fetched.get("block_state") and fetched["block_state"] != stored.get("block_state"):
        store.update_block_state(block_id, fetched["block_state"], fetched.get("solid"))
    return {"block_id": block_id, "on_ledger": True, "indexed": stored is not None, "match": same,
            "block_state": fetched.get("block_state"), "solid": fetched.get("solid"), "parents": fetched.get("parents"),
            "detail": "Ledger content identical to indexed copy" if same else
                      ("Block not indexed yet" if not stored else "LEDGER CONTENT DIFFERS FROM INDEXED COPY")}
