"""Read blocks from the Hornet node REST API (core v2) and decode tagged-data payloads."""
import hashlib
import json
import re

import httpx

from .. import config

BLOCK_ID_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


def _unhex(h: str) -> bytes:
    return bytes.fromhex(h[2:] if h.startswith("0x") else h)


async def node_health(client: httpx.AsyncClient) -> dict:
    try:
        r = await client.get(f"{config.NODE_URL}/api/core/v2/info", timeout=4)
        r.raise_for_status()
        info = r.json()
        st = info.get("status", {})
        return {"ok": True, "node": config.NODE_URL,
                "healthy": st.get("isHealthy"),
                "latest_milestone": (st.get("latestMilestone") or {}).get("index"),
                "confirmed_milestone": (st.get("confirmedMilestone") or {}).get("index")}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "node": config.NODE_URL, "error": str(e)[:200]}


async def fetch_block(client: httpx.AsyncClient, block_id: str) -> dict | None:
    """Return a decoded view of the block, or None if the node does not know it."""
    r = await client.get(f"{config.NODE_URL}/api/core/v2/blocks/{block_id}", timeout=10)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    blk = r.json()
    payload = blk.get("payload") or {}
    out = {"block_id": block_id, "parents": blk.get("parents", []), "payload_type": payload.get("type"),
           "tag": None, "raw_data": None, "message": None, "json_error": None, "block_state": None}
    if payload.get("type") == 5:  # tagged data
        out["tag"] = _unhex(payload.get("tag", "0x")).decode("utf-8", "replace")
        raw = _unhex(payload.get("data", "0x")).decode("utf-8", "replace")
        out["raw_data"] = raw
        try:
            out["message"] = json.loads(raw)
        except ValueError as e:
            out["json_error"] = str(e)
        out["ledger_fingerprint"] = hashlib.sha256(
            (payload.get("tag", "") + payload.get("data", "")).encode()).hexdigest()
    state, solid = await fetch_state(client, block_id)
    out["block_state"], out["solid"] = state, solid
    return out


def describe_state(md: dict) -> tuple[str, bool]:
    """Turn Hornet block metadata into (human readable state, isSolid)."""
    solid = bool(md.get("isSolid"))
    idx = md.get("referencedByMilestoneIndex")
    if idx is not None:
        return f"confirmed (milestone {idx})", solid
    return ("solid (awaiting milestone)" if solid else "pending"), solid


async def fetch_state(client: httpx.AsyncClient, block_id: str) -> tuple[str | None, bool | None]:
    """GET block metadata; returns (state, isSolid) or (None, None) if unavailable."""
    try:
        m = await client.get(f"{config.NODE_URL}/api/core/v2/blocks/{block_id}/metadata", timeout=5)
        if m.status_code == 200:
            return describe_state(m.json())
    except Exception:  # noqa: BLE001
        pass
    return None, None
