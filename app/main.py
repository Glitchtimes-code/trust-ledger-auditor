import asyncio
import json
from datetime import datetime, timezone
from typing import Any
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel

from . import config, pipeline, store
from .sources import hornet, mqtt_source

STATIC = Path(__file__).parent / "static"
http: httpx.AsyncClient


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global http
    http = httpx.AsyncClient()
    store.conn()
    mqtt_source.start(asyncio.get_running_loop(), lambda bid: pipeline.ingest(http, bid))
    yield
    await http.aclose()


app = FastAPI(title="Trust Ledger Auditor", lifespan=lifespan)


class IngestRequest(BaseModel):
    block_id: str
    tag: str | None = None          # what the publisher sent (set by the modified Messages API)
    message: Any | None = None


def _to_ts(v: str | None) -> float | None:
    """Accept unix seconds or an ISO date/datetime (naive = UTC)."""
    if v in (None, ""):
        return None
    try:
        return float(v)
    except ValueError:
        pass
    try:
        d = datetime.fromisoformat(v)
    except ValueError:
        raise HTTPException(400, f"Invalid date: {v!r} (use ISO 8601 or unix seconds)")
    return (d if d.tzinfo else d.replace(tzinfo=timezone.utc)).timestamp()


def _check_id(block_id: str) -> str:
    if not hornet.BLOCK_ID_RE.match(block_id):
        raise HTTPException(400, "block_id must be 0x + 64 hex characters")
    return block_id.lower()


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/health")
async def health():
    return {"node": await hornet.node_health(http), "mqtt": bool(config.MQTT_HOST)}


@app.post("/api/ingest")
async def ingest(req: IngestRequest):
    sent = {"tag": req.tag, "message": req.message} if req.message is not None else None
    block = await pipeline.ingest(http, _check_id(req.block_id), sent)
    if block is None:
        raise HTTPException(404, "Block not found on the ledger node")
    return block


@app.get("/api/search")
async def search(q: str | None = None, date_from: str | None = Query(None, alias="from"),
                 date_to: str | None = Query(None, alias="to"), status: str | None = None,
                 device_id: str | None = None, limit: int = 100):
    """On-demand lookup. A block id is looked up on the ledger itself (and audited if new);
    anything else is matched against tag / device id / partial block id in the index, optionally
    narrowed by date range (`from`/`to`: ISO 8601 or unix seconds) and status."""
    q = (q or "").strip() or None
    if q and hornet.BLOCK_ID_RE.match(q):
        block = await pipeline.ingest(http, q.lower())
        if block is None:
            return {"query": q, "kind": "block_id", "found": False, "results": []}
        return {"query": q, "kind": "block_id", "found": True, "results": [block]}
    if not any([q, date_from, date_to, status, device_id]):
        raise HTTPException(400, "Provide q (block id, tag or device id) and/or from/to/status filters")
    results = store.query_blocks(q, status, device_id, None, _to_ts(date_from), _to_ts(date_to), min(limit, 500))
    return {"query": q, "kind": "filter", "found": bool(results), "results": results}


@app.get("/api/blocks")
def blocks(limit: int = 100, status: str | None = None, device_id: str | None = None, tag: str | None = None,
           date_from: str | None = Query(None, alias="from"), date_to: str | None = Query(None, alias="to")):
    return store.query_blocks(None, status, device_id, tag, _to_ts(date_from), _to_ts(date_to), min(limit, 500))


@app.get("/api/blocks/{block_id}")
def block(block_id: str):
    b = store.get_block(_check_id(block_id))
    if not b:
        raise HTTPException(404, "Not indexed. Use /api/search?q=<block id> to look it up on the ledger.")
    return b


@app.get("/api/blocks/{block_id}/verify")
async def verify(block_id: str):
    return await pipeline.verify_on_ledger(http, _check_id(block_id))


@app.get("/api/devices")
def devices():
    return store.list_devices()


@app.post("/api/reset")
def reset():
    store.reset()
    return {"ok": True}


@app.websocket("/ws")
async def ws(sock: WebSocket):
    await sock.accept()
    q = pipeline.subscribe()
    try:
        while True:
            await sock.send_text(json.dumps(await q.get()))
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        pipeline.unsubscribe(q)
