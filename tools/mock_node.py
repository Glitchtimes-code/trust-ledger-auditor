"""Tiny stand-in for iota-messages-api + Hornet REST, for testing without Docker.

    uvicorn tools.mock_node:app --port 14266
    NODE_URL=http://localhost:14266 MESSAGES_API_URL=http://localhost:14266 uvicorn app.main:app
"""
import codecs
import hashlib
import json
import os

import httpx

from fastapi import Body, FastAPI, HTTPException

app = FastAPI()
BLOCKS: dict[str, dict] = {}


@app.post("/upload")
def upload(body: dict = Body(...), node: str = "iota-hornet"):  # sync: runs in a worker thread, so the
    # call to the auditor (which reads back from this same server) cannot deadlock the event loop
    tag = "0x" + codecs.encode(body["tag"], "utf-8").hex()
    data = "0x" + codecs.encode(json.dumps(body["message"]), "utf-8").hex()
    bid = "0x" + hashlib.sha256((tag + data + str(len(BLOCKS))).encode()).hexdigest()
    BLOCKS[bid] = {"protocolVersion": 2, "parents": ["0x" + "11" * 32, "0x" + "22" * 32],
                   "payload": {"type": 5, "tag": tag, "data": data}, "nonce": "0"}
    result = {"status_code": 201, "return_payload": json.dumps({"blockId": bid}) + "\n"}
    auditor = os.getenv("AUDITOR_URL", "http://localhost:8000/api/ingest")
    if auditor:  # same behaviour as messages_api/send_data.py
        try:
            r = httpx.post(auditor, json={"block_id": bid, "tag": body["tag"], "message": body["message"]}, timeout=10)
            result["auditor"] = {"forwarded": r.is_success, "audit_result": r.json().get("status")}
        except Exception as e:  # noqa: BLE001
            result["auditor"] = {"forwarded": False, "reason": str(e)[:100]}
    return result


@app.get("/api/core/v2/blocks/{bid}")
def get_block(bid: str):
    if bid not in BLOCKS:
        raise HTTPException(404)
    return BLOCKS[bid]


@app.get("/api/core/v2/blocks/{bid}/metadata")
def meta(bid: str):
    if bid not in BLOCKS:
        raise HTTPException(404)
    return {"blockId": bid, "parents": BLOCKS[bid]["parents"], "isSolid": True, "referencedByMilestoneIndex": 1 if len(BLOCKS) % 2 == 0 else None, "ledgerInclusionState": "noTransaction", "whiteFlagIndex": 0}


@app.get("/api/core/v2/info")
def info():
    return {"status": {"isHealthy": True, "latestMilestone": {"index": 1}, "confirmedMilestone": {"index": 1}}}
