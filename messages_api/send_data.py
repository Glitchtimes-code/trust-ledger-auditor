"""iota-messages-api (eclipse-aerios, Apache-2.0) — modified for the Trust Ledger Auditor.

Change vs upstream: after a block is accepted by Hornet, the API forwards the original
tag + message and the resulting blockId to the auditor (AUDITOR_URL), so the auditor can
(a) index it right away and (b) compare what was sent with what the ledger actually holds.
A failing/unreachable auditor never fails the upload.
"""
import codecs
import json
import os

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
app.debug = True

# Reachable from inside the container; host.docker.internal = the machine running the auditor.
AUDITOR_URL = os.getenv("AUDITOR_URL", "http://host.docker.internal:8000/api/ingest")


def forward_to_auditor(block_id, tag, message):
    if not AUDITOR_URL:
        return {"forwarded": False, "reason": "AUDITOR_URL not set"}
    try:
        r = requests.post(AUDITOR_URL, json={"block_id": block_id, "tag": tag, "message": message}, timeout=10)
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        return {"forwarded": r.ok, "auditor_status": r.status_code, "audit_result": body.get("status")}
    except Exception as e:  # noqa: BLE001
        print("auditor forward failed:", e)
        return {"forwarded": False, "reason": str(e)[:200]}


@app.route('/upload', methods=['POST'])
def post_clear():
    node = "http://" + request.args.get("node") + ":14265/api/core/v2/blocks"
    requestData = request.get_json()
    tag = requestData["tag"]
    message = requestData["message"]
    message_json = json.dumps(message)

    tag_hex = "0x" + codecs.encode(tag, 'utf-8').hex()
    message_hex = "0x" + codecs.encode(message_json, 'utf-8').hex()

    payload = json.dumps({
        "protocolVersion": 2,
        "payload": {"type": 5, "tag": tag_hex, "data": message_hex}
    })
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    try:
        response = requests.request("POST", node, headers=headers, data=payload)
    except Exception:  # noqa: BLE001
        return "Hornet node not found, check that the Hornet node exists.\n", 400

    print(response.status_code)
    print(response.text)

    result = {"status_code": response.status_code, "return_payload": response.text}
    if response.status_code in (200, 201):
        try:
            block_id = json.loads(response.text)["blockId"]
            result["auditor"] = forward_to_auditor(block_id, tag, message)
        except (ValueError, KeyError):
            pass
    return jsonify(result)


app.run(port=5555, host='0.0.0.0')
