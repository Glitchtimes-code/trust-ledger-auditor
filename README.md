# Trust Ledger Auditor — Veles Hack 2026, Challenge 2 (O-CEI)

Audit dashboard for messages inserted into the aeriOS IOTA Tangle.
Look up any message on demand (by block id, tag or device id), see whether it is on the ledger,
and whether its content is intact, correctly chained and plausible. Each device gets a live trust score.

```
edge devices ──► Messages API (modified) ──► Hornet (private Tangle)
                       │                              │  GET /api/core/v2/blocks/{id} + /metadata
                       │ {block_id, tag, message}     ▼
                       └────────────────► Auditor: fetch → solid check → content check → audit rules
                                                       → SQLite index → WebSocket → dashboard
```

## Message format (what the auditor expects in `message`)

```json
{"device_id":"device-01","seq":7,"timestamp":1790000000,
 "metrics":{"temperature_c":4.1},"prev_hash":"<hash of message seq-1, 64 zeros for seq 0/1>",
 "hash":"<sha256 of canonical JSON of the message without `hash`>"}
```
Canonical JSON = `json.dumps(obj, sort_keys=True, separators=(",", ":"))` (see `app/audit.py`).
Messages sent with tag `iot.<device_id>`.

## Audit rules
| Code | Meaning | Status |
|---|---|---|
| SCHEMA_INVALID | not JSON / missing fields | INVALID |
| CONTENT_MISMATCH | what the publisher sent ≠ what the ledger holds (needs the modified Messages API) | TAMPERED |
| HASH_MISMATCH | content changed after hashing | TAMPERED |
| CHAIN_BROKEN | `prev_hash` ≠ previous message | TAMPERED |
| SEQ_REPLAY | `seq` not increasing | TAMPERED |
| TS_REGRESSION | timestamp earlier than previous | TAMPERED |
| SEQ_GAP / TS_FUTURE / OUT_OF_RANGE | missing messages, future timestamp, implausible metric | SUSPICIOUS |

Trust score starts at 100: flagged messages subtract points (capped at 40 per message), verified messages add 2.

## Challenge requirements → where they are met
| Requirement | Implementation |
|---|---|
| Send a message to the Tangle | Messages API (`messages_api/`, based on the Eclipse aeriOS API) |
| Parallel relational DB with enriched info (timestamps, tag…) | SQLite index (`app/store.py`): block id, tag, device, seq, message time, audit time, status, checks, parents, ledger state |
| Modify the Messages API to also send messages to our app | `messages_api/send_data.py` forwards `{block_id, tag, message}` to `POST /api/ingest` |
| Retrieve by ID, date, tag automatically via REST | `GET /api/search?q=&from=&to=&status=` and `GET /api/blocks` |
| Check the block is valid (solid) via block metadata | `GET /api/core/v2/blocks/{id}/metadata` → `isSolid`, `referencedByMilestoneIndex`; state is watched until confirmed |
| Check the content is the correct one via GET block | Ledger content is compared with what the publisher sent (CONTENT_MISMATCH) and re-read on demand (`/api/blocks/{id}/verify`) |
| Extras | hash-chain traceability per device, trust score, live alerts, incident-style feed |

## Run
Prerequisite: the private Tangle from `eclipse-aerios/iota-tangle` is up (Hornet API on :14265, network `iota-net`).

```bash
# 1. Auditor
python3 -m venv ~/venvs/audit && ~/venvs/audit/bin/pip install -r requirements.txt
cp .env.example .env
~/venvs/audit/bin/uvicorn app.main:app --port 8000        # dashboard: http://localhost:8000

# 2. Modified Messages API (replaces the stock one)
docker build -t iota_api_audit messages_api
docker rm -f iota_api
docker run -d --name iota_api --network iota-net -p 5555:5555 \
  --add-host=host.docker.internal:host-gateway \
  -e AUDITOR_URL=http://host.docker.internal:8000/api/ingest iota_api_audit

# 3. Demo: 3 devices, device-03 turns malicious
~/venvs/audit/bin/python -m scripts.simulate
```

Without Docker you can try everything against a mock node (it also mimics the forwarding Messages API):
```bash
uvicorn tools.mock_node:app --port 14266
NODE_URL=http://localhost:14266 MESSAGES_API_URL=http://localhost:14266 uvicorn app.main:app --port 8000
NODE_URL=http://localhost:14266 MESSAGES_API_URL=http://localhost:14266 python -m scripts.simulate
```

## How blocks get into the index
- The modified Messages API calls `POST /api/ingest {"block_id", "tag", "message"}` after every upload.
- Searching a block id the index has not seen reads it from the node and audits it on the spot.
- Optional: set `MQTT_HOST` (and publish port 1883 of the hornet container) to ingest every referenced block live.

## API
`GET /api/search?q=&from=&to=&status=&device_id=` (`from`/`to`: ISO 8601 or unix seconds) · `GET /api/blocks?status=&device_id=&tag=&from=&to=` ·
`GET /api/blocks/{id}` · `GET /api/blocks/{id}/verify` (re-reads the ledger, compares with the indexed copy, refreshes solid/confirmed state) ·
`GET /api/devices` · `GET /api/health` · `POST /api/ingest` · `POST /api/reset` · `WS /ws`

## Layout
`app/audit.py` rules and trust score · `app/pipeline.py` ingest flow · `app/sources/` Hornet REST + MQTT ·
`app/store.py` SQLite · `app/static/index.html` dashboard · `messages_api/` modified Messages API ·
`scripts/simulate.py` demo · `tools/mock_node.py` test double.

## License
Apache License 2.0 — see [LICENSE](LICENSE). (Matches the licenses of the aeriOS and IOTA repositories this project builds on;
adjust if the Veles Hack challenge description specifies a different one.)

## Built on
- [eclipse-aerios/iota-tangle](https://github.com/eclipse-aerios/iota-tangle) — private IOTA Tangle (Hornet + INX) deployment
- [eclipse-aerios/iota-messages-api](https://github.com/eclipse-aerios/iota-messages-api) — REST API that writes messages into the Tangle
- [iotaledger/hornet](https://github.com/iotaledger/hornet) — IOTA node (REST core API v2 is used to read blocks)

