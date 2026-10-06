"""Demo: 3 simulated edge devices write hash-chained messages to the Tangle via iota-messages-api
and tell the auditor about each block. device-03 turns malicious after --attack-after messages.

    python -m scripts.simulate                # run until Ctrl+C
    python -m scripts.simulate --count 20 --interval 1
"""
import argparse
import itertools
import json
import random
import time

import httpx

from app import audit, config

AUDITOR = "http://localhost:8000"


class Device:
    def __init__(self, device_id, base_temp):
        self.id, self.base, self.seq, self.last_hash = device_id, base_temp, 0, audit.GENESIS

    def message(self, temp=None, ts=None, wrong_prev=False):
        self.seq += 1
        msg = {"device_id": self.id, "seq": self.seq,
               "timestamp": int(ts if ts is not None else time.time()),
               "metrics": {"temperature_c": round(temp if temp is not None else self.base + random.uniform(-0.6, 0.6), 2),
                           "humidity_pct": round(random.uniform(40, 55), 1)},
               "prev_hash": "f" * 64 if wrong_prev else self.last_hash}
        msg["hash"] = audit.compute_hash(msg)
        self.last_hash = msg["hash"]
        return msg


def send(client, message):
    r = client.post(f"{config.MESSAGES_API_URL}/upload", params={"node": config.HORNET_NODE_NAME},
                    json={"tag": f"iot.{message['device_id']}", "message": message}, timeout=15)
    r.raise_for_status()
    payload = r.json()
    block_id = json.loads(payload["return_payload"])["blockId"]
    aud = payload.get("auditor") or {}
    if aud.get("forwarded"):  # modified Messages API already told the auditor
        return block_id, aud.get("audit_result") or "?"
    # unmodified Messages API: tell the auditor ourselves
    a = client.post(f"{AUDITOR}/api/ingest",
                    json={"block_id": block_id, "tag": f"iot.{message['device_id']}", "message": message}, timeout=15)
    a.raise_for_status()
    return block_id, a.json()["status"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--count", type=int, default=0, help="rounds, 0 = forever")
    ap.add_argument("--attack-after", type=int, default=5, help="healthy rounds before device-03 turns malicious")
    args = ap.parse_args()

    good1, good2, evil = Device("device-01", 4.0), Device("device-02", 5.0), Device("device-03", 4.5)
    attacks = itertools.cycle(["tamper", "bad_chain", "past_ts", "spike"])

    with httpx.Client() as client:
        # Continue each device's chain where the auditor last saw it, so re-running the demo
        # does not look like a replay attack.
        try:
            known = {d["device_id"]: d for d in client.get(f"{AUDITOR}/api/devices", timeout=5).json()}
            for dev in (good1, good2, evil):
                d = known.get(dev.id)
                if d and d.get("last_seq") is not None:
                    dev.seq, dev.last_hash = d["last_seq"], d["last_hash"]
                    print(f"{dev.id}: resuming at seq {dev.seq + 1}")
        except Exception as e:  # noqa: BLE001
            print("could not read device state from the auditor, starting from seq 1:", e)

        for i in itertools.count(1):
            if args.count and i > args.count:
                break
            jobs = [("device-01", good1.message()), ("device-02", good2.message())]
            if i <= args.attack_after:
                jobs.append(("device-03", evil.message()))
            else:
                kind = next(attacks)
                if kind == "tamper":       # content altered after the hash was computed
                    m = evil.message(); m["metrics"]["temperature_c"] = 99.9
                elif kind == "bad_chain":  # forged link to a previous message
                    m = evil.message(wrong_prev=True)
                elif kind == "past_ts":    # backdated
                    m = evil.message(ts=time.time() - 3600)
                else:                      # implausible reading with a valid hash
                    m = evil.message(temp=60.0)
                jobs.append((f"device-03 [{kind}]", m))
            for name, m in jobs:
                bid, status = send(client, m)
                print(f"round {i:>3}  {name:<24} seq={m['seq']:<3} {status:<10} {bid[:18]}…")
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
