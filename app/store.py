"""SQLite index of blocks seen on the Tangle plus per-device trust state."""
import json
import sqlite3
import threading
import time
from pathlib import Path

from . import config

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS blocks (
  block_id TEXT PRIMARY KEY,
  tag TEXT, device_id TEXT, seq INTEGER, msg_ts REAL,
  status TEXT NOT NULL,
  checks TEXT, message TEXT, raw_data TEXT, parents TEXT,
  block_state TEXT, solid INTEGER, penalty INTEGER DEFAULT 0, score_after REAL,
  ingested_at REAL NOT NULL, ledger_fingerprint TEXT
);
CREATE INDEX IF NOT EXISTS idx_blocks_device ON blocks(device_id, ingested_at);
CREATE INDEX IF NOT EXISTS idx_blocks_tag ON blocks(tag);
CREATE TABLE IF NOT EXISTS devices (
  device_id TEXT PRIMARY KEY,
  score REAL NOT NULL, last_seq INTEGER, last_hash TEXT, last_ts REAL,
  total INTEGER DEFAULT 0, verified INTEGER DEFAULT 0, flagged INTEGER DEFAULT 0,
  last_seen REAL
);
"""


def conn() -> sqlite3.Connection:
    global _conn
    with _lock:
        if _conn is None:
            Path(config.DB_PATH).parent.mkdir(parents=True, exist_ok=True)
            _conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
            _conn.row_factory = sqlite3.Row
            _conn.executescript(SCHEMA)
            cols = [r[1] for r in _conn.execute("PRAGMA table_info(blocks)")]
            if "solid" not in cols:  # migrate databases created by an earlier version
                _conn.execute("ALTER TABLE blocks ADD COLUMN solid INTEGER")
            _conn.commit()
        return _conn


def _block(row) -> dict | None:
    if row is None:
        return None
    d = dict(row)
    for k in ("checks", "message", "parents"):
        if d.get(k):
            d[k] = json.loads(d[k])
    return d


def get_block(block_id: str) -> dict | None:
    with _lock:
        return _block(conn().execute("SELECT * FROM blocks WHERE block_id=?", (block_id,)).fetchone())


def query_blocks(q: str | None = None, status: str | None = None, device_id: str | None = None,
                 tag: str | None = None, date_from: float | None = None, date_to: float | None = None,
                 limit: int = 100) -> list[dict]:
    """Filter the index. Dates are unix seconds and apply to the time the block was audited."""
    sql, args = "SELECT * FROM blocks WHERE 1=1", []
    if q:
        sql += " AND (tag LIKE ? OR device_id LIKE ? OR block_id LIKE ?)"; args += [f"%{q}%"] * 3
    if status:
        sql += " AND status=?"; args.append(status.upper())
    if device_id:
        sql += " AND device_id=?"; args.append(device_id)
    if tag:
        sql += " AND tag LIKE ?"; args.append(f"%{tag}%")
    if date_from is not None:
        sql += " AND ingested_at>=?"; args.append(date_from)
    if date_to is not None:
        sql += " AND ingested_at<=?"; args.append(date_to)
    sql += " ORDER BY ingested_at DESC, rowid DESC LIMIT ?"; args.append(limit)
    with _lock:
        return [_block(r) for r in conn().execute(sql, args).fetchall()]


def list_blocks(limit=100, status=None, device_id=None, tag=None) -> list[dict]:
    return query_blocks(status=status, device_id=device_id, tag=tag, limit=limit)


def get_device(device_id: str) -> dict | None:
    with _lock:
        r = conn().execute("SELECT * FROM devices WHERE device_id=?", (device_id,)).fetchone()
    return dict(r) if r else None


def list_devices() -> list[dict]:
    with _lock:
        return [dict(r) for r in conn().execute("SELECT * FROM devices ORDER BY score ASC, device_id").fetchall()]


def save_result(block: dict, device: dict | None) -> None:
    with _lock:
        c = conn()
        c.execute(
            "INSERT OR REPLACE INTO blocks (block_id, tag, device_id, seq, msg_ts, status, checks, message, raw_data,"
            " parents, block_state, solid, penalty, score_after, ingested_at, ledger_fingerprint) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (block["block_id"], block.get("tag"), block.get("device_id"), block.get("seq"), block.get("msg_ts"),
             block["status"], json.dumps(block.get("checks")), json.dumps(block.get("message")),
             block.get("raw_data"), json.dumps(block.get("parents")), block.get("block_state"),
             None if block.get("solid") is None else int(block["solid"]),
             block.get("penalty", 0), block.get("score_after"), block["ingested_at"], block.get("ledger_fingerprint")))
        if device:
            c.execute(
                "INSERT OR REPLACE INTO devices (device_id, score, last_seq, last_hash, last_ts, total, verified, flagged, last_seen)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (device["device_id"], device["score"], device.get("last_seq"), device.get("last_hash"),
                 device.get("last_ts"), device["total"], device["verified"], device["flagged"], time.time()))
        c.commit()


def update_block_state(block_id: str, state: str | None, solid: bool | None = None) -> None:
    with _lock:
        c = conn()
        c.execute("UPDATE blocks SET block_state=?, solid=? WHERE block_id=?",
                  (state, None if solid is None else int(solid), block_id))
        c.commit()


def reset() -> None:
    with _lock:
        c = conn()
        c.execute("DELETE FROM blocks"); c.execute("DELETE FROM devices"); c.commit()
