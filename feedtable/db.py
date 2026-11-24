"""SQLite schema and connection helpers.

Everything lives in *one* database file:

``records``
    The source table.  ``pk`` is the immutable primary key, ``route`` is the
    integer used by subscriptions, ``payload`` is the business content.
``subscriptions``
    Durable half-open range selections, the sequence boundary of the start
    view, and the durable consumption cursor (``acked_seq``).
``subscription_start_view``
    The immutable start view captured at subscription creation.
``commits`` / ``subscription_commits`` / ``commit_changes``
    The append-only notification stream.  One write transaction produces one
    ``commits`` row and one ``subscription_commits`` row per affected
    subscription; net per-pk changes live in ``commit_changes``.
``baseline_records`` / ``batches`` / ``batch_conditions`` / ``batch_writes``
    The batch journal, present once batch journaling is enabled.
    ``baseline_records`` is the immutable, non-revocable table as it stood at
    enable time; each registered batch keeps its preconditions and writes for
    re-evaluation on withdrawal.  Indexes on the ``pk`` columns let a
    withdrawal trace only the batches a key can actually influence.

One database and one write transaction is deliberate: data, subscription
definitions and pending notifications are committed or rolled back together,
so a client can never observe a partial result.  WAL mode plus
``synchronous=FULL`` makes a committed transaction durable even if the process
is killed and restarted immediately afterwards.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    pk      TEXT PRIMARY KEY,
    route   INTEGER NOT NULL,
    payload TEXT
);

CREATE INDEX IF NOT EXISTS records_route_pk ON records (route, pk);

CREATE TABLE IF NOT EXISTS feed_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT OR IGNORE INTO feed_meta (key, value) VALUES ('next_seq', '0');

CREATE TABLE IF NOT EXISTS subscriptions (
    name       TEXT PRIMARY KEY,
    lo         INTEGER NOT NULL,
    hi         INTEGER NOT NULL,
    start_lo   INTEGER NOT NULL,
    start_hi   INTEGER NOT NULL,
    start_seq  INTEGER NOT NULL,
    acked_seq  INTEGER,
    created_at REAL    NOT NULL,
    CHECK (hi >= lo),
    CHECK (start_hi >= start_lo)
);

CREATE TABLE IF NOT EXISTS subscription_meta (
    subscription TEXT NOT NULL,
    key          TEXT NOT NULL,
    value        TEXT NOT NULL,
    PRIMARY KEY (subscription, key)
);

CREATE TABLE IF NOT EXISTS subscription_start_view (
    subscription TEXT    NOT NULL,
    idx          INTEGER NOT NULL,
    pk           TEXT    NOT NULL,
    route        INTEGER NOT NULL,
    payload      TEXT,
    PRIMARY KEY (subscription, idx)
);

CREATE TABLE IF NOT EXISTS commits (
    seq        INTEGER PRIMARY KEY,
    created_at REAL NOT NULL,
    meta       TEXT NOT NULL,
    audit      TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS subscription_commits (
    subscription TEXT    NOT NULL,
    seq          INTEGER NOT NULL,
    range_lo     INTEGER NOT NULL,
    range_hi     INTEGER NOT NULL,
    change_count INTEGER NOT NULL,
    PRIMARY KEY (subscription, seq)
);

CREATE TABLE IF NOT EXISTS commit_changes (
    subscription TEXT    NOT NULL,
    seq          INTEGER NOT NULL,
    idx          INTEGER NOT NULL,
    kind         TEXT    NOT NULL CHECK (kind IN ('enter', 'update', 'exit')),
    pk           TEXT    NOT NULL,
    route        INTEGER,
    payload      TEXT,
    PRIMARY KEY (subscription, seq, idx)
);

-- Batch journal -------------------------------------------------------------
-- Enabling batch journaling snapshots the whole record table into
-- ``baseline_records``: that snapshot is the non-revocable starting point.
-- Every later record write must arrive through a registered batch.
CREATE TABLE IF NOT EXISTS baseline_records (
    pk      TEXT PRIMARY KEY,
    route   INTEGER NOT NULL,
    payload TEXT
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id      TEXT PRIMARY KEY,
    seq           INTEGER NOT NULL UNIQUE,   -- first-registration order
    status        TEXT NOT NULL CHECK (status IN ('registered', 'skipped', 'withdrawn')),
    fingerprint   TEXT NOT NULL,
    notify_seq    INTEGER,                   -- notification seq when effective
    created_at    REAL NOT NULL,
    last_change_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_conditions (
    batch_id TEXT    NOT NULL,
    idx      INTEGER NOT NULL,
    pk       TEXT    NOT NULL,
    kind     TEXT    NOT NULL CHECK (kind IN ('absent', 'matches')),
    route    INTEGER,
    payload  TEXT,
    PRIMARY KEY (batch_id, idx)
);
-- The withdrawal impact closure follows primary keys through the journal;
-- these two indexes make every step a primary-key lookup, never a scan.
CREATE INDEX IF NOT EXISTS batch_conditions_pk_idx
    ON batch_conditions (pk, batch_id);

CREATE TABLE IF NOT EXISTS batch_writes (
    batch_id  TEXT    NOT NULL,
    idx       INTEGER NOT NULL,
    pk        TEXT    NOT NULL,
    is_delete INTEGER NOT NULL CHECK (is_delete IN (0, 1)),
    route     INTEGER,
    payload   TEXT,
    PRIMARY KEY (batch_id, idx)
);
CREATE INDEX IF NOT EXISTS batch_writes_pk_idx
    ON batch_writes (pk, batch_id);
CREATE INDEX IF NOT EXISTS batch_writes_batch_idx
    ON batch_writes (batch_id, idx);
"""


def connect(path) -> sqlite3.Connection:
    """Open a connection configured for transactional, multi-process use."""
    path = str(path)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


class transaction:
    """Explicit ``BEGIN IMMEDIATE`` transaction context manager.

    ``BEGIN IMMEDIATE`` takes the write lock up front, so concurrent writers
    queue on the busy timeout instead of discovering a conflict halfway
    through.  Any exception rolls back the whole transaction.
    """

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            self.conn.execute("COMMIT")
        else:
            try:
                self.conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        return False


class read_transaction:
    """Read-only snapshot: every statement sees the same committed state.

    WAL mode lets readers proceed while a writer holds the write lock, so a
    delivery read never blocks a commit and vice versa.
    """

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self.conn.execute("BEGIN DEFERRED")
        # Force the read snapshot to be taken now, before the first query:
        # a deferred transaction that has not read yet can otherwise observe
        # a later commit part-way through the sequence.
        self.conn.execute("SELECT value FROM feed_meta WHERE key = 'next_seq'").fetchone()
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            self.conn.execute("COMMIT" if exc_type is None else "ROLLBACK")
        except sqlite3.Error:
            pass
        return False
