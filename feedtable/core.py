"""Durable, local notification feed over a routed record table.

Public surface (see :class:`Feed`):

* :meth:`Feed.create_subscription` - choose a half-open integer route
  interval; receive the start view visible at that moment plus its boundary.
* :meth:`Feed.apply` - one atomic transaction that may insert/update/delete
  records and/or move one or more subscription intervals.  The net effect is
  computed from the complete before/after visible result and persisted as one
  ordered group per affected subscription.
* :meth:`Feed.pending` / :meth:`Feed.read_group` - read unacknowledged groups,
  exactly as stored (never recomputed from the current range).
* :meth:`Feed.ack` - advance the subscription's durable consumption cursor.

Why one SQLite database
-----------------------
Records, subscription definitions and the notification log are committed in a
single ``BEGIN IMMEDIATE`` transaction.  A crash before ``COMMIT`` rolls all of
it back; a crash after ``COMMIT`` leaves all of it durable (WAL +
``synchronous=FULL``).  A client can therefore never observe records without
their notifications, or notifications without their records.  No network is
involved; restarting the process and calling :meth:`pending` resumes the same
ordered sequence.

Read-cost rule
--------------
Consider moving subscription ``S`` from ``O`` to ``N``.  Rows can only change
visibility by being written by this transaction (point lookups by primary
key), or by lying in ``N \\ O`` (candidates to enter) or ``O \\ N`` (candidates
to exit).  Rows in ``O & N`` (visible before and after) and rows outside
``O | N`` are never scanned, because a range move alone cannot change their
visibility.  The union of the moved ranges is *not* scanned wholesale.  Every
commit returns a :class:`ReadAudit` listing each interval scanned, the exact
SQL, its query plan, and the number of rows the statement yielded, plus the
number of point lookups and the intervals deliberately not read.  The audit is
what makes the read-cost claim checkable.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Iterable, Optional

from .db import connect, init_db, read_transaction, transaction
from .intervals import Interval, subtract

ENTER = "enter"
UPDATE = "update"
EXIT = "exit"
KINDS = (ENTER, UPDATE, EXIT)

_UNSET = object()

#: Sentinel for "no row exists at this key at this point in the replay".  It is
#: distinct from a real row whose payload is ``NULL``.
_ABSENT = object()

#: The only range scan shape this module is allowed to run.  ``INDEXED BY``
#: pins the plan so a schema change cannot silently turn it into a full scan.
_SEGMENT_SQL = (
    "SELECT pk, route, payload FROM records INDEXED BY records_route_pk "
    "WHERE route >= ? AND route < ? ORDER BY route, pk"
)
_START_VIEW_SQL = _SEGMENT_SQL

#: Point lookups used while evaluating a batch against the live record table.
_RECORD_POINT_SQL = "SELECT pk, route, payload FROM records WHERE pk = ?"

#: Batch-journal lookups.  Every one is a primary-key / indexed equality lookup:
#: withdrawal tracing is a closure of key follow-ups, never a journal scan.
_BATCH_BY_ID_SQL = (
    "SELECT batch_id, seq, status, fingerprint, notify_seq, created_at, "
    "last_change_at FROM batches WHERE batch_id = ?"
)
_CONDITIONS_OF_SQL = (
    "SELECT idx, pk, kind, route, payload FROM batch_conditions "
    "WHERE batch_id = ? ORDER BY idx"
)
_WRITES_OF_SQL = (
    "SELECT idx, pk, is_delete, route, payload FROM batch_writes "
    "WHERE batch_id = ? ORDER BY idx"
)
#: Batches that reference a key in the ``IN (...)`` list, either as a
#: precondition or as a write.  Both supporting indexes lead with ``pk``, so
#: the closure expansion stays an indexed lookup as it grows; forward-only and
#: already-reached filtering is done in Python against the numeric ``seq``.
_COND_REFS_SQL = (
    "SELECT DISTINCT batch_id FROM batch_conditions WHERE pk IN ({marks})"
)
_WRITE_REFS_SQL = (
    "SELECT DISTINCT batch_id FROM batch_writes WHERE pk IN ({marks})"
)
_BASELINE_POINT_SQL = (
    "SELECT pk, route, payload FROM baseline_records WHERE pk = ?"
)
_JOURNAL_EVENTS_SQL = (
    "SELECT b.seq AS bseq, w.batch_id, b.status, w.idx, w.pk, w.is_delete, "
    "w.route, w.payload FROM batch_writes AS w "
    "JOIN batches AS b ON b.batch_id = w.batch_id "
    "WHERE w.pk IN ({marks}) ORDER BY b.seq, w.idx"
)


def canonical_payload(payload):
    """Normalise business content to the stored ``payload`` text."""
    if payload is None:
        return None
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# --------------------------------------------------------------------------
# Value objects
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class Change:
    """One net change to one primary key in one commit."""

    kind: str
    pk: str
    route: Optional[int]
    payload: Optional[str]

    def to_dict(self) -> dict:
        return {"kind": self.kind, "pk": self.pk, "route": self.route, "payload": self.payload}

    @classmethod
    def from_row(cls, row) -> "Change":
        return cls(row["kind"], row["pk"], row["route"], row["payload"])


@dataclass(frozen=True)
class RowSpec:
    """A record write.  ``delete=True`` removes the record.

    A non-delete spec requires ``route``; ``payload`` defaults to ``None``.
    """

    pk: str
    route: Optional[int] = None
    payload: object = _UNSET
    delete: bool = False

    @classmethod
    def delete_pk(cls, pk: str) -> "RowSpec":
        return cls(pk=pk, delete=True)

    def __post_init__(self):
        if not isinstance(self.pk, str):
            raise TypeError("pk must be a string")
        if self.delete:
            if self.route is not None or self.payload is not _UNSET:
                raise ValueError(f"delete spec for {self.pk!r} cannot carry route/payload")
            return
        if self.route is None:
            raise ValueError(f"upsert spec for {self.pk!r} needs a route")
        if isinstance(self.route, bool) or not isinstance(self.route, int):
            raise ValueError(f"route for {self.pk!r} must be an integer")

    @property
    def payload_value(self):
        return None if self.payload is _UNSET else canonical_payload(self.payload)


@dataclass(frozen=True)
class MoveSpec:
    """Move a subscription's half-open interval."""

    subscription: str
    lo: int
    hi: int

    def __post_init__(self):
        Interval(self.lo, self.hi)  # validates bounds

    @property
    def interval(self) -> Interval:
        return Interval(self.lo, self.hi)


@dataclass
class ScanAudit:
    """One range scan over the source table."""

    label: str
    interval: tuple
    sql: str
    plan: list
    rows: int                     # rows the SQL statement yielded
    excluded_written: int = 0     # of those, rows already known from this txn's writes
    for_subscriptions: list = field(default_factory=list)

    @property
    def candidates(self) -> int:
        return self.rows - self.excluded_written

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "interval": list(self.interval),
            "sql": self.sql,
            "plan": list(self.plan),
            "rows": self.rows,
            "excluded_already_written": self.excluded_written,
            "candidates": self.candidates,
            "for_subscriptions": list(self.for_subscriptions),
        }


@dataclass
class ReadAudit:
    """Checkable account of the source-table reads performed by one commit.

    ``scanned`` lists every range scan with the SQL, its SQLite query plan and
    the row counts.  ``allowed`` and ``retained`` make the read-cost rule
    mechanically checkable via :meth:`violations`.
    """

    scans: list = field(default_factory=list)
    point_lookups: int = 0
    allowed: dict = field(default_factory=dict)   # sub -> [(lo, hi), ...]
    retained: dict = field(default_factory=dict)  # sub -> (lo, hi) or None

    @property
    def rows_scanned(self) -> int:
        return sum(s.rows for s in self.scans)

    @property
    def candidate_rows(self) -> int:
        """Rows read as candidates for entering/exiting (excluding writes)."""
        return sum(s.candidates for s in self.scans)

    def violations(self) -> list:
        """Scans that touched a region a move was not allowed to read.

        For every subscription a scan is attributed to, the scanned interval
        must fit inside one of that subscription's added/removed segments.
        A row in the retained intersection (or outside both ranges) can never
        be read, so any overlap here is a bug.
        """
        bad = []
        for scan in self.scans:
            lo, hi = scan.interval
            for sub in scan.for_subscriptions:
                segs = self.allowed.get(sub, [])
                if not any(lo >= a and hi <= b for a, b in segs):
                    bad.append({"scan": scan.to_dict(), "subscription": sub})
        return bad

    def to_dict(self) -> dict:
        bad = self.violations()
        return {
            "scanned": [s.to_dict() for s in self.scans],
            "rows_scanned": self.rows_scanned,
            "candidate_rows": self.candidate_rows,
            "point_lookups": self.point_lookups,
            "stable_rows_read": len(bad),
            "violations": bad,
            "allowed_segments": {k: [list(iv) for iv in v] for k, v in self.allowed.items()},
            "retained_not_scanned": {
                k: (list(v) if v is not None else None) for k, v in self.retained.items()
            },
        }


@dataclass
class CommitResult:
    seq: int
    changes: dict = field(default_factory=dict)  # subscription -> list[Change]
    audit: ReadAudit = field(default_factory=ReadAudit)
    committed: bool = True

    def to_dict(self) -> dict:
        return {
            "seq": self.seq,
            "committed": self.committed,
            "changes": {s: [c.to_dict() for c in cs] for s, cs in self.changes.items()},
            "audit": self.audit.to_dict(),
        }


@dataclass
class StartView:
    subscription: str
    lo: int
    hi: int
    seq: int
    rows: list

    def to_dict(self) -> dict:
        return {
            "kind": "start_view",
            "subscription": self.subscription,
            "seq": self.seq,
            "range": [self.lo, self.hi],
            "boundary": {"acked_through": self.seq, "next": self.seq + 1},
            "rows": self.rows,
        }


@dataclass
class Batch:
    subscription: str
    seq: int
    range_lo: int
    range_hi: int
    changes: list

    def to_dict(self) -> dict:
        return {
            "kind": "commit",
            "subscription": self.subscription,
            "seq": self.seq,
            "range": [self.range_lo, self.range_hi],
            "changes": [c.to_dict() for c in self.changes],
        }


# --------------------------------------------------------------------------
# The feed
# --------------------------------------------------------------------------
class Feed:
    def __init__(self, path):
        self.path = str(path)
        self.conn = connect(self.path)
        init_db(self.conn)

    # -- lifecycle ---------------------------------------------------------
    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # -- helpers -----------------------------------------------------------
    @staticmethod
    def _row_dict(row) -> dict:
        return {"pk": row["pk"], "route": row["route"], "payload": row["payload"]}

    @staticmethod
    def _sub_row(conn, name):
        row = conn.execute(
            "SELECT name, lo, hi, start_lo, start_hi, start_seq, acked_seq "
            "FROM subscriptions WHERE name = ?",
            (name,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown subscription {name!r}")
        return row

    @staticmethod
    def _all_subs(conn):
        return conn.execute(
            "SELECT name, lo, hi, start_lo, start_hi, start_seq, acked_seq "
            "FROM subscriptions ORDER BY name"
        ).fetchall()

    @staticmethod
    def _head(conn) -> int:
        row = conn.execute("SELECT value FROM feed_meta WHERE key = 'next_seq'").fetchone()
        return int(row["value"])

    @staticmethod
    def _meta_value(conn, key, default=None):
        row = conn.execute(
            "SELECT value FROM feed_meta WHERE key = ?", (key,)
        ).fetchone()
        return default if row is None else row["value"]

    def batches_enabled(self) -> bool:
        """Whether batch journaling is enabled on this database."""
        return self._meta_value(self.conn, "batch_mode") == "on"

    @staticmethod
    def _read_audit_plan(conn):
        return [
            r["detail"] for r in conn.execute("EXPLAIN QUERY PLAN " + _SEGMENT_SQL, (0, 0))
        ]

    # -- reads -------------------------------------------------------------
    def head(self) -> int:
        return self._head(self.conn)

    def subscription(self, name: str) -> dict:
        s = self._sub_row(self.conn, name)
        return {
            "name": s["name"],
            "lo": s["lo"],
            "hi": s["hi"],
            "range": [s["lo"], s["hi"]],
            "start_range": [s["start_lo"], s["start_hi"]],
            "start_seq": s["start_seq"],
            "acked_seq": s["acked_seq"],
            "start_view_pending": s["acked_seq"] is None or s["acked_seq"] < s["start_seq"],
        }

    def subscriptions(self) -> list:
        return [self.subscription(s["name"]) for s in self._all_subs(self.conn)]

    def start_view(self, name: str) -> StartView:
        with read_transaction(self.conn) as conn:
            sub = self._sub_row(conn, name)
            rows = conn.execute(
                "SELECT pk, route, payload FROM subscription_start_view "
                "WHERE subscription = ? ORDER BY idx",
                (name,),
            ).fetchall()
        return StartView(
            name, sub["start_lo"], sub["start_hi"], sub["start_seq"],
            [self._row_dict(r) for r in rows],
        )

    def cursor(self, subscription: str) -> Optional[int]:
        """Last acknowledged seq; ``None`` while the start view is pending."""
        return self._sub_row(self.conn, subscription)["acked_seq"]

    # -- subscription creation --------------------------------------------
    def create_subscription(self, name: str, lo: int, hi: int, *,
                            meta: Optional[dict] = None) -> StartView:
        """Create a subscription and atomically capture its start view.

        The start view contains every row visible in ``[lo, hi)`` at this
        instant.  Its ``seq`` is the boundary: commits with a greater sequence
        number are what the client consumes next.  The view stays pending
        until :meth:`ack` reaches at least that boundary, so a crash before or
        after delivery replays it without loss.

        The creation range is remembered separately from the live range, so a
        client may move the interval before it has consumed the start view.
        """
        interval = Interval(lo, hi)
        with transaction(self.conn) as conn:
            if conn.execute("SELECT 1 FROM subscriptions WHERE name = ?", (name,)).fetchone():
                raise ValueError(f"subscription {name!r} already exists")
            head = self._head(conn)
            conn.execute(
                "INSERT INTO subscriptions "
                "(name, lo, hi, start_lo, start_hi, start_seq, acked_seq, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
                (name, interval.lo, interval.hi, interval.lo, interval.hi, head, time.time()),
            )
            rows = []
            if not interval.empty:
                rows = conn.execute(_START_VIEW_SQL, (interval.lo, interval.hi)).fetchall()
                conn.executemany(
                    "INSERT INTO subscription_start_view "
                    "(subscription, idx, pk, route, payload) VALUES (?, ?, ?, ?, ?)",
                    [(name, i, r["pk"], r["route"], r["payload"]) for i, r in enumerate(rows)],
                )
            if meta:
                conn.execute(
                    "INSERT OR REPLACE INTO subscription_meta (subscription, key, value) "
                    "VALUES (?, 'create_meta', ?)",
                    (name, canonical_payload(meta)),
                )
        return StartView(
            name, interval.lo, interval.hi, head, [self._row_dict(r) for r in rows]
        )

    # -- the one atomic write transaction ----------------------------------
    def apply(
        self,
        writes: Iterable[RowSpec] = (),
        moves: Iterable[MoveSpec] = (),
        *,
        meta: Optional[dict] = None,
    ) -> CommitResult:
        """Apply record writes and/or range moves as one transaction.

        Semantics come from the before and after *visible result*:

        * not visible before, visible after -> ``enter``
        * visible before, not visible after -> ``exit``
        * visible both and the delivered row differs -> ``update``
        * otherwise no change.

        Each pk yields at most one net change per subscription per commit.
        A row that stays visible while the range moves is neither read nor
        announced, so it is never "deleted then re-created".

        If a transaction contains several writes for one pk, the last one
        wins, so delete-then-reinsert collapses to whatever is visible at the
        end - still exactly one net change.
        """
        # Collapse repeated writes: last spec for a pk wins.
        final: dict[str, RowSpec] = {}
        for w in writes:
            final.pop(w.pk, None)
            final[w.pk] = w
        writes = list(final.values())

        move_map: dict[str, Interval] = {}
        for m in moves:
            if m.subscription in move_map:
                raise ValueError(
                    f"subscription {m.subscription!r} moved twice in one transaction"
                )
            move_map[m.subscription] = m.interval

        # Once batch journaling is on, record writes only enter through
        # submit_batch.  Range moves (and read/ack) keep using this entry.
        if writes and self.batches_enabled():
            raise RuntimeError(
                "batch journaling is enabled: record writes must be submitted "
                "via Feed.submit_batch; apply() may only move subscription ranges"
            )

        audit = ReadAudit()
        with transaction(self.conn) as conn:
            subs = {r["name"]: r for r in self._all_subs(conn)}
            for name in move_map:
                if name not in subs:
                    raise KeyError(f"unknown subscription {name!r}")

            # (1) Before images of written rows: one point lookup each.
            before = {}
            for w in writes:
                row = conn.execute(
                    "SELECT pk, route, payload FROM records WHERE pk = ?", (w.pk,)
                ).fetchone()
                audit.point_lookups += 1
                if row is not None:
                    before[w.pk] = row

            # (2) Apply writes.  After images come straight from the specs:
            # one source-table read per written pk, never two.
            after = {}
            for w in writes:
                if w.delete:
                    conn.execute("DELETE FROM records WHERE pk = ?", (w.pk,))
                else:
                    conn.execute(
                        "INSERT INTO records (pk, route, payload) VALUES (?, ?, ?) "
                        "ON CONFLICT(pk) DO UPDATE SET route = excluded.route, "
                        "payload = excluded.payload",
                        (w.pk, w.route, w.payload_value),
                    )
                    after[w.pk] = {"pk": w.pk, "route": w.route, "payload": w.payload_value}

            # (3) Apply range moves.
            for name, interval in move_map.items():
                conn.execute(
                    "UPDATE subscriptions SET lo = ?, hi = ? WHERE name = ?",
                    (interval.lo, interval.hi, name),
                )

            # (4) Net changes per subscription from before/after visibility.
            changes_by_sub = self._net_changes_for_subs(
                conn, subs, move_map, before, after, set(final), audit
            )

            if not changes_by_sub:
                # Nothing observable happened.  Record writes still commit,
                # but no sequence number and no group are allocated: a client
                # has nothing to acknowledge.
                return CommitResult(self._head(conn), {}, audit, committed=False)

            seq = self._store_commit(conn, changes_by_sub, meta, move_map, subs, audit)
            return CommitResult(seq, changes_by_sub, audit, committed=True)

    def _net_changes_for_subs(
        self, conn, subs, move_map, before, after, written_pks, audit
    ):
        """Compute the per-subscription net change groups for one operation.

        ``before`` / ``after`` map pk to a row dict (``pk``/``route``/
        ``payload``) for every row the operation itself wrote.  Visibility is
        decided from those images plus bounded segment scans of the regions a
        range move adds or removes; retained and out-of-range rows are never
        read.  The same routine serves ordinary :meth:`apply` transactions,
        effective batch submissions and withdrawal corrections (the latter
        pass an empty ``move_map``).
        """
        changes_by_sub: dict[str, list[Change]] = {}
        segment_cache: dict[tuple, dict] = {}
        planned = self._read_audit_plan(conn)

        def segment_rows(seg: Interval, label: str, for_sub: str) -> dict:
            key = (seg.lo, seg.hi)
            cached = segment_cache.get(key)
            if cached is None:
                scan = ScanAudit(label, key, _SEGMENT_SQL, planned, 0, 0, [])
                rows = conn.execute(_SEGMENT_SQL, (seg.lo, seg.hi)).fetchall()
                scan.rows = len(rows)
                scan.excluded_written = sum(1 for r in rows if r["pk"] in written_pks)
                cached = {
                    "rows": {r["pk"]: r for r in rows if r["pk"] not in written_pks},
                    "audit": scan,
                }
                segment_cache[key] = cached
                audit.scans.append(scan)
            if for_sub not in cached["audit"].for_subscriptions:
                cached["audit"].for_subscriptions.append(for_sub)
            return cached["rows"]

        for name, sub in subs.items():
            old_iv = Interval(sub["lo"], sub["hi"])
            new_iv = move_map.get(name, old_iv)

            if new_iv == old_iv:
                added, removed = [], []
                audit.retained.setdefault(name, None)
            else:
                added = subtract(new_iv, old_iv)
                removed = subtract(old_iv, new_iv)
                audit.allowed[name] = [(s.lo, s.hi) for s in added + removed]
                inter_lo, inter_hi = max(old_iv.lo, new_iv.lo), min(old_iv.hi, new_iv.hi)
                audit.retained[name] = (
                    (inter_lo, inter_hi) if inter_lo < inter_hi else None
                )

            visible_before = {
                pk: row for pk, row in before.items() if old_iv.contains(row["route"])
            }
            visible_after = {
                pk: row for pk, row in after.items() if new_iv.contains(row["route"])
            }

            for seg in added:
                label = f"{name}:enter[{seg.lo},{seg.hi})"
                visible_after.update(segment_rows(seg, label, name))
            for seg in removed:
                label = f"{name}:exit[{seg.lo},{seg.hi})"
                visible_before.update(segment_rows(seg, label, name))

            changes = []
            for pk in sorted(set(visible_before) | set(visible_after)):
                b = visible_before.get(pk)
                a = visible_after.get(pk)
                if b is None:
                    changes.append(Change(ENTER, pk, a["route"], a["payload"]))
                elif a is None:
                    changes.append(Change(EXIT, pk, b["route"], b["payload"]))
                elif b["route"] != a["route"] or b["payload"] != a["payload"]:
                    changes.append(Change(UPDATE, pk, a["route"], a["payload"]))
                # else: visible both, delivered row identical -> no change.

            if changes:
                changes_by_sub[name] = changes
            elif name in move_map and new_iv != old_iv:
                # A move is an event in the stream even when it exposes
                # nothing new: the client still needs the new boundary.
                changes_by_sub[name] = []
        return changes_by_sub

    def _store_commit(self, conn, changes_by_sub, meta, move_map, subs, audit,
                      extra_audit=None):
        """Allocate the sequence number and persist the groups with it.

        The read audit is stored alongside the commit so the candidate-read
        count can be checked after the fact, not just from the return value.
        ``extra_audit`` (used by batch withdrawals) is merged into the stored
        audit so the batch ids touched by a correction stay reviewable.
        """
        seq = self._head(conn) + 1
        audit_dict = audit.to_dict()
        if extra_audit:
            audit_dict.update(extra_audit)
        conn.execute(
            "INSERT INTO commits (seq, created_at, meta, audit) VALUES (?, ?, ?, ?)",
            (seq, time.time(), canonical_payload(meta or {}),
             canonical_payload(audit_dict)),
        )
        conn.execute("UPDATE feed_meta SET value = ? WHERE key = 'next_seq'", (str(seq),))

        for name, changes in changes_by_sub.items():
            new_iv = move_map.get(name, Interval(subs[name]["lo"], subs[name]["hi"]))
            conn.execute(
                "INSERT INTO subscription_commits "
                "(subscription, seq, range_lo, range_hi, change_count) "
                "VALUES (?, ?, ?, ?, ?)",
                (name, seq, new_iv.lo, new_iv.hi, len(changes)),
            )
            conn.executemany(
                "INSERT INTO commit_changes "
                "(subscription, seq, idx, kind, pk, route, payload) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (name, seq, i, c.kind, c.pk, c.route, c.payload)
                    for i, c in enumerate(changes)
                ],
            )

        return seq

    # -- delivery ----------------------------------------------------------
    def _batch(self, conn, subscription: str, row) -> Batch:
        changes = conn.execute(
            "SELECT kind, pk, route, payload FROM commit_changes "
            "WHERE subscription = ? AND seq = ? ORDER BY idx",
            (subscription, row["seq"]),
        ).fetchall()
        return Batch(
            subscription, row["seq"], row["range_lo"], row["range_hi"],
            [Change.from_row(r) for r in changes],
        )

    def read_group(self, subscription: str, seq: int) -> Optional[Batch]:
        """Read one stored group verbatim; ``None`` if none exists for it."""
        with read_transaction(self.conn) as conn:
            self._sub_row(conn, subscription)
            row = conn.execute(
                "SELECT seq, range_lo, range_hi FROM subscription_commits "
                "WHERE subscription = ? AND seq = ?",
                (subscription, seq),
            ).fetchone()
            if row is None:
                return None
            return self._batch(conn, subscription, row)

    def commit_info(self, seq: int) -> Optional[dict]:
        """Metadata and stored read audit for a commit."""
        row = self.conn.execute(
            "SELECT seq, created_at, meta, audit FROM commits WHERE seq = ?", (seq,)
        ).fetchone()
        if row is None:
            return None
        return {
            "seq": row["seq"],
            "created_at": row["created_at"],
            "meta": json.loads(row["meta"]),
            "audit": json.loads(row["audit"]),
        }

    def pending(self, subscription: str, *, limit: Optional[int] = None) -> list:
        """Unacknowledged messages in original order.

        The first message is the start view while it is unacknowledged.  Every
        subsequent message is a stored commit group; nothing is recomputed
        from the subscription's current range.
        """
        out = []
        with read_transaction(self.conn) as conn:
            sub = self._sub_row(conn, subscription)
            if sub["acked_seq"] is None or sub["acked_seq"] < sub["start_seq"]:
                rows = conn.execute(
                    "SELECT pk, route, payload FROM subscription_start_view "
                    "WHERE subscription = ? ORDER BY idx",
                    (subscription,),
                ).fetchall()
                out.append(
                    StartView(
                        subscription, sub["start_lo"], sub["start_hi"], sub["start_seq"],
                        [self._row_dict(r) for r in rows],
                    ).to_dict()
                )
                after = sub["start_seq"]
            else:
                after = sub["acked_seq"]

            remaining = None if limit is None else max(0, limit - len(out))
            if remaining == 0:
                return out

            q = (
                "SELECT seq, range_lo, range_hi FROM subscription_commits "
                "WHERE subscription = ? AND seq > ? ORDER BY seq"
            )
            args = [subscription, after]
            if remaining is not None:
                q += " LIMIT ?"
                args.append(remaining)
            for row in conn.execute(q, args).fetchall():
                out.append(self._batch(conn, subscription, row).to_dict())
        return out

    def ack(self, subscription: str, seq: int) -> None:
        """Advance the durable cursor through ``seq`` (inclusive).

        Acknowledging a commit group implicitly acknowledges every earlier
        group and the start view.  Moves are rejected so a crash-replayed
        batch can always be re-read.
        """
        with transaction(self.conn) as conn:
            sub = self._sub_row(conn, subscription)
            head = self._head(conn)
            if isinstance(seq, bool) or not isinstance(seq, int):
                raise TypeError("seq must be an integer")
            if seq > head:
                raise ValueError(f"cannot acknowledge seq {seq} beyond head {head}")
            if seq < sub["start_seq"]:
                raise ValueError(
                    f"cannot acknowledge seq {seq} before the start boundary "
                    f"{sub['start_seq']}"
                )
            if sub["acked_seq"] is not None and seq < sub["acked_seq"]:
                raise ValueError(
                    f"cannot move cursor backwards from {sub['acked_seq']} to {seq}"
                )
            conn.execute(
                "UPDATE subscriptions SET acked_seq = ? WHERE name = ?", (seq, subscription)
            )

    # ======================================================================
    # Batch journal
    # ======================================================================
    def enable_batches(self) -> dict:
        """Turn on batch journaling.

        The record table as it stands right now is snapshotted into
        ``baseline_records`` and becomes the non-revocable starting point:
        earlier writes are never re-interpreted or withdrawn.  After this
        returns, every record insert/update/delete must go through
        :meth:`submit_batch`; a direct :meth:`apply` carrying writes is
        rejected.  Enabling twice is an idempotent no-op.
        """
        from .batches import REGISTERED  # local import: avoid an import cycle

        with transaction(self.conn) as conn:
            if self._meta_value(conn, "batch_mode") == "on":
                return {"enabled": False, "baseline_rows":
                        conn.execute("SELECT count(*) FROM baseline_records").fetchone()[0]}
            conn.execute(
                "INSERT INTO baseline_records (pk, route, payload) "
                "SELECT pk, route, payload FROM records"
            )
            conn.execute(
                "INSERT OR REPLACE INTO feed_meta (key, value) "
                "VALUES ('batch_mode', 'on')"
            )
            conn.execute(
                "INSERT OR IGNORE INTO feed_meta (key, value) "
                "VALUES ('next_batch_seq', '0')"
            )
            baseline_rows = conn.execute(
                "SELECT count(*) FROM baseline_records"
            ).fetchone()[0]
        return {"enabled": True, "baseline_rows": baseline_rows}

    # -- condition evaluation ---------------------------------------------
    @staticmethod
    def _condition_holds(conn, cond, audit):
        """Evaluate one precondition against the live table.

        ``absent`` holds only when no row exists at all (a NULL-payload row is
        still a row).  ``matches`` holds only for an existing row whose route
        and payload are byte-for-byte the expected ones.
        """
        row = conn.execute(_RECORD_POINT_SQL, (cond.pk,)).fetchone()
        audit.point_lookups += 1
        if cond.kind == "absent":
            return row is None
        if row is None:
            return False
        return row["route"] == cond.route and row["payload"] == cond.payload_value

    def _register_batch_rows(self, conn, request, fingerprint, status, notify_seq):
        """Insert the journal rows for a newly registered batch."""
        now = time.time()
        bseq = int(self._meta_value(conn, "next_batch_seq", "0")) + 1
        conn.execute(
            "UPDATE feed_meta SET value = ? WHERE key = 'next_batch_seq'", (str(bseq),)
        )
        conn.execute(
            "INSERT INTO batches (batch_id, seq, status, fingerprint, notify_seq, "
            "created_at, last_change_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (request.batch_id, bseq, status, fingerprint, notify_seq, now, now),
        )
        conn.executemany(
            "INSERT INTO batch_conditions (batch_id, idx, pk, kind, route, payload) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [(request.batch_id, i, c.pk, c.kind, c.route, c.payload_value)
             for i, c in enumerate(request.conditions)],
        )
        conn.executemany(
            "INSERT INTO batch_writes (batch_id, idx, pk, is_delete, route, payload) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [(request.batch_id, i, w.pk, 1 if w.delete else 0,
              None if w.delete else w.route, None if w.delete else w.payload_value)
             for i, w in enumerate(request.writes)],
        )
        return bseq

    # -- batch submission ---------------------------------------------------
    def submit_batch(self, request) -> "BatchSubmitResult":
        """Register and evaluate one batch atomically.

        All preconditions see the table immediately before this batch.  If they
        all hold, the writes take effect in one transaction with their
        subscription notifications; otherwise no row changes, but the batch is
        still registered as ``skipped``.  Retrying the same id with identical
        content returns the stored outcome without re-registering; different
        content under the same id is rejected and changes nothing.
        """
        from .batches import (
            BatchRequest, BatchSubmitResult, REGISTERED, SKIPPED,
            batch_fingerprint,
        )

        if not isinstance(request, BatchRequest):
            raise TypeError("submit_batch expects a BatchRequest")
        fingerprint = batch_fingerprint(request)

        with transaction(self.conn) as conn:
            if self._meta_value(conn, "batch_mode") != "on":
                raise RuntimeError(
                    "batch journaling is not enabled; call Feed.enable_batches() first"
                )
            existing = conn.execute(_BATCH_BY_ID_SQL, (request.batch_id,)).fetchone()
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    raise ValueError(
                        f"batch {request.batch_id!r} already registered with "
                        "different content"
                    )
                # Idempotent retry: nothing is re-registered or rewritten.
                return BatchSubmitResult(
                    request.batch_id, existing["seq"], registered=False,
                    applied=existing["status"] == REGISTERED,
                    notify_seq=existing["notify_seq"], fingerprint=fingerprint,
                )

            audit = ReadAudit()
            holds = [self._condition_holds(conn, c, audit) for c in request.conditions]
            if not all(holds):
                bseq = self._register_batch_rows(
                    conn, request, fingerprint, SKIPPED, None
                )
                return BatchSubmitResult(
                    request.batch_id, bseq, registered=True, applied=False,
                    notify_seq=None, fingerprint=fingerprint,
                )

            # Preconditions hold: take before images, apply the writes, then
            # derive subscription net changes exactly like apply() does.
            before, after = {}, {}
            for w in request.writes:
                row = conn.execute(_RECORD_POINT_SQL, (w.pk,)).fetchone()
                audit.point_lookups += 1
                if row is not None:
                    before[w.pk] = row
                if w.delete:
                    conn.execute("DELETE FROM records WHERE pk = ?", (w.pk,))
                else:
                    conn.execute(
                        "INSERT INTO records (pk, route, payload) VALUES (?, ?, ?) "
                        "ON CONFLICT(pk) DO UPDATE SET route = excluded.route, "
                        "payload = excluded.payload",
                        (w.pk, w.route, w.payload_value),
                    )
                    after[w.pk] = {
                        "pk": w.pk, "route": w.route, "payload": w.payload_value,
                    }

            subs = {r["name"]: r for r in self._all_subs(conn)}
            changes_by_sub = self._net_changes_for_subs(
                conn, subs, {}, before, after, {w.pk for w in request.writes}, audit
            )
            notify_seq = None
            if changes_by_sub:
                notify_seq = self._store_commit(
                    conn, changes_by_sub,
                    {"batch_id": request.batch_id, "kind": "batch"},
                    {}, subs, audit,
                )
            bseq = self._register_batch_rows(
                conn, request, fingerprint, REGISTERED, notify_seq
            )
            return BatchSubmitResult(
                request.batch_id, bseq, registered=True, applied=True,
                notify_seq=notify_seq, changes=changes_by_sub,
                fingerprint=fingerprint,
            )

    # -- withdrawal ---------------------------------------------------------
    def withdraw_batch(self, batch_id: str):
        """Revoke one registered batch and settle everything in one transaction.

        The table is re-understood from the enable-time baseline: the withdrawn
        batch is skipped, and every still-active batch it could have influenced
        is re-evaluated in original registration order.  Batches unrelated by
        primary key are never read.  Subscribers receive one net correction
        group built from the real before/after row difference; repeat
        withdrawal of the same id changes nothing, an unknown id is an error.
        """
        from .batches import (
            REGISTERED, SKIPPED, WITHDRAWN, WithdrawAudit, WithdrawResult,
        )

        if not isinstance(batch_id, str) or not batch_id:
            raise TypeError("batch_id must be a non-empty string")

        with transaction(self.conn) as conn:
            if self._meta_value(conn, "batch_mode") != "on":
                raise RuntimeError("batch journaling is not enabled")
            target = conn.execute(_BATCH_BY_ID_SQL, (batch_id,)).fetchone()
            if target is None:
                raise KeyError(f"unknown batch {batch_id!r}")
            if target["status"] == WITHDRAWN:
                # Repeated withdrawal is a stable no-op.
                return WithdrawResult(
                    batch_id, changed=False, notify_seq=None,
                    statuses=self._batch_statuses(conn, (batch_id,)),
                )

            audit = WithdrawAudit(withdrawn_batch=batch_id)
            read_ids = {batch_id}

            # A batch that never took effect applied no rows: its withdrawal
            # cannot change the table or any later evaluation, so nothing is
            # replayed.  Its own details are the only journal data consulted.
            if target["status"] == SKIPPED:
                conn.execute(
                    "UPDATE batches SET status = ?, last_change_at = ? "
                    "WHERE batch_id = ?",
                    (WITHDRAWN, time.time(), batch_id),
                )
                # Touch the stored details so the read report is truthful.
                self._batch_keys(conn, batch_id, audit)
                audit.read_batches = sorted(read_ids)
                return WithdrawResult(
                    batch_id, changed=False, notify_seq=None,
                    statuses=self._batch_statuses(conn, (batch_id,)),
                    audit=audit,
                )

            # (1) Closure of batches this withdrawal can influence, built by a
            # deterministic fixpoint over *affected primary keys*, processed in
            # registration order.
            #
            # ``affected_keys`` starts with the effective target's write keys
            # (a skipped target returned above).  A later, not-withdrawn batch
            # is admitted when it either writes an affected key or reads one as
            # a precondition - exactly the spec's "reads that key as a
            # condition or writes that key" rule.  A reached batch's status can
            # change either way (an effective batch may flip to skipped and
            # stop writing; a skipped one may flip to effective and start), so
            # every reached batch contributes the keys it writes, and a reached
            # *skipped* batch also contributes its gate keys (whether its gate
            # now holds decides everything downstream).  Batches are processed
            # in seq order; the indexed pk lookup is repeated only while new
            # batches join, so unrelated batches are never opened and neither
            # the journal nor the record table is scanned.
            affected: dict[int, str] = {target["seq"]: batch_id}
            touched = set(self._write_keys(conn, batch_id, audit))
            affected_keys = set(self._write_keys(conn, batch_id, audit))
            while True:
                candidates = self._referencing_write_batches(
                    conn, affected_keys, target["seq"], audit
                ) + self._referencing_condition_batches(
                    conn, affected_keys, target["seq"], audit
                )
                pending = sorted(
                    (c for c in candidates
                     if c[0] not in affected and c[2] != WITHDRAWN),
                    key=lambda c: c[0],
                )
                added_keys = set()
                admitted = 0
                for bseq, bid, status in pending:
                    cond = set(self._condition_keys(conn, bid, audit))
                    write = set(self._write_keys(conn, bid, audit))
                    via_write = bool(write & affected_keys)
                    via_cond = bool(cond & affected_keys)
                    if not (via_write or via_cond):
                        continue
                    affected[bseq] = bid
                    admitted += 1
                    read_ids.add(bid)
                    touched.update(write | cond)
                    # Its writes may appear or disappear in the new settlement.
                    added_keys.update(write)
                    if status == SKIPPED:
                        # Its gate may newly hold; those keys drive later gates.
                        added_keys.update(cond)
                # Settle only when no new batch joined; new keys drive the next
                # round's candidate lookup.
                if admitted == 0:
                    break
                affected_keys |= added_keys
            audit.affected_batches = [bid for _, bid in sorted(affected.items())]

            # (2) Every key the re-evaluation can observe.  ``touched`` already
            # holds the target's keys plus every key added while expanding the
            # closure, including keys only read as preconditions, so outside
            # writers of such a key are replayed as fixed events rather than
            # silently assumed constant.
            boundary, events = self._key_timelines(conn, touched, affected, audit)
            read_ids.update(events["read_batches"])

            # (3) Walk the affected batches in registration order, maintaining
            # the current value of every touched key.  Events from unaffected,
            # still-effective batches are applied unconditionally; affected
            # batches are re-evaluated against this reconstructed state.
            values = dict(boundary)
            new_status: dict[str, str] = {}
            affected_order = sorted(affected.items())   # (seq, batch_id)
            outside = events["events"]           # (bseq, bid, write-row, pk)
            oi = 0
            for bseq, bid in affected_order:
                while oi < len(outside) and outside[oi][0] < bseq:
                    _, _, wrow, pk = outside[oi]
                    values[pk] = self._event_value(wrow)
                    oi += 1

                if bid == batch_id:
                    new_status[bid] = WITHDRAWN
                    continue
                conds = conn.execute(_CONDITIONS_OF_SQL, (bid,)).fetchall()
                audit.journal_rows_read += len(conds)
                holds = True
                for c in conds:
                    cur = values.get(c["pk"], _ABSENT)
                    if c["kind"] == "absent":
                        if cur is not _ABSENT:
                            holds = False
                            break
                    else:
                        if cur is _ABSENT or cur[0] != c["route"] or cur[1] != c["payload"]:
                            holds = False
                            break
                if not holds:
                    new_status[bid] = SKIPPED
                    continue
                new_status[bid] = REGISTERED
                for wrow in conn.execute(_WRITES_OF_SQL, (bid,)).fetchall():
                    audit.journal_rows_read += 1
                    values[wrow["pk"]] = self._event_value(wrow)
            while oi < len(outside):
                _, _, wrow, pk = outside[oi]
                values[pk] = self._event_value(wrow)
                oi += 1

            # (4) Real row difference vs the live table, restricted to keys
            # whose final value can have changed.  Each is one point lookup.
            before, after, written_pks = {}, {}, set()
            for pk in sorted(touched):
                live = conn.execute(_RECORD_POINT_SQL, (pk,)).fetchone()
                audit.point_lookups += 1
                final_value = values.get(pk, _ABSENT)
                if final_value is _ABSENT:
                    final_row = None
                else:
                    final_row = {"pk": pk, "route": final_value[0],
                                 "payload": final_value[1]}
                if self._same_row(live, final_row):
                    continue
                written_pks.add(pk)
                if live is not None:
                    before[pk] = live
                if final_row is not None:
                    after[pk] = final_row
                if final_row is None:
                    conn.execute("DELETE FROM records WHERE pk = ?", (pk,))
                else:
                    conn.execute(
                        "INSERT INTO records (pk, route, payload) VALUES (?, ?, ?) "
                        "ON CONFLICT(pk) DO UPDATE SET route = excluded.route, "
                        "payload = excluded.payload",
                        (pk, final_row["route"], final_row["payload"]),
                    )

            # (5) Persist the new statuses and publish one net correction.
            now = time.time()
            for bid, status in new_status.items():
                notify = None
                if status == REGISTERED:
                    notify = conn.execute(
                        "SELECT notify_seq FROM batches WHERE batch_id = ?", (bid,)
                    ).fetchone()["notify_seq"]
                conn.execute(
                    "UPDATE batches SET status = ?, notify_seq = ?, last_change_at = ? "
                    "WHERE batch_id = ?",
                    (status, notify, now, bid),
                )

            changes_by_sub = {}
            notify_seq = None
            if written_pks:
                change_audit = ReadAudit()
                change_audit.point_lookups = audit.point_lookups
                subs = {r["name"]: r for r in self._all_subs(conn)}
                changes_by_sub = self._net_changes_for_subs(
                    conn, subs, {}, before, after, written_pks, change_audit
                )
                if changes_by_sub:
                    notify_seq = self._store_commit(
                        conn, changes_by_sub,
                        {"kind": "withdrawal", "batch_id": batch_id},
                        {}, subs, change_audit,
                        extra_audit={"withdrawal": audit.to_dict()},
                    )

            audit.read_batches = sorted(read_ids)
            return WithdrawResult(
                batch_id, changed=True, notify_seq=notify_seq,
                changes=changes_by_sub,
                statuses=self._batch_statuses(conn, set(affected.values())),
                audit=audit,
            )

    # -- batch journal helpers ---------------------------------------------
    def _batch_keys(self, conn, batch_id, audit):
        """Distinct primary keys a batch references in conditions or writes."""
        return set(self._condition_keys(conn, batch_id, audit)) | set(
            self._write_keys(conn, batch_id, audit)
        )

    def _condition_keys(self, conn, batch_id, audit):
        keys = []
        for c in conn.execute(_CONDITIONS_OF_SQL, (batch_id,)).fetchall():
            audit.journal_rows_read += 1
            keys.append(c["pk"])
        return keys

    def _write_keys(self, conn, batch_id, audit):
        keys = []
        for w in conn.execute(_WRITES_OF_SQL, (batch_id,)).fetchall():
            audit.journal_rows_read += 1
            keys.append(w["pk"])
        return keys

    def _referencing_write_batches(self, conn, keys, target_seq, audit):
        """Later batches that *write* any of ``keys`` (indexed pk lookups)."""
        return self._referencing_via(
            conn, keys, target_seq, audit, _WRITE_REFS_SQL
        )

    def _referencing_condition_batches(self, conn, keys, target_seq, audit):
        """Later batches that read any of ``keys`` as a precondition."""
        return self._referencing_via(
            conn, keys, target_seq, audit, _COND_REFS_SQL
        )

    def _referencing_via(self, conn, keys, target_seq, audit, sql_template):
        """Later batches referencing ``keys`` through one journal table.

        The backing index leads with ``pk``, so this stays an indexed lookup as
        the key set grows, rather than a journal scan.  Returns
        ``[(seq, batch_id, status), ...]`` for batches after the target.
        """
        if not keys:
            return []
        marks = ",".join("?" for _ in keys)
        ids = {
            row["batch_id"]
            for row in conn.execute(
                sql_template.format(marks=marks), list(keys)
            ).fetchall()
        }
        audit.journal_rows_read += len(ids)
        out = []
        for bid in ids:
            brow = conn.execute(_BATCH_BY_ID_SQL, (bid,)).fetchone()
            if brow["seq"] > target_seq:
                out.append((brow["seq"], bid, brow["status"]))
        return out

    def _key_timelines(self, conn, keys, affected, audit):
        """Reconstruct per-key history for the re-evaluation walk.

        ``boundary`` is each key's value at the enable-time baseline (absent
        keys are simply omitted).  ``events`` lists write events from batches
        outside the affected closure that are still effective, ordered by
        registration sequence; affected-batch writes are applied during the
        walk itself.
        """
        from .batches import REGISTERED

        boundary = {}
        for pk in keys:
            row = conn.execute(_BASELINE_POINT_SQL, (pk,)).fetchall()
            audit.baseline_lookups += 1
            if row:
                r = row[0]
                boundary[pk] = (r["route"], r["payload"])

        marks = ",".join("?" for _ in keys)
        events = []
        read_batches = set()
        affected_ids = set(affected.values())
        for row in conn.execute(
            _JOURNAL_EVENTS_SQL.format(marks=marks), list(keys)
        ).fetchall():
            audit.journal_rows_read += 1
            if row["status"] != REGISTERED:
                continue
            if row["batch_id"] in affected_ids:
                continue
            read_batches.add(row["batch_id"])
            events.append((row["bseq"], row["batch_id"], row, row["pk"]))
        events.sort(key=lambda e: (e[0], e[2]["idx"]))
        return boundary, {"events": events, "read_batches": read_batches}

    @staticmethod
    def _event_value(wrow):
        """The (route, payload) a write event leaves behind, or ``_ABSENT``."""
        if wrow["is_delete"]:
            return _ABSENT
        return (wrow["route"], wrow["payload"])

    @staticmethod
    def _same_row(live, final_row):
        if live is None and final_row is None:
            return True
        if live is None or final_row is None:
            return False
        return (live["route"] == final_row["route"]
                and live["payload"] == final_row["payload"])

    def _batch_statuses(self, conn, ids):
        from .batches import BatchStatus

        out = []
        if not ids:
            return out
        marks = ",".join("?" for _ in ids)
        for row in conn.execute(
            f"SELECT batch_id, seq, status, fingerprint, notify_seq, created_at, "
            f"last_change_at FROM batches WHERE batch_id IN ({marks}) ORDER BY seq",
            list(ids),
        ).fetchall():
            cc = conn.execute(
                "SELECT count(*) FROM batch_conditions WHERE batch_id = ?",
                (row["batch_id"],),
            ).fetchone()[0]
            wc = conn.execute(
                "SELECT count(*) FROM batch_writes WHERE batch_id = ?",
                (row["batch_id"],),
            ).fetchone()[0]
            out.append(BatchStatus(
                row["batch_id"], row["seq"], row["status"], row["fingerprint"],
                cc, wc, row["notify_seq"], row["created_at"], row["last_change_at"],
            ))
        return out

    def batch_status(self, batch_id: str):
        """Return one batch's journal status; ``KeyError`` if unknown."""
        with read_transaction(self.conn) as conn:
            if self._meta_value(conn, "batch_mode") != "on":
                raise RuntimeError("batch journaling is not enabled")
            statuses = self._batch_statuses(conn, (batch_id,))
        if not statuses:
            raise KeyError(f"unknown batch {batch_id!r}")
        return statuses[0]

    def batches(self) -> list:
        """All registered batches in first-registration order."""
        with read_transaction(self.conn) as conn:
            if self._meta_value(conn, "batch_mode") != "on":
                return []
            rows = conn.execute(
                "SELECT batch_id FROM batches ORDER BY seq"
            ).fetchall()
            return self._batch_statuses(conn, [r["batch_id"] for r in rows])

