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

#: The only range scan shape this module is allowed to run.  ``INDEXED BY``
#: pins the plan so a schema change cannot silently turn it into a full scan.
_SEGMENT_SQL = (
    "SELECT pk, route, payload FROM records INDEXED BY records_route_pk "
    "WHERE route >= ? AND route < ? ORDER BY route, pk"
)
_START_VIEW_SQL = _SEGMENT_SQL


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
            changes_by_sub: dict[str, list[Change]] = {}
            segment_cache: dict[tuple, dict] = {}
            planned = self._read_audit_plan(conn)
            written_pks = set(final)

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

            if not changes_by_sub:
                # Nothing observable happened.  Record writes still commit,
                # but no sequence number and no group are allocated: a client
                # has nothing to acknowledge.
                return CommitResult(self._head(conn), {}, audit, committed=False)

            seq = self._store_commit(conn, changes_by_sub, meta, move_map, subs, audit)
            return CommitResult(seq, changes_by_sub, audit, committed=True)

    def _store_commit(self, conn, changes_by_sub, meta, move_map, subs, audit):
        """Allocate the sequence number and persist the groups with it.

        The read audit is stored alongside the commit so the candidate-read
        count can be checked after the fact, not just from the return value.
        """
        seq = self._head(conn) + 1
        conn.execute(
            "INSERT INTO commits (seq, created_at, meta, audit) VALUES (?, ?, ?, ?)",
            (seq, time.time(), canonical_payload(meta or {}),
             canonical_payload(audit.to_dict())),
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
