"""Batch journal value objects, request validation and fingerprints.

A *batch* is the only way record writes may enter the table once batch
journaling is enabled (see :meth:`feedtable.Feed.enable_batches`).  It carries:

* a unique ``batch_id``;
* a possibly empty list of *preconditions* - each pins one primary key to
  "must not exist" or "must exist with exactly this route and payload";
* a non-empty list of record writes that reuse the existing
  :class:`~feedtable.core.RowSpec` upsert/delete semantics.

Preconditions are always evaluated against the table **as it stands before the
batch takes effect**.  They use the table's own row semantics: an existing row
whose payload is ``NULL`` is a real row, so it neither satisfies an
"absent" precondition nor compares equal to a non-null expected payload.

Every batch is *registered* once, in first-registration order, even when its
preconditions fail (such a batch changes no rows but keeps its place in the
journal).  Re-submitting the same ``batch_id`` with the identical content is a
harmless no-op; the same id with different content is rejected.  Content
identity comes from :func:`batch_fingerprint`, a canonical hash of the
conditions and writes, so operator-facing dicts and stored rows compare by
value, not by object identity.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Optional

from .core import RowSpec, canonical_payload

#: Batch lifecycle states persisted in ``batches.status``.
REGISTERED = "registered"   # took effect when first evaluated (and still does)
SKIPPED = "skipped"         # preconditions failed when evaluated; no rows of
                            # its own were ever applied
WITHDRAWN = "withdrawn"     # revoked by an operator; never takes effect


@dataclass(frozen=True)
class Condition:
    """One precondition on a single primary key.

    Two shapes, both compared against the live table's ``(route, payload)``
    row semantics:

    * ``Condition.absent(pk)`` - the key must not resolve to any row.  A row
      with a ``NULL`` payload is still a row, so it fails this condition.
    * ``Condition.matches(pk, route, payload)`` - a row must exist with
      exactly this route and payload (a ``None`` payload matches only an
      existing row whose payload is ``NULL``).
    """

    pk: str
    kind: str                      # "absent" | "matches"
    route: Optional[int] = None
    payload: object = None

    def __post_init__(self):
        if not isinstance(self.pk, str) or not self.pk:
            raise TypeError("condition pk must be a non-empty string")
        if self.kind not in ("absent", "matches"):
            raise ValueError(f"unknown condition kind {self.kind!r}")
        if self.kind == "matches":
            if isinstance(self.route, bool) or not isinstance(self.route, int):
                raise ValueError(f"matches condition for {self.pk!r} needs an integer route")
        elif self.route is not None:
            raise ValueError(f"absent condition for {self.pk!r} cannot carry a route")

    @classmethod
    def absent(cls, pk: str) -> "Condition":
        return cls(pk=pk, kind="absent")

    @classmethod
    def matches(cls, pk: str, route: int, payload=None) -> "Condition":
        return cls(pk=pk, kind="matches", route=route, payload=canonical_payload(payload))

    @property
    def payload_value(self):
        """Payload normalised exactly like a stored record payload."""
        return canonical_payload(self.payload)

    def to_dict(self) -> dict:
        out = {"pk": self.pk, "kind": self.kind}
        if self.kind == "matches":
            out["route"] = self.route
            out["payload"] = self.payload
        return out

    @classmethod
    def from_dict(cls, obj: dict) -> "Condition":
        kind = obj.get("kind", "matches" if "route" in obj else "absent")
        if kind == "absent":
            return cls.absent(obj["pk"])
        return cls.matches(obj["pk"], obj["route"], obj.get("payload"))

    def canonical(self) -> dict:
        """Stable, JSON-serialisable form used for fingerprints/storage."""
        if self.kind == "absent":
            return {"pk": self.pk, "kind": "absent"}
        return {"pk": self.pk, "kind": "matches",
                "route": self.route, "payload": self.payload_value}


@dataclass(frozen=True)
class BatchRequest:
    """A validated batch submission: id, preconditions and writes."""

    batch_id: str
    conditions: tuple = ()
    writes: tuple = ()

    def __post_init__(self):
        if not isinstance(self.batch_id, str) or not self.batch_id:
            raise TypeError("batch_id must be a non-empty string")
        conds = tuple(self.conditions)
        writes = tuple(self.writes)
        for c in conds:
            if not isinstance(c, Condition):
                raise TypeError("conditions must be Condition instances")
        if not writes:
            raise ValueError(f"batch {self.batch_id!r} needs at least one write")
        seen = set()
        for w in writes:
            if not isinstance(w, RowSpec):
                raise TypeError("writes must be RowSpec instances")
            if w.pk in seen:
                raise ValueError(
                    f"batch {self.batch_id!r} writes pk {w.pk!r} more than once"
                )
            seen.add(w.pk)
        # frozen dataclass: store the normalised tuples.
        object.__setattr__(self, "conditions", conds)
        object.__setattr__(self, "writes", writes)

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "conditions": [c.to_dict() for c in self.conditions],
            "writes": [_write_dict(w) for w in self.writes],
        }

    def canonical(self) -> dict:
        """Order-independent (conditions) / order-bearing (writes) form."""
        return {
            "batch_id": self.batch_id,
            "conditions": sorted(
                (c.canonical() for c in self.conditions),
                key=lambda d: json.dumps(d, sort_keys=True, ensure_ascii=False),
            ),
            "writes": [_write_canonical(w) for w in self.writes],
        }


def _write_dict(w: RowSpec) -> dict:
    if w.delete:
        return {"pk": w.pk, "delete": True}
    return {"pk": w.pk, "route": w.route, "payload": _json_or_text(w.payload_value)}


def _write_canonical(w: RowSpec) -> dict:
    if w.delete:
        return {"pk": w.pk, "delete": True, "route": None, "payload": None}
    return {"pk": w.pk, "delete": False, "route": w.route,
            "payload": _json_or_text(w.payload_value)}


def _json_or_text(value):
    """Stored payloads are canonical text; expose parsed JSON where possible."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


def batch_fingerprint(request: BatchRequest) -> str:
    """Stable content hash covering conditions and writes (not the id)."""
    blob = json.dumps(
        {"conditions": request.canonical()["conditions"],
         "writes": request.canonical()["writes"]},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass
class BatchStatus:
    """The journal state of one batch."""

    batch_id: str
    seq: int                       # first-registration order (1-based, dense)
    status: str                    # registered | skipped | withdrawn
    fingerprint: str
    condition_count: int
    write_count: int
    notify_seq: Optional[int] = None     # notification seq when it took effect
    created_at: float = 0.0
    last_change_at: float = 0.0

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "seq": self.seq,
            "status": self.status,
            "effective": self.status == REGISTERED,
            "fingerprint": self.fingerprint,
            "condition_count": self.condition_count,
            "write_count": self.write_count,
            "notify_seq": self.notify_seq,
            "created_at": self.created_at,
            "last_change_at": self.last_change_at,
        }


@dataclass
class BatchSubmitResult:
    """Result of :meth:`Feed.submit_batch`.

    ``registered`` is ``False`` for an idempotent retry of an identical,
    already-registered batch.  ``applied`` says whether the preconditions held
    and the writes took effect on this evaluation.  A failed batch is still
    registered (``registered=True``, ``applied=False``) and produces no
    notification.
    """

    batch_id: str
    seq: int
    registered: bool
    applied: bool
    notify_seq: Optional[int]
    changes: dict = field(default_factory=dict)
    fingerprint: str = ""

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "seq": self.seq,
            "registered": self.registered,
            "applied": self.applied,
            "notify_seq": self.notify_seq,
            "fingerprint": self.fingerprint,
            "changes": {
                s: [c.to_dict() for c in cs] for s, cs in self.changes.items()
            },
        }


@dataclass
class WithdrawAudit:
    """Checkable read-cost account for one withdrawal.

    Only the withdrawn batch and the batches it *indirectly affects* along
    registration order are ever read.  A later batch is affected only if it
    reads, as a precondition, or writes a primary key that an affected batch
    touched; the closure is computed with indexed primary-key lookups and never
    scans the record table or the whole journal.
    """

    withdrawn_batch: str
    read_batches: list = field(default_factory=list)        # distinct batch ids
    affected_batches: list = field(default_factory=list)    # re-evaluated ids
    point_lookups: int = 0
    baseline_lookups: int = 0
    journal_rows_read: int = 0

    def to_dict(self) -> dict:
        return {
            "withdrawn_batch": self.withdrawn_batch,
            "read_batches": list(self.read_batches),
            "affected_batches": list(self.affected_batches),
            "point_lookups": self.point_lookups,
            "baseline_lookups": self.baseline_lookups,
            "journal_rows_read": self.journal_rows_read,
        }


@dataclass
class WithdrawResult:
    """Result of :meth:`Feed.withdraw_batch`."""

    batch_id: str
    changed: bool                  # False for an already-withdrawn repeat
    notify_seq: Optional[int]      # new correction group, when rows moved
    changes: dict = field(default_factory=dict)
    statuses: list = field(default_factory=list)   # BatchStatus after the op
    audit: Optional[WithdrawAudit] = None

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "changed": self.changed,
            "notify_seq": self.notify_seq,
            "changes": {
                s: [c.to_dict() for c in cs] for s, cs in self.changes.items()
            },
            "statuses": [s.to_dict() for s in self.statuses],
            "audit": self.audit.to_dict() if self.audit else None,
        }
