"""Local durable roster-notification feed.

Quick start::

    from feedtable import Feed, RowSpec, MoveSpec

    feed = Feed("roster.db")
    view = feed.create_subscription("qc-a", 0, 100)
    view.seq, view.rows          # start view + boundary

    result = feed.apply(
        [RowSpec("sku-1", 42, {"status": "new"})],
        [MoveSpec("qc-a", 50, 150)],          # widen the range to the right
    )
    result.changes["qc-a"]       # one ordered group of enter/update/exit

    feed.ack("qc-a", result.seq)
    feed.pending("qc-a")         # [] - nothing left

See ``README.md`` for the guarantees and ``tests/`` for executable checks.
"""

from .core import (
    Batch,
    Change,
    CommitResult,
    ENTER,
    EXIT,
    Feed,
    MoveSpec,
    ReadAudit,
    RowSpec,
    StartView,
    UPDATE,
    canonical_payload,
)
from .batches import (
    REGISTERED,
    SKIPPED,
    WITHDRAWN,
    BatchRequest,
    BatchStatus,
    BatchSubmitResult,
    Condition,
    WithdrawAudit,
    WithdrawResult,
    batch_fingerprint,
)
from .intervals import Interval, intersect, subtract

__all__ = [
    "Batch",
    "BatchRequest",
    "BatchStatus",
    "BatchSubmitResult",
    "Change",
    "CommitResult",
    "Condition",
    "ENTER",
    "EXIT",
    "Feed",
    "Interval",
    "MoveSpec",
    "ReadAudit",
    "REGISTERED",
    "RowSpec",
    "SKIPPED",
    "StartView",
    "UPDATE",
    "WITHDRAWN",
    "WithdrawAudit",
    "WithdrawResult",
    "batch_fingerprint",
    "canonical_payload",
    "intersect",
    "subtract",
]
