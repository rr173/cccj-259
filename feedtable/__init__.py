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
from .compaction import (
    CompactReport,
    CompactResult,
    Compaction,
    CompactionReport,
    CompactionResult,
    RetentionReport,
    SubscriptionCompaction,
    SubscriptionRetention,
)
from .intervals import Interval, intersect, subtract


def compact_database(path, **kwargs) -> CompactionResult:
    """Open ``path``, run :meth:`Feed.compact`, and close the database."""
    feed = Feed(path)
    try:
        return feed.compact(**kwargs)
    finally:
        feed.close()


compact_feed = compact_database
compact = compact_database
compact_history = compact_database
compress = compact_database
compress_database = compact_database
prune_history = compact_database
run_compaction = compact_database

__all__ = [
    "Batch",
    "BatchRequest",
    "BatchStatus",
    "BatchSubmitResult",
    "Change",
    "CommitResult",
    "CompactReport",
    "CompactResult",
    "Compaction",
    "CompactionReport",
    "CompactionResult",
    "Condition",
    "ENTER",
    "EXIT",
    "Feed",
    "Interval",
    "MoveSpec",
    "ReadAudit",
    "REGISTERED",
    "RetentionReport",
    "RowSpec",
    "SKIPPED",
    "StartView",
    "SubscriptionCompaction",
    "SubscriptionRetention",
    "UPDATE",
    "WITHDRAWN",
    "WithdrawAudit",
    "WithdrawResult",
    "batch_fingerprint",
    "canonical_payload",
    "compact",
    "compact_database",
    "compact_feed",
    "compact_history",
    "compress",
    "compress_database",
    "intersect",
    "prune_history",
    "run_compaction",
    "subtract",
]
