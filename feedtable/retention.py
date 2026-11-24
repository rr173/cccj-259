"""Compatibility alias for callers using the term "retention frontier"."""

from .compact import compact, compact_database
from .compaction import (
    CompactionResult as RetentionReport,
    SubscriptionCompaction as SubscriptionRetention,
)
from .core import Feed

__all__ = [
    "Feed",
    "RetentionReport",
    "SubscriptionRetention",
    "compact",
    "compact_database",
]
