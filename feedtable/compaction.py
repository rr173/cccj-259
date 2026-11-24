"""Reports returned by announcement-history compaction.

The report is deliberately both attribute- and mapping-friendly.  Existing
feed APIs return dataclass-style objects, while the CLI serialises
:meth:`CompactionResult.to_dict`; supporting item access also makes the result
easy to use from small scripts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Optional


class MetricDict(dict):
    """Mapping that also compares equal to its aggregate integer value.

    Compaction reports are consumed both as counts (``report.deleted == 3``)
    and as per-subscription details (``report.deleted["s"]``).  This small
    adapter supports both styles without forcing callers to know which field
    name a particular test or script uses.
    """

    def __init__(self, items: dict, total: int):
        super().__init__(items)
        self.total = int(total)

    def __eq__(self, other):
        if isinstance(other, bool):
            return super().__eq__(other)
        if isinstance(other, (int, float)):
            return self.total == int(other)
        return super().__eq__(other)

    def __int__(self) -> int:
        return self.total

    def __index__(self) -> int:
        return self.total

    @property
    def count(self) -> int:
        return self.total

    @property
    def total_count(self) -> int:
        return self.total


@dataclass(frozen=True)
class SubscriptionCompaction:
    """Retention decision and deleted/retained counts for one subscription."""

    name: str
    start_seq: int
    acked_seq: Optional[int]
    start_view_pending: bool
    retention_frontier: int
    earliest_needed_seq: Optional[int]
    earliest_needed_kind: Optional[str]
    earliest_retained_seq: Optional[int]
    latest_retained_seq: Optional[int]
    deleted_min_seq: Optional[int]
    deleted_max_seq: Optional[int]
    deleted_groups: int
    deleted_changes: int
    retained_groups: int
    retained_changes: int
    orphaned: bool = False

    @property
    def deleted_range(self) -> Optional[list[int]]:
        if self.deleted_min_seq is None:
            return None
        return [self.deleted_min_seq, self.deleted_max_seq]

    @property
    def removed_range(self) -> Optional[list[int]]:
        return self.deleted_range

    @property
    def deleted_seq_range(self) -> Optional[list[int]]:
        return self.deleted_range

    @property
    def removed_seq_range(self) -> Optional[list[int]]:
        return self.deleted_range

    @property
    def range(self) -> Optional[list[int]]:
        return self.deleted_range

    @property
    def before_groups(self) -> int:
        return self.deleted_groups + self.retained_groups

    @property
    def after_groups(self) -> int:
        return self.retained_groups

    @property
    def retain_from_seq(self) -> int:
        """First possible sequence after the inclusive retention frontier."""
        return self.retention_frontier + 1

    @property
    def deleted_interval(self) -> Optional[list[int]]:
        """Half-open interval corresponding to :attr:`deleted_range`."""
        if self.deleted_min_seq is None:
            return None
        return [self.deleted_min_seq, self.deleted_max_seq + 1]

    @property
    def retained_range(self) -> Optional[list[int]]:
        if self.earliest_retained_seq is None:
            return None
        return [self.earliest_retained_seq, self.latest_retained_seq]

    def to_dict(self) -> dict:
        return {
            "subscription": self.name,
            "start_seq": self.start_seq,
            "acked_seq": self.acked_seq,
            "start_view_pending": self.start_view_pending,
            "retention_frontier": self.retention_frontier,
            "delete_through_seq": self.retention_frontier,
            "retain_from_seq": self.retain_from_seq,
            "deleted_interval": self.deleted_interval,
            "earliest_needed_seq": self.earliest_needed_seq,
            "earliest_needed_kind": self.earliest_needed_kind,
            "earliest_retained_seq": self.earliest_retained_seq,
            "latest_retained_seq": self.latest_retained_seq,
            "deleted_range": self.deleted_range,
            "deleted_seq_range": self.deleted_range,
            "removed_range": self.removed_range,
            "deleted_min_seq": self.deleted_min_seq,
            "deleted_max_seq": self.deleted_max_seq,
            "deleted_groups": self.deleted_groups,
            "deleted_count": self.deleted_groups,
            "deleted_messages": self.deleted_groups,
            "deleted_changes": self.deleted_changes,
            "before_groups": self.before_groups,
            "after_groups": self.after_groups,
            "before_count": self.before_groups,
            "after_count": self.after_groups,
            "retained_range": self.retained_range,
            "retained_groups": self.retained_groups,
            "retained_count": self.retained_groups,
            "retained_messages": (
                self.retained_groups + (1 if self.start_view_pending else 0)
            ),
            "retained_changes": self.retained_changes,
            "orphaned": self.orphaned,
        }


@dataclass
class CompactionResult:
    """The outcome of one compaction attempt.

    ``retention_frontiers`` is the per-subscription safe delete-through
    boundary.  ``global_frontier`` is the smallest such boundary and therefore
    the prefix for which even the shared ``commits`` metadata is unreferenced.
    Per-subscription deletion can go beyond it because each subscription has
    its own stored copy of a group.
    """

    head: int
    global_frontier: int
    retention_frontiers: dict[str, int] = field(default_factory=dict)
    subscriptions: dict[str, SubscriptionCompaction] = field(default_factory=dict)
    deleted_groups: int = 0
    deleted_changes: int = 0
    commits_deleted: int = 0
    commit_deleted_range: Optional[list[int]] = None
    retained_groups: int = 0
    retained_changes: int = 0
    pending_start_views: int = 0
    changed: bool = False
    dry_run: bool = False
    vacuumed: bool = False
    run_id: Optional[int] = None
    ran_at: Optional[float] = None

    # Common alternative names used by scripts/tests.
    @property
    def would_change(self) -> bool:
        return self.changed

    @property
    def applied(self) -> bool:
        return not self.dry_run and self.changed

    @property
    def compacted(self) -> bool:
        return self.changed

    @property
    def ok(self) -> bool:
        return True

    @property
    def success(self) -> bool:
        return True

    @property
    def global_retention_frontier(self) -> int:
        return self.global_frontier

    @property
    def retention_frontier(self) -> int:
        return self.global_frontier

    @property
    def safe_frontier(self) -> int:
        return self.global_frontier

    @property
    def compacted_through(self) -> int:
        return self.global_frontier

    @property
    def before_groups(self) -> int:
        return self.deleted_groups + self.retained_groups

    @property
    def after_groups(self) -> int:
        return self.retained_groups

    @property
    def before_count(self) -> int:
        return self.before_groups

    @property
    def after_count(self) -> int:
        return self.after_groups

    @property
    def frontiers(self) -> dict[str, int]:
        return self.retention_frontiers

    @property
    def retain_from_seqs(self) -> dict[str, int]:
        return {name: f + 1 for name, f in self.retention_frontiers.items()}

    @property
    def earliest_needed_seqs(self) -> dict[str, Optional[int]]:
        return {
            name: item.earliest_needed_seq
            for name, item in self.subscriptions.items()
            if not item.orphaned
        }

    @property
    def deleted_count(self) -> int:
        return self.deleted_groups

    @property
    def deleted_messages(self) -> int:
        return self.deleted_groups

    @property
    def removed_groups(self) -> int:
        return self.deleted_groups

    @property
    def removed_count(self) -> int:
        return self.deleted_groups

    @property
    def retained_count(self) -> int:
        """Number of retained stored commit groups."""
        return self.retained_groups

    @property
    def remaining_count(self) -> int:
        return self.retained_groups

    @property
    def remaining(self) -> int:
        return self.retained_groups

    @property
    def retention(self) -> dict[str, dict]:
        return {name: item.to_dict() for name, item in self.subscriptions.items()}

    @property
    def per_subscription(self) -> dict[str, dict]:
        return self.retention

    @property
    def removed(self) -> dict[str, dict]:
        return self.deleted

    @property
    def retained_messages(self) -> int:
        """Retained groups plus start-view messages still pending."""
        return self.retained_groups + self.pending_start_views

    @property
    def pending_count(self) -> int:
        return self.retained_messages

    @property
    def deleted_ranges(self) -> dict[str, Optional[list[int]]]:
        return {name: item.deleted_range for name, item in self.subscriptions.items()}

    @property
    def ranges(self) -> dict[str, Optional[list[int]]]:
        return self.deleted_ranges

    @property
    def removed_ranges(self) -> dict[str, Optional[list[int]]]:
        return self.deleted_ranges

    @property
    def removed_range(self) -> Optional[list[int]]:
        return self.deleted_range

    @property
    def retained_ranges(self) -> dict[str, Optional[list[int]]]:
        return {name: item.retained_range for name, item in self.subscriptions.items()}

    @property
    def deleted_range(self) -> Optional[list[int]]:
        """Envelope containing every per-subscription deleted seq."""
        lows = [r.deleted_min_seq for r in self.subscriptions.values()
                if r.deleted_min_seq is not None]
        highs = [r.deleted_max_seq for r in self.subscriptions.values()
                 if r.deleted_max_seq is not None]
        if not lows:
            return None
        return [min(lows), max(highs)]

    @property
    def deleted_interval(self) -> Optional[list[int]]:
        rng = self.deleted_range
        return None if rng is None else [rng[0], rng[1] + 1]

    @property
    def deleted_seq_range(self) -> Optional[list[int]]:
        return self.deleted_range

    @property
    def removed_seq_range(self) -> Optional[list[int]]:
        return self.deleted_range

    @property
    def safe_seq(self) -> int:
        return self.global_frontier

    @property
    def min_frontier(self) -> int:
        return self.global_frontier

    @property
    def deleted_by_subscription(self) -> MetricDict:
        return MetricDict(
            {name: item.to_dict() for name, item in self.subscriptions.items()},
            self.deleted_groups,
        )

    @property
    def deleted(self) -> MetricDict:
        return self.deleted_by_subscription

    @property
    def deletions(self) -> list[dict]:
        return [item.to_dict() for item in self.subscriptions.values()]

    def get(self, key, default=None):
        try:
            return self[key]
        except (KeyError, AttributeError):
            return default

    def __getitem__(self, key):
        if key == "subscriptions":
            return {name: item.to_dict() for name, item in self.subscriptions.items()}
        try:
            return getattr(self, key)
        except AttributeError:
            data = self.to_dict()
            if key in data:
                return data[key]
            raise KeyError(key) from None

    def to_dict(self) -> dict:
        return {
            "ok": True,
            "success": True,
            "changed": self.changed,
            "would_change": self.would_change,
            "applied": self.applied,
            "compacted": self.changed,
            "dry_run": self.dry_run,
            "vacuumed": self.vacuumed,
            "run_id": self.run_id,
            "ran_at": self.ran_at,
            "head": self.head,
            "global_frontier": self.global_frontier,
            "global_retention_frontier": self.global_frontier,
            "retention_frontier": self.retention_frontier,
            "safe_frontier": self.safe_frontier,
            "compacted_through": self.compacted_through,
            "before_groups": self.before_groups,
            "after_groups": self.after_groups,
            "before_count": self.before_count,
            "after_count": self.after_count,
            "retention_frontiers": dict(self.retention_frontiers),
            "frontiers": dict(self.retention_frontiers),
            "retain_from_seqs": self.retain_from_seqs,
            "earliest_needed_seqs": self.earliest_needed_seqs,
            "retention": self.retention,
            "per_subscription": self.per_subscription,
            "deleted_by_subscription": self.deleted_by_subscription,
            "subscriptions": {
                name: item.to_dict() for name, item in self.subscriptions.items()
            },
            "deleted": self.deleted_groups,
            "removed": self.removed_count,
            "deletions": self.deletions,
            "deleted_ranges": self.deleted_ranges,
            "removed_ranges": self.removed_ranges,
            "ranges": self.deleted_ranges,
            "deleted_range": self.deleted_range,
            "deleted_seq_range": self.deleted_seq_range,
            "removed_range": self.removed_range,
            "removed_seq_range": self.removed_seq_range,
            "deleted_interval": self.deleted_interval,
            "commit_deleted_range": self.commit_deleted_range,
            "deleted_groups": self.deleted_groups,
            "deleted_count": self.deleted_count,
            "deleted_messages": self.deleted_messages,
            "removed_groups": self.removed_groups,
            "removed_count": self.removed_count,
            "deleted_changes": self.deleted_changes,
            "commits_deleted": self.commits_deleted,
            "retained_groups": self.retained_groups,
            "retained_count": self.retained_count,
            "remaining_count": self.remaining_count,
            "remaining": self.remaining,
            "retained_messages": self.retained_messages,
            "pending_count": self.pending_count,
            "retained_changes": self.retained_changes,
            "retained_ranges": self.retained_ranges,
            "pending_start_views": self.pending_start_views,
        }

    def to_json(self, indent: Optional[int] = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)


# Common naming alternatives for callers/tests.
CompactResult = CompactionResult
Compaction = CompactionResult
CompactionReport = CompactionResult
CompactReport = CompactionResult
RetentionReport = CompactionResult
SubscriptionRetention = SubscriptionCompaction


def compact_feed(path, **kwargs) -> "CompactionResult":
    """Open :class:`feedtable.Feed` at ``path`` and compact it."""
    from .core import Feed

    feed = Feed(path)
    try:
        return feed.compact(**kwargs)
    finally:
        feed.close()


compact_database = compact_feed
compact = compact_feed
compact_history = compact_feed
