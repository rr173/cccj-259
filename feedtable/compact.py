"""Convenience Python and command-line entry point for compaction.

The main supported API remains :meth:`feedtable.Feed.compact` and the main CLI
remains ``python -m feedtable --db DB compact``.  This module additionally
allows ``python -m feedtable.compact DB`` and
``from feedtable.compact import compact_database``.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

from .compaction import (
    CompactReport,
    CompactResult,
    CompactionReport,
    CompactionResult,
    SubscriptionCompaction,
)
from .core import Feed


def compact_database(path, **kwargs) -> CompactionResult:
    """Open a feed at ``path``, compact it, and close it."""
    feed = Feed(path)
    try:
        return feed.compact(**kwargs)
    finally:
        feed.close()


# Short alias for ``from feedtable.compact import compact``.
compact = compact_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m feedtable.compact",
        description="Safely compact acknowledged feedtable announcements.",
    )
    parser.add_argument("path", nargs="?", help="path to the SQLite database")
    parser.add_argument("--db", dest="db", help="database path (alternative to PATH)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--vacuum", action="store_true")
    parser.add_argument("--keep-commit-metadata", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    path = args.db or args.path
    if not path:
        print("error: database path is required", file=sys.stderr)
        return 2
    result = compact_database(
        path,
        dry_run=args.dry_run,
        vacuum=args.vacuum,
        delete_commit_metadata=not args.keep_commit_metadata,
    )
    print(json.dumps(result.to_dict(), indent=2))
    return 0


__all__ = [
    "CompactReport",
    "CompactResult",
    "CompactionReport",
    "CompactionResult",
    "SubscriptionCompaction",
    "compact",
    "compact_database",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
