"""Module alias for compaction's historical-log entry point."""

from .compact import compact_database, main

compact_history = compact_database
compact_feed = compact_database

__all__ = ["compact_history", "compact_feed", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
