#!/usr/bin/env python3
"""Reference consumer: consume the feed without ever back-filling.

The consumer only uses the delivered start view and delivered commit groups.
Its local roster is kept in a small JSON state file, so successive runs
continue from what was already received instead of refetching.

Per message the order is: apply -> write state file -> acknowledge.  A crash
at any point replays safely:

* crash before the state write: the message is delivered again and applied;
* crash after the state write, before the ack: the message is delivered again,
  but the state file already records its sequence number, so it is skipped and
  only the ack is redone.

The roster is checked after every group: each ``enter`` must be for a pk not
currently held and each ``update``/``exit`` for one that is held.  With
``--source`` the final roster is also compared with the source table (only
when the last message read is the current head).

Run::

    python examples/consumer.py --db roster.db --name qc-a --state qc-a.json --commit
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sqlite3
import sys
import tempfile


def open_db(path):
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


class InMemoryRoster:
    def __init__(self):
        self.rows = {}          # pk -> [route, payload]
        self.last_seq = -1      # last message incorporated into `rows`

    # -- persistence -------------------------------------------------------
    def load(self, path):
        if not path or not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as fh:
            saved = json.load(fh)
        self.rows = saved["rows"]
        self.last_seq = saved["last_seq"]

    def save(self, path):
        if not path:
            return
        directory = os.path.dirname(os.path.abspath(path))
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".consumer-", suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump({"last_seq": self.last_seq, "rows": self.rows}, fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # -- message application ----------------------------------------------
    def apply(self, message):
        rows = message["rows"] if message["kind"] == "start_view" else message["changes"]
        for change in rows:
            pk = change["pk"]
            if message["kind"] == "start_view":
                if pk in self.rows:
                    raise AssertionError(f"duplicate pk {pk!r} in start view")
                self.rows[pk] = [change["route"], change["payload"]]
                continue
            kind = change["kind"]
            if kind == "enter":
                if pk in self.rows:
                    raise AssertionError(f"enter for already-held pk {pk!r}")
                self.rows[pk] = [change["route"], change["payload"]]
            elif kind == "update":
                if pk not in self.rows:
                    raise AssertionError(f"update for unheld pk {pk!r}")
                self.rows[pk] = [change["route"], change["payload"]]
            elif kind == "exit":
                if pk not in self.rows:
                    raise AssertionError(f"exit for unheld pk {pk!r}")
                del self.rows[pk]
            else:
                raise AssertionError(f"unknown change kind {kind!r}")
        self.last_seq = message["seq"]


def source_visible(source_conn, lo, hi):
    rows = source_conn.execute(
        "SELECT pk, route, payload FROM records WHERE route >= ? AND route < ?",
        (lo, hi),
    ).fetchall()
    return {(r["pk"], r["route"], r["payload"]) for r in rows}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="feed database")
    ap.add_argument("--name", required=True, help="subscription name")
    ap.add_argument("--state", help="JSON file holding the consumer's local roster")
    ap.add_argument("--source", help="cross-check the final roster against this database")
    ap.add_argument("--commit", action="store_true", help="acknowledge messages as consumed")
    args = ap.parse_args(argv)

    # Import from the checkout this script lives in, not the current directory.
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
    from feedtable.core import Feed  # noqa: E402

    feed = Feed(args.db)
    roster = InMemoryRoster()
    roster.load(args.state)
    source = open_db(args.source) if args.source else None
    applied = 0
    try:
        for message in feed.pending(args.name):
            if message["seq"] <= roster.last_seq:
                # Applied by a previous run that crashed before acking.
                pass
            else:
                roster.apply(message)
                roster.save(args.state)
                applied += 1
            if args.commit:
                feed.ack(args.name, message["seq"])

        if source is not None:
            sub = feed.subscription(args.name)
            lo, hi = sub["range"]
            mine = {(pk, r, p) for pk, (r, p) in roster.rows.items()}
            visible = source_visible(source, lo, hi)
            if mine != visible:
                raise AssertionError(
                    f"roster diverges from source in [{lo},{hi}): "
                    f"missing={sorted(visible - mine)[:5]} "
                    f"extra={sorted(mine - visible)[:5]}"
                )

        print(json.dumps({
            "subscription": args.name,
            "applied": applied,
            "rows": len(roster.rows),
            "last_seq": roster.last_seq,
            "acked_seq": feed.cursor(args.name),
        }, indent=2))
    finally:
        if source is not None:
            source.close()
        feed.close()


if __name__ == "__main__":
    main()
