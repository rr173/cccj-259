"""Command line access to a feed database.

Every subcommand is a single process that opens the database, does one thing and
exits; all state is in the file.  That makes the offline "kill it anywhere and
re-run" workflow testable::

    python -m feedtable --db roster.db init
    python -m feedtable --db roster.db load skus.jsonl
    python -m feedtable --db roster.db subscribe qc-a 0 100
    python -m feedtable --db roster.db apply --move qc-a:50:150 --upsert "sku-1:42:{...}"
    python -m feedtable --db roster.db pending qc-a
    python -m feedtable --db roster.db ack qc-a 3

``apply`` reads a JSON transaction from ``--tx`` or stdin:

.. code-block:: json

    {
      "writes": [
        {"pk": "sku-1", "route": 42, "payload": {"status": "new"}},
        {"pk": "sku-2", "delete": true}
      ],
      "moves": [{"subscription": "qc-a", "lo": 50, "hi": 150}],
      "meta": {"operator": "alice"}
    }
"""

from __future__ import annotations

import argparse
import json
import sys

from .core import Feed, MoveSpec, RowSpec
from .batches import BatchRequest, Condition


def _load_json(path):
    if path in (None, "-"):
        return json.load(sys.stdin)
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _transaction_from(obj):
    writes = []
    for spec in obj.get("writes", []):
        if spec.get("delete"):
            writes.append(RowSpec.delete_pk(spec["pk"]))
        else:
            payload = spec.get("payload")
            writes.append(RowSpec(spec["pk"], spec["route"], payload))
    moves = [
        MoveSpec(m["subscription"], m["lo"], m["hi"]) for m in obj.get("moves", [])
    ]
    return writes, moves, obj.get("meta")


def _batch_request_from(obj):
    """Build a :class:`BatchRequest` from an operator JSON document.

    Accepted condition shapes::

        {"pk": "sku-1", "kind": "absent"}
        {"pk": "sku-2", "kind": "matches", "route": 7, "payload": {"v": 1}}

    Writes reuse the transaction shape (``delete: true`` or route + payload).
    """
    conditions = [Condition.from_dict(c) for c in obj.get("conditions", [])]
    writes = []
    for spec in obj["writes"]:
        if spec.get("delete"):
            writes.append(RowSpec.delete_pk(spec["pk"]))
        else:
            writes.append(RowSpec(spec["pk"], spec["route"], spec.get("payload")))
    return BatchRequest(obj["batch_id"], conditions, writes)


def cmd_init(args):
    feed = Feed(args.db)
    feed.close()
    print(json.dumps({"db": args.db, "head": 0}))


def cmd_load(args):
    feed = Feed(args.db)
    rows = []
    stream = sys.stdin if args.path in (None, "-") else open(args.path, "r", encoding="utf-8")
    try:
        for line in stream:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            rows.append(RowSpec(obj["pk"], obj["route"], obj.get("payload")))
    finally:
        if stream is not sys.stdin:
            stream.close()
    result = feed.apply(rows)
    feed.close()
    print(json.dumps(result.to_dict(), indent=2))


def cmd_subscribe(args):
    feed = Feed(args.db)
    view = feed.create_subscription(args.name, args.lo, args.hi)
    feed.close()
    print(json.dumps(view.to_dict(), indent=2))


def cmd_apply(args):
    feed = Feed(args.db)
    if args.tx:
        writes, moves, meta = _transaction_from(_load_json(args.tx))
    else:
        writes, moves, meta = [], [], None
        for item in args.upsert:
            pk, route, payload = item.split(":", 2)
            writes.append(RowSpec(pk, int(route), payload))
        for item in args.delete:
            writes.append(RowSpec.delete_pk(item))
        for item in args.move:
            name, lo, hi = item.rsplit(":", 2)
            moves.append(MoveSpec(name, int(lo), int(hi)))
    result = feed.apply(writes, moves, meta=meta)
    feed.close()
    print(json.dumps(result.to_dict(), indent=2))


def cmd_pending(args):
    feed = Feed(args.db)
    messages = feed.pending(args.name, limit=args.limit)
    feed.close()
    print(json.dumps(messages, indent=2))


def cmd_ack(args):
    feed = Feed(args.db)
    feed.ack(args.name, args.seq)
    print(json.dumps({"subscription": args.name, "acked_seq": feed.cursor(args.name)}))
    feed.close()


def cmd_compact(args):
    feed = Feed(args.db)
    try:
        result = feed.compact(
            dry_run=args.dry_run,
            vacuum=args.vacuum,
            delete_commit_metadata=not args.keep_commit_metadata,
        )
        print(json.dumps(result.to_dict(), indent=2))
    finally:
        feed.close()


def cmd_compaction_status(args):
    feed = Feed(args.db)
    try:
        if args.limit == 1:
            result = feed.last_compaction()
        else:
            result = feed.compaction_runs(limit=args.limit)
        print(json.dumps(result, indent=2))
    finally:
        feed.close()


def cmd_status(args):
    feed = Feed(args.db)
    print(json.dumps({
        "head": feed.head(),
        "batches_enabled": feed.batches_enabled(),
        "subscriptions": feed.subscriptions(),
    }, indent=2))
    feed.close()


def cmd_enable_batches(args):
    feed = Feed(args.db)
    result = feed.enable_batches()
    result["batches_enabled"] = feed.batches_enabled()
    feed.close()
    print(json.dumps(result, indent=2))


def cmd_batch_submit(args):
    feed = Feed(args.db)
    request = _batch_request_from(_load_json(args.tx))
    result = feed.submit_batch(request)
    feed.close()
    print(json.dumps(result.to_dict(), indent=2))


def cmd_batch_withdraw(args):
    feed = Feed(args.db)
    result = feed.withdraw_batch(args.batch_id)
    feed.close()
    print(json.dumps(result.to_dict(), indent=2))


def cmd_batch_status(args):
    feed = Feed(args.db)
    if args.batch_id:
        result = feed.batch_status(args.batch_id).to_dict()
    else:
        result = [s.to_dict() for s in feed.batches()]
    feed.close()
    print(json.dumps(result, indent=2))


def build_parser():
    p = argparse.ArgumentParser(prog="feedtable", description=__doc__)
    p.add_argument("--db", required=True, help="path to the feed database")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("init", help="create the database")
    sp.set_defaults(func=cmd_init)

    sp = sub.add_parser("load", help="bulk upsert JSONL rows (pk, route, payload)")
    sp.add_argument("path", nargs="?", default="-")
    sp.set_defaults(func=cmd_load)

    sp = sub.add_parser("subscribe", help="create a subscription and get its start view")
    sp.add_argument("name")
    sp.add_argument("lo", type=int)
    sp.add_argument("hi", type=int)
    sp.set_defaults(func=cmd_subscribe)

    sp = sub.add_parser("apply", help="commit one transaction of writes and/or moves")
    sp.add_argument("--tx", help="JSON transaction file ('-' for stdin)")
    sp.add_argument("--upsert", action="append", default=[], metavar="PK:ROUTE:PAYLOAD")
    sp.add_argument("--delete", action="append", default=[], metavar="PK")
    sp.add_argument("--move", action="append", default=[], metavar="NAME:LO:HI")
    sp.set_defaults(func=cmd_apply)

    sp = sub.add_parser("pending", help="unacknowledged messages in order")
    sp.add_argument("name")
    sp.add_argument("--limit", type=int)
    sp.set_defaults(func=cmd_pending)

    sp = sub.add_parser("ack", help="acknowledge a sequence number")
    sp.add_argument("name")
    sp.add_argument("seq", type=int)
    sp.set_defaults(func=cmd_ack)

    sp = sub.add_parser(
        "compact",
        aliases=["compact-history", "compact_history", "compact-announcements",
                 "compress", "gc", "prune", "prune-history", "prune_history"],
        help="delete confirmed announcement groups safe for every subscription",
    )
    sp.add_argument("--dry-run", action="store_true",
                    help="calculate and print the reclaimable range without deleting")
    sp.add_argument("--vacuum", action="store_true",
                    help="run SQLite VACUUM after commit to shrink the database file")
    sp.add_argument("--keep-commit-metadata", action="store_true",
                    help="retain the global commits/audit rows even below the safe frontier")
    sp.set_defaults(func=cmd_compact)

    sp = sub.add_parser(
        "compaction-status",
        aliases=["compactions"],
        help="show persisted compaction audit records",
    )
    sp.add_argument("--limit", type=int, default=20)
    sp.set_defaults(func=cmd_compaction_status)

    sp = sub.add_parser("status", help="head and subscriptions")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser(
        "enable-batches",
        help="snapshot the table and make batch journaling the only write path",
    )
    sp.set_defaults(func=cmd_enable_batches)

    sp = sub.add_parser(
        "batch-submit",
        help="register and evaluate one batch from a JSON document (--tx)",
    )
    sp.add_argument("--tx", required=True, help="JSON batch file ('-' for stdin)")
    sp.set_defaults(func=cmd_batch_submit)

    sp = sub.add_parser("batch-withdraw", help="revoke one batch by id")
    sp.add_argument("batch_id")
    sp.set_defaults(func=cmd_batch_withdraw)

    sp = sub.add_parser(
        "batch-status",
        help="show one batch, or the whole journal in registration order",
    )
    sp.add_argument("batch_id", nargs="?")
    sp.set_defaults(func=cmd_batch_status)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except (KeyError, ValueError, TypeError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
