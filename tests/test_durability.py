"""Durability, isolation and the offline CLI workflow."""

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from feedtable import Feed, MoveSpec, RowSpec

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class DurabilityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "feed.db")

    def tearDown(self):
        self.tmp.cleanup()

    def open(self):
        return Feed(self.path)

    # -- partial acknowledgement across restarts ---------------------------
    def test_ack_one_group_then_crash_resumes_rest(self):
        feed = self.open()
        feed.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(10)])
        view = feed.create_subscription("s", 0, 10)
        feed.ack("s", view.seq)

        a = feed.apply([RowSpec("r0", 0, {"n": 0, "v": 2})])
        b = feed.apply(moves=[MoveSpec("s", 0, 12)])
        c = feed.apply([RowSpec("r11", 11, {"n": 11})])
        self.assertEqual(feed.cursor("s"), view.seq)
        feed.ack("s", a.seq)          # one group confirmed
        first, rest = feed.pending("s")[:1], feed.pending("s")[1:]
        feed.close()

        feed = self.open()
        self.assertEqual(feed.cursor("s"), a.seq)
        self.assertEqual(feed.pending("s"), first + rest)
        self.assertEqual([m["seq"] for m in feed.pending("s")], [b.seq, c.seq])
        feed.ack("s", c.seq)
        feed.close()

        feed = self.open()
        self.assertEqual(feed.pending("s"), [])
        self.assertEqual(feed.cursor("s"), c.seq)
        feed.close()

    def test_acked_progress_never_rewrites_history(self):
        feed = self.open()
        feed.apply([RowSpec("a", 1, {"v": 1})])
        view = feed.create_subscription("s", 0, 10)
        feed.ack("s", view.seq)
        first = feed.apply([RowSpec("a", 1, {"v": 2})])
        # Move the range so any recomputation would produce different rows.
        feed.apply(moves=[MoveSpec("s", 100, 200)])
        feed.close()

        feed = self.open()
        group = feed.read_group("s", first.seq)
        self.assertEqual([(c.kind, c.pk) for c in group.changes], [("update", "a")])
        self.assertEqual(group.to_dict()["range"], [0, 10])
        # The replayed payload is the value as of that commit, not now.
        feed.apply([RowSpec("a", 1, {"v": 3})])
        self.assertEqual(
            [(c.kind, c.pk, c.payload) for c in feed.read_group("s", first.seq).changes],
            [("update", "a", '{"v":2}')],
        )
        feed.close()

    # -- no partial results ------------------------------------------------
    def test_second_connection_never_sees_partial_transaction(self):
        feed = self.open()
        feed.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(5)])
        view = feed.create_subscription("s", 0, 5)
        feed.ack("s", view.seq)

        other = sqlite3.connect(self.path, isolation_level=None, timeout=5)
        other.row_factory = sqlite3.Row
        try:
            # Start a write transaction and inspect it from the other
            # connection *before* it commits.
            feed.conn.execute("BEGIN IMMEDIATE")
            feed.conn.execute(
                "UPDATE records SET payload = ? WHERE pk = 'r1'", ('{"n":1,"v":2}',)
            )
            feed.conn.execute(
                "UPDATE subscriptions SET lo = ?, hi = ? WHERE name = 's'", (5, 9)
            )
            feed.conn.execute(
                "INSERT INTO commits (seq, created_at, meta, audit) VALUES (9, 0, '{}', '{}')"
            )
            # A concurrent reader sees the last committed state only.
            self.assertEqual(other.execute("SELECT payload FROM records WHERE pk='r1'")
                             .fetchone()["payload"], '{"n":1}')
            self.assertEqual(other.execute("SELECT lo, hi FROM subscriptions WHERE name='s'")
                             .fetchone()["lo"], 0)
            self.assertEqual(other.execute("SELECT count(*) FROM commits WHERE seq=9")
                             .fetchone()[0], 0)
            feed.conn.execute("ROLLBACK")
        finally:
            other.close()
            feed.close()

    def test_rollback_after_writes_publishes_nothing(self):
        feed = self.open()
        feed.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(5)])
        view = feed.create_subscription("s", 0, 5)
        feed.ack("s", view.seq)
        head = feed.head()
        with self.assertRaises(TypeError):
            feed.apply(
                [RowSpec("r1", 1, {"ok": True}), RowSpec("r9", 4, {"bad": {1}})],
                [MoveSpec("s", 0, 9)],
            )
        self.assertEqual(feed.head(), head)
        self.assertEqual(feed.pending("s"), [])
        self.assertEqual(feed.subscription("s")["range"], [0, 5])
        self.assertEqual(
            feed.conn.execute("SELECT payload FROM records WHERE pk='r1'").fetchone()[0],
            '{"n":1}',
        )
        feed.close()

    # -- persisted audit ---------------------------------------------------
    def test_audit_is_persisted_with_commit(self):
        feed = self.open()
        feed.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(30)])
        view = feed.create_subscription("s", 0, 10)
        feed.ack("s", view.seq)
        out = feed.apply([], [MoveSpec("s", 5, 15)])
        stored = feed.commit_info(out.seq)
        self.assertEqual(stored["audit"], out.audit.to_dict())
        self.assertEqual(stored["audit"]["candidate_rows"], 10)
        feed.close()

    # -- edge cases --------------------------------------------------------
    def test_negative_routes_and_ranges(self):
        feed = self.open()
        feed.apply([RowSpec("n1", -5, {"v": 1}), RowSpec("n2", -1, {"v": 2}),
                    RowSpec("p1", 1, {"v": 3})])
        view = feed.create_subscription("s", -5, 0)
        self.assertEqual([r["pk"] for r in view.rows], ["n1", "n2"])
        feed.ack("s", view.seq)
        out = feed.apply([], [MoveSpec("s", -1, 2)])
        self.assertEqual([(c.kind, c.pk) for c in out.changes["s"]],
                         [("exit", "n1"), ("enter", "p1")])
        feed.close()

    def test_empty_subscription_interval(self):
        feed = self.open()
        feed.apply([RowSpec("a", 1, {"v": 1})])
        view = feed.create_subscription("s", 3, 3)
        self.assertEqual(view.rows, [])
        feed.ack("s", view.seq)
        out = feed.apply([RowSpec("a", 1, {"v": 2})])
        self.assertFalse(out.committed)
        out = feed.apply([], [MoveSpec("s", 0, 2)])
        self.assertEqual([(c.kind, c.pk) for c in out.changes["s"]], [("enter", "a")])
        feed.close()

    def test_read_group_unknown_seq(self):
        feed = self.open()
        feed.apply([RowSpec("a", 1, {"v": 1})])
        view = feed.create_subscription("s", 0, 5)
        self.assertIsNone(feed.read_group("s", view.seq))  # start view is not a group
        self.assertIsNone(feed.read_group("s", 999))
        feed.close()

    def test_unknown_subscription_errors(self):
        feed = self.open()
        with self.assertRaises(KeyError):
            feed.subscription("nope")
        with self.assertRaises(KeyError):
            feed.pending("nope")
        with self.assertRaises(KeyError):
            feed.ack("nope", 0)
        with self.assertRaises(ValueError):
            feed.create_subscription("s", 5, 4)
        feed.close()

    def test_duplicate_subscription_rejected(self):
        feed = self.open()
        feed.create_subscription("s", 0, 1)
        with self.assertRaises(ValueError):
            feed.create_subscription("s", 0, 1)
        feed.close()


class CliTest(unittest.TestCase):
    """The whole workflow as separate processes, as an operator would run it."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "roster.db")

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *args, stdin=None, check=True):
        env = dict(os.environ, PYTHONPATH=ROOT)
        proc = subprocess.run(
            [sys.executable, "-m", "feedtable", "--db", self.db, *args],
            input=stdin, capture_output=True, text=True, env=env, cwd=ROOT,
        )
        if check and proc.returncode != 0:
            self.fail(f"cli failed: {proc.stderr}")
        return proc

    def cli(self, *args, stdin=None):
        return json.loads(self.run_cli(*args, stdin=stdin).stdout)

    def test_end_to_end(self):
        self.cli("init")
        rows = "\n".join(
            json.dumps({"pk": f"sku-{i:03d}", "route": i, "payload": {"n": i}})
            for i in range(12)
        )
        load = self.cli("load", stdin=rows)
        self.assertFalse(load["committed"])  # no subscription yet

        view = self.cli("subscribe", "qc-a", "0", "5")
        self.assertEqual([r["pk"] for r in view["rows"]],
                         [f"sku-{i:03d}" for i in range(5)])
        self.cli("ack", "qc-a", str(view["seq"]))

        tx = json.dumps({
            "writes": [
                {"pk": "sku-001", "route": 1, "payload": {"n": 1, "hold": True}},
                {"pk": "sku-007", "route": 7, "payload": {"n": 7}},
            ],
            "moves": [{"subscription": "qc-a", "lo": 2, "hi": 8}],
            "meta": {"operator": "alice"},
        })
        result = self.cli("apply", "--tx", "-", stdin=tx)
        self.assertTrue(result["committed"])
        self.assertEqual(
            [(c["kind"], c["pk"]) for c in result["changes"]["qc-a"]],
            [("exit", "sku-000"), ("exit", "sku-001"),
             ("enter", "sku-005"), ("enter", "sku-006"), ("enter", "sku-007")],
        )
        self.assertEqual(result["audit"]["stable_rows_read"], 0)

        # Fresh process: unacknowledged group still there, in order.
        pending = self.cli("pending", "qc-a")
        self.assertEqual([m["seq"] for m in pending], [result["seq"]])
        self.cli("ack", "qc-a", str(result["seq"]))
        self.assertEqual(self.cli("pending", "qc-a"), [])

        status = self.cli("status")
        self.assertEqual(status["head"], result["seq"])
        self.assertEqual(status["subscriptions"][0]["lo"], 2)

    def test_cli_errors_are_nonzero(self):
        self.cli("init")
        proc = self.run_cli("subscribe", "bad", "5", "4", check=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("error:", proc.stderr)


if __name__ == "__main__":
    unittest.main()
