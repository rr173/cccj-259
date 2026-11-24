"""Batch journal durability, atomicity and read-cost enforcement."""

import json
import os
import subprocess
import sys
import tempfile
import unittest

from feedtable import (
    BatchRequest,
    Condition,
    Feed,
    REGISTERED,
    RowSpec,
    SKIPPED,
    WITHDRAWN,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class BatchAtomicityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "f.db")
        self.feed = Feed(self.path)
        self.feed.enable_batches()
        self.feed.create_subscription("s", 0, 100)

    def tearDown(self):
        self.feed.close()
        self.tmp.cleanup()

    def snapshot(self):
        return (
            {r["pk"]: (r["route"], r["payload"])
             for r in self.feed.conn.execute(
                 "SELECT pk, route, payload FROM records")},
            {r["batch_id"]: r["status"]
             for r in self.feed.conn.execute(
                 "SELECT batch_id, status FROM batches")},
            self.feed.head(),
        )

    def test_failed_withdrawal_rolls_back_everything(self):
        self.feed.submit_batch(
            BatchRequest("a", (), (RowSpec("hot", 1, {"v": 0}),))
        )
        self.feed.submit_batch(
            BatchRequest("b", (Condition.matches("hot", 1, {"v": 0}),),
                         (RowSpec("hot", 1, {"v": 1}),))
        )
        snap = self.snapshot()

        def boom(*_a, **_k):
            raise RuntimeError("simulated crash during commit")

        self.feed._store_commit = boom
        with self.assertRaises(RuntimeError):
            self.feed.withdraw_batch("a")
        self.assertEqual(self.snapshot(), snap)

    def test_failed_submission_leaves_no_journal_or_rows(self):
        snap = self.snapshot()
        with self.assertRaises(TypeError):
            # A non-serializable payload raises while canonicalising inside
            # the transaction; everything must roll back.
            self.feed.submit_batch(
                BatchRequest("a", (), (RowSpec("x", 1, {"bad": {1}}),))
            )
        self.assertEqual(self.snapshot(), snap)

    def test_state_survives_reopen(self):
        self.feed.submit_batch(
            BatchRequest("a", (), (RowSpec("hot", 1, {"v": 0}),))
        )
        self.feed.submit_batch(
            BatchRequest("b", (Condition.matches("hot", 1, {"v": 0}),),
                         (RowSpec("hot", 1, {"v": 1}),))
        )
        self.feed.close()

        feed = Feed(self.path)
        self.assertTrue(feed.batches_enabled())
        w = feed.withdraw_batch("a")
        self.assertTrue(w.changed)
        feed.close()

        again = Feed(self.path)
        statuses = {b.batch_id: b.status for b in again.batches()}
        self.assertEqual(statuses["a"], WITHDRAWN)
        self.assertEqual(statuses["b"], SKIPPED)
        # The correction group is readable verbatim after reopen.
        self.assertIsNotNone(again.read_group("s", w.notify_seq))
        again.close()


class WithdrawalReadCostTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "f.db")
        self.feed = Feed(self.path)
        self.feed.enable_batches()
        self.feed.create_subscription("s", 0, 1_000_000)
        for i in range(300):
            self.feed.submit_batch(
                BatchRequest(f"ind-{i}", (),
                             (RowSpec(f"k{i}", 1000 + i, {"i": i}),))
            )
        self.feed.submit_batch(
            BatchRequest("t", (), (RowSpec("hot", 1, {"v": 0}),))
        )
        self.feed.submit_batch(
            BatchRequest("u", (Condition.matches("hot", 1, {"v": 0}),),
                         (RowSpec("hot", 1, {"v": 1}),))
        )

    def tearDown(self):
        self.feed.close()
        self.tmp.cleanup()

    def test_no_full_scan_of_records_or_journal(self):
        statements = []
        self.feed.conn.set_trace_callback(statements.append)
        try:
            self.feed.withdraw_batch("t")
        finally:
            self.feed.conn.set_trace_callback(None)

        for sql in statements:
            normalised = " ".join(sql.split())
            upper = normalised.upper()
            if upper.startswith("SELECT") and " FROM RECORDS" in upper:
                point = normalised.startswith(
                    "SELECT pk, route, payload FROM records WHERE pk = "
                )
                segment = normalised.startswith(
                    "SELECT pk, route, payload FROM records INDEXED BY "
                    "records_route_pk WHERE route >= "
                )
                self.assertTrue(point or segment, f"unexpected records read: {sql}")
            for table in ("batch_writes", "batch_conditions",
                          "baseline_records", "batches"):
                if f"FROM {table}" in upper:
                    self.assertIn("WHERE", upper, f"unqualified scan: {sql}")
            self.assertNotIn("SCAN records", upper)

    def test_journal_indexes_lead_with_pk(self):
        plans = {
            "cond": "EXPLAIN QUERY PLAN SELECT DISTINCT batch_id "
                    "FROM batch_conditions WHERE pk IN (?, ?)",
            "write": "EXPLAIN QUERY PLAN SELECT DISTINCT batch_id "
                     "FROM batch_writes WHERE pk IN (?, ?)",
            "events": "EXPLAIN QUERY PLAN SELECT b.seq FROM batch_writes AS w "
                      "JOIN batches AS b ON b.batch_id = w.batch_id "
                      "WHERE w.pk IN (?, ?)",
            "baseline": "EXPLAIN QUERY PLAN SELECT pk FROM baseline_records "
                        "WHERE pk = ?",
        }
        for label, sql in plans.items():
            detail = " ".join(
                r["detail"] for r in self.feed.conn.execute(
                    sql, ("a", "b") if "?" in sql and sql.count("?") == 2 else ("a",)
                )
            )
            self.assertIn("SEARCH", detail, label)
            self.assertNotIn("SCAN ", detail, label)


class BatchCliTest(unittest.TestCase):
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

    def test_batch_workflow_as_separate_processes(self):
        self.cli("init")
        self.cli("enable-batches")
        self.cli("subscribe", "qc-a", "0", "100")
        jia = json.dumps({
            "batch_id": "jia", "conditions": [],
            "writes": [{"pk": "mark", "route": 5, "payload": {"flag": "ok"}}],
        })
        submitted = self.cli("batch-submit", "--tx", "-", stdin=jia)
        self.assertTrue(submitted["applied"])
        # Idempotent retry.
        retry = self.cli("batch-submit", "--tx", "-", stdin=jia)
        self.assertFalse(retry["registered"])

        withdrawn = self.cli("batch-withdraw", "jia")
        self.assertTrue(withdrawn["changed"])
        self.assertIn("jia", withdrawn["audit"]["read_batches"])
        journal = self.cli("batch-status")
        self.assertEqual(journal[0]["status"], WITHDRAWN)

        proc = self.run_cli("batch-withdraw", "ghost", check=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("error:", proc.stderr)


if __name__ == "__main__":
    unittest.main()
