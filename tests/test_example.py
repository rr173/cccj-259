"""The reference consumer in examples/ must converge using deliveries only."""

import json
import os
import subprocess
import sys
import tempfile
import unittest

from feedtable import Feed, MoveSpec, RowSpec

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONSUMER = os.path.join(ROOT, "examples", "consumer.py")


class ExampleConsumerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "roster.db")
        self.state = os.path.join(self.tmp.name, "consumer.json")
        feed = Feed(self.db)
        feed.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(40)])
        feed.create_subscription("qc", 0, 10)
        feed.close()

    def tearDown(self):
        self.tmp.cleanup()

    def consume(self, *extra):
        proc = subprocess.run(
            [sys.executable, CONSUMER, "--db", self.db, "--name", "qc",
             "--state", self.state, *extra],
            capture_output=True, text=True,
        )
        if proc.returncode != 0:
            self.fail(proc.stderr)
        return json.loads(proc.stdout)

    def live_rows(self):
        feed = Feed(self.db)
        try:
            lo, hi = feed.subscription("qc")["range"]
            return {
                r["pk"]: [r["route"], r["payload"]]
                for r in feed.conn.execute(
                    "SELECT pk, route, payload FROM records WHERE route >= ? AND route < ?",
                    (lo, hi),
                )
            }
        finally:
            feed.close()

    def test_consume_slice_consume(self):
        out = self.consume("--commit", "--source", self.db)
        self.assertEqual(out["rows"], 10)
        self.assertEqual(out["applied"], 1)  # start view

        feed = Feed(self.db)
        feed.apply(
            [RowSpec("r5", 5, {"n": 5, "v": 2}), RowSpec.delete_pk("r7")],
            [MoveSpec("qc", 3, 12)],
        )
        feed.close()

        out = self.consume("--commit", "--source", self.db)
        self.assertEqual(out["rows"], len(self.live_rows()))
        again = self.consume("--commit", "--source", self.db)
        self.assertEqual(again["applied"], 0)

    def test_unacked_start_view_and_reslice_are_both_replayed(self):
        feed = Feed(self.db)
        feed.apply([RowSpec("r5", 5, {"n": 5, "v": 2})], [MoveSpec("qc", 5, 20)])
        feed.close()
        out = self.consume("--source", self.db)   # no --commit
        self.assertEqual(out["applied"], 2)       # start view + one group
        self.assertIsNone(out["acked_seq"])
        self.assertEqual(out["rows"], len(self.live_rows()))

    def test_resume_after_crash_between_state_write_and_ack(self):
        self.consume("--commit")
        feed = Feed(self.db)
        feed.apply(moves=[MoveSpec("qc", 5, 20)])
        head = feed.head()
        feed.close()

        # Run without --commit: applies and writes state, but acknowledges none.
        out = self.consume("--source", self.db)
        self.assertEqual(out["applied"], 1)
        self.assertEqual(out["acked_seq"], 0)   # start view only, not the group

        # Re-run with --commit: the message is delivered again (unacked) but
        # already in the state file, so it is skipped and only the ack is done.
        out = self.consume("--commit", "--source", self.db)
        self.assertEqual(out["applied"], 0)
        self.assertEqual(out["acked_seq"], head)
        self.assertEqual(out["rows"], len(self.live_rows()))

        # Nothing left, nothing re-applied.
        out = self.consume("--commit", "--source", self.db)
        self.assertEqual(out["applied"], 0)
        self.assertEqual(out["acked_seq"], head)


if __name__ == "__main__":
    unittest.main()
