"""Batch journal: registration, preconditions and idempotent submission."""

import os
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
    batch_fingerprint,
)


class BatchEnablingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "f.db")
        self.feed = Feed(self.path)

    def tearDown(self):
        self.feed.close()
        self.tmp.cleanup()

    def rows(self):
        return {
            r["pk"]: (r["route"], r["payload"])
            for r in self.feed.conn.execute("SELECT pk, route, payload FROM records")
        }

    def test_enable_snapshots_existing_rows_and_is_idempotent(self):
        self.feed.apply([RowSpec("old", 1, {"v": 1}), RowSpec("old2", 2, None)])
        result = self.feed.enable_batches()
        self.assertTrue(result["enabled"])
        self.assertEqual(result["baseline_rows"], 2)
        baseline = {
            r["pk"]: (r["route"], r["payload"])
            for r in self.feed.conn.execute("SELECT pk, route, payload FROM baseline_records")
        }
        self.assertEqual(baseline, self.rows())
        again = self.feed.enable_batches()
        self.assertFalse(again["enabled"])
        self.assertEqual(again["baseline_rows"], 2)
        self.assertTrue(self.feed.batches_enabled())

    def test_pre_enable_writes_are_not_retroactively_journaled(self):
        self.feed.apply([RowSpec("old", 1, {})])
        self.feed.enable_batches()
        self.assertEqual(self.feed.batches(), [])
        # The pre-enable row cannot be withdrawn through the journal.
        with self.assertRaises(KeyError):
            self.feed.withdraw_batch("old")

    def test_direct_record_writes_are_rejected_after_enable(self):
        self.feed.apply([RowSpec("seed", 1, {})])
        self.feed.enable_batches()
        before = self.rows()
        with self.assertRaises(RuntimeError):
            self.feed.apply([RowSpec("x", 2, {})])
        with self.assertRaises(RuntimeError):
            self.feed.apply([RowSpec.delete_pk("seed")])
        # Nothing changed.
        self.assertEqual(self.rows(), before)

    def test_range_moves_still_use_apply_after_enable(self):
        self.feed.enable_batches()
        view = self.feed.create_subscription("s", 0, 10)
        self.feed.ack("s", view.seq)
        out = self.feed.apply(moves=[__import__("feedtable").MoveSpec("s", 5, 15)])
        self.assertTrue(out.committed)
        self.assertEqual(self.feed.subscription("s")["range"], [5, 15])

    def test_submit_before_enable_is_rejected(self):
        with self.assertRaises(RuntimeError):
            self.feed.submit_batch(
                BatchRequest("b", (), (RowSpec("a", 1, {}),))
            )


class ConditionTest(unittest.TestCase):
    def test_absent_requires_no_row_at_all(self):
        Condition.absent("k")
        with self.assertRaises(ValueError):
            Condition("k", "absent", route=3)
        with self.assertRaises(TypeError):
            Condition("", "absent")

    def test_matches_requires_route(self):
        with self.assertRaises(ValueError):
            Condition.matches("k", None)  # type: ignore[arg-type]
        c = Condition.matches("k", 3, {"a": 1})
        self.assertEqual(c.kind, "matches")
        self.assertEqual(c.payload_value, '{"a":1}')

    def test_round_trip_through_dict(self):
        for c in (Condition.absent("k"), Condition.matches("k", 3, None),
                  Condition.matches("j", -1, {"x": [1, 2]})):
            self.assertEqual(Condition.from_dict(c.to_dict()).canonical(), c.canonical())

    def test_fingerprint_ignores_condition_order_not_write_content(self):
        writes = (RowSpec("a", 1, {}),)
        r1 = BatchRequest("b", (Condition.absent("z"),
                                Condition.matches("a", 1, {"k": 1})), writes)
        r2 = BatchRequest("b", (Condition.matches("a", 1, {"k": 1}),
                                Condition.absent("z")), writes)
        self.assertEqual(batch_fingerprint(r1), batch_fingerprint(r2))
        r3 = BatchRequest("b", (), (RowSpec("a", 2, {}),))
        self.assertNotEqual(batch_fingerprint(r1), batch_fingerprint(r3))


class BatchRequestValidationTest(unittest.TestCase):
    def test_needs_nonempty_writes(self):
        with self.assertRaises(ValueError):
            BatchRequest("b", (), ())

    def test_one_write_per_pk(self):
        with self.assertRaises(ValueError):
            BatchRequest("b", (), (RowSpec("a", 1, {}), RowSpec("a", 2, {})))

    def test_id_must_be_nonempty_string(self):
        with self.assertRaises(TypeError):
            BatchRequest("", (), (RowSpec("a", 1, {}),))


class SubmissionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "f.db")
        self.feed = Feed(self.path)
        self.feed.enable_batches()

    def tearDown(self):
        self.feed.close()
        self.tmp.cleanup()

    def rows(self):
        return {
            r["pk"]: (r["route"], r["payload"])
            for r in self.feed.conn.execute("SELECT pk, route, payload FROM records")
        }

    def test_successful_batch_applies_and_registers(self):
        out = self.feed.submit_batch(
            BatchRequest("b1", (), (RowSpec("a", 1, {"v": 1}),
                                   RowSpec.delete_pk("ghost"),))
        )
        self.assertTrue(out.registered)
        self.assertTrue(out.applied)
        self.assertEqual(out.seq, 1)
        status = self.feed.batch_status("b1")
        self.assertEqual(status.status, REGISTERED)
        self.assertEqual(status.write_count, 2)
        self.assertEqual(status.condition_count, 0)

    def test_failed_preconditions_register_but_write_nothing(self):
        req = BatchRequest(
            "b1",
            (Condition.absent("a"),),
            (RowSpec("a", 1, {}),),
        )
        # First make "a" exist.
        self.feed.submit_batch(BatchRequest("seed", (), (RowSpec("a", 9, {}),)))
        before = self.rows()
        out = self.feed.submit_batch(req)
        self.assertTrue(out.registered)
        self.assertFalse(out.applied)
        self.assertIsNone(out.notify_seq)
        self.assertEqual(self.rows(), before)
        self.assertEqual(self.feed.batch_status("b1").status, SKIPPED)

    def test_null_payload_row_is_not_absent(self):
        self.feed.submit_batch(
            BatchRequest("seed", (), (RowSpec("nul", 1, None),))
        )
        fail = self.feed.submit_batch(
            BatchRequest("x", (Condition.absent("nul"),), (RowSpec("a", 1, {}),))
        )
        self.assertFalse(fail.applied)
        ok = self.feed.submit_batch(
            BatchRequest("y", (Condition.matches("nul", 1, None),),
                         (RowSpec("a", 1, {}),))
        )
        self.assertTrue(ok.applied)

    def test_matches_compares_route_and_payload_exactly(self):
        self.feed.submit_batch(
            BatchRequest("seed", (), (RowSpec("k", 1, {"v": 1}),))
        )
        wrong_route = self.feed.submit_batch(
            BatchRequest("a", (Condition.matches("k", 2, {"v": 1}),),
                         (RowSpec("a", 1, {}),))
        )
        wrong_payload = self.feed.submit_batch(
            BatchRequest("b", (Condition.matches("k", 1, {"v": 2}),),
                         (RowSpec("b", 1, {}),))
        )
        exact = self.feed.submit_batch(
            BatchRequest("c", (Condition.matches("k", 1, {"v": 1}),),
                         (RowSpec("c", 1, {}),))
        )
        self.assertFalse(wrong_route.applied)
        self.assertFalse(wrong_payload.applied)
        self.assertTrue(exact.applied)

    def test_idempotent_identical_retry_does_not_reregister(self):
        req = BatchRequest("b", (Condition.matches("k", 1, {"v": 1}),),
                           (RowSpec("a", 1, {}),))
        first = self.feed.submit_batch(req)  # skipped: k missing
        self.assertTrue(first.registered)
        second = self.feed.submit_batch(req)
        self.assertFalse(second.registered)
        self.assertEqual(second.seq, first.seq)
        self.assertFalse(second.applied)
        self.assertEqual(len(self.feed.batches()), 1)

    def test_same_id_different_content_is_rejected_and_changes_nothing(self):
        req = BatchRequest("b", (), (RowSpec("a", 1, {}),))
        self.feed.submit_batch(req)
        before = self.rows()
        with self.assertRaises(ValueError):
            self.feed.submit_batch(
                BatchRequest("b", (), (RowSpec("a", 1, {"different": True}),))
            )
        self.assertEqual(self.rows(), before)
        self.assertEqual(len(self.feed.batches()), 1)

    def test_registration_order_is_dense_and_preserved(self):
        for i in range(5):
            self.feed.submit_batch(
                BatchRequest(f"b{i}", (Condition.absent("never"),),
                             (RowSpec(f"k{i}", i, {}),))
            )
        seqs = [b.seq for b in self.feed.batches()]
        self.assertEqual(seqs, [1, 2, 3, 4, 5])

    def test_effective_batch_publishes_notification_skipped_does_not(self):
        view = self.feed.create_subscription("s", 0, 100)
        self.feed.ack("s", view.seq)
        ok = self.feed.submit_batch(
            BatchRequest("ok", (), (RowSpec("in", 5, {}),))
        )
        self.assertIsNotNone(ok.notify_seq)
        bad = self.feed.submit_batch(
            BatchRequest("bad", (Condition.absent("in"),), (RowSpec("x", 6, {}),))
        )
        self.assertIsNone(bad.notify_seq)
        seqs = [m["seq"] for m in self.feed.pending("s")]
        self.assertEqual(seqs, [ok.notify_seq])


if __name__ == "__main__":
    unittest.main()
