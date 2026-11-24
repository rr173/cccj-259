"""Withdrawal: re-understanding the table from the enable-time baseline."""

import os
import tempfile
import unittest

from feedtable import (
    BatchRequest,
    Condition,
    Feed,
    MoveSpec,
    REGISTERED,
    RowSpec,
    SKIPPED,
    WITHDRAWN,
)


class WithdrawalTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "f.db")
        self.feed = Feed(self.path)
        # Irrevocable starting point.
        self.feed.apply([RowSpec("base", 1, {"k": 0})])
        self.feed.enable_batches()
        self.view = self.feed.create_subscription("s", 0, 100)
        self.feed.ack("s", self.view.seq)

    def tearDown(self):
        self.feed.close()
        self.tmp.cleanup()

    def rows(self):
        return {
            r["pk"]: (r["route"], r["payload"])
            for r in self.feed.conn.execute("SELECT pk, route, payload FROM records")
        }

    def build_chain(self):
        """甲 mark -> 乙 two records gated on mark -> 丙 gated on d1 absent."""
        jia = self.feed.submit_batch(
            BatchRequest("jia", (), (RowSpec("mark", 5, {"flag": "ok"}),))
        )
        yi = self.feed.submit_batch(
            BatchRequest(
                "yi",
                (Condition.matches("mark", 5, {"flag": "ok"}),),
                (RowSpec("d1", 6, {"v": 1}), RowSpec("d2", 7, {"v": 2})),
            )
        )
        bing = self.feed.submit_batch(
            BatchRequest(
                "bing",
                (Condition.absent("d1"),),
                (RowSpec("review", 8, {"wait": True}),),
            )
        )
        return jia, yi, bing

    def test_spec_example_withdraw_jia(self):
        self.build_chain()
        self.assertEqual(self.feed.batch_status("bing").status, SKIPPED)
        result = self.feed.withdraw_batch("jia")
        self.assertTrue(result.changed)
        rows = self.rows()
        # 甲's mark and 乙's two records disappear; 丙 now takes effect.
        self.assertNotIn("mark", rows)
        self.assertNotIn("d1", rows)
        self.assertNotIn("d2", rows)
        self.assertEqual(rows["review"], (8, '{"wait":true}'))
        # Baseline is untouched.
        self.assertEqual(rows["base"], (1, '{"k":0}'))
        statuses = {s.batch_id: s.status for s in result.statuses}
        self.assertEqual(statuses["jia"], WITHDRAWN)
        self.assertEqual(statuses["yi"], SKIPPED)
        self.assertEqual(statuses["bing"], REGISTERED)

    def test_withdrawal_publishes_one_net_correction(self):
        jia, yi, _bing = self.build_chain()
        head_before = self.feed.head()
        result = self.feed.withdraw_batch("jia")
        self.assertIsNotNone(result.notify_seq)
        self.assertGreater(result.notify_seq, head_before)
        group = self.feed.read_group("s", result.notify_seq)
        kinds = {(c.kind, c.pk) for c in group.changes}
        self.assertEqual(
            kinds,
            {("exit", "mark"), ("exit", "d1"), ("exit", "d2"),
             ("enter", "review")},
        )
        # One new group is appended; it is what pending shows last.  The
        # earlier groups (jia, yi) remain stored verbatim.
        pending = self.feed.pending("s")
        self.assertEqual(pending[-1]["seq"], result.notify_seq)
        self.assertEqual([m["seq"] for m in pending],
                         [jia.notify_seq, yi.notify_seq, result.notify_seq])
        # Historical groups are still readable byte-for-byte.
        for earlier in (jia.notify_seq, yi.notify_seq):
            self.assertIsNotNone(self.feed.read_group("s", earlier))

    def test_repeat_withdrawal_is_a_noop(self):
        self.build_chain()
        first = self.feed.withdraw_batch("jia")
        rows = self.rows()
        head = self.feed.head()
        second = self.feed.withdraw_batch("jia")
        self.assertFalse(second.changed)
        self.assertIsNone(second.notify_seq)
        self.assertEqual(self.feed.head(), head)
        self.assertEqual(self.rows(), rows)

    def test_unknown_batch_id_is_an_error(self):
        with self.assertRaises(KeyError):
            self.feed.withdraw_batch("ghost")

    def test_withdraw_skipped_batch_moves_nothing(self):
        # 丙 is already skipped; withdrawing it changes no rows and reads
        # nothing beyond its own details.
        self.build_chain()
        head = self.feed.head()
        rows = self.rows()
        result = self.feed.withdraw_batch("bing")
        self.assertFalse(result.changed)
        self.assertIsNone(result.notify_seq)
        self.assertEqual(self.feed.head(), head)
        self.assertEqual(self.rows(), rows)
        self.assertEqual(result.audit.read_batches, ["bing"])
        self.assertEqual(self.feed.batch_status("bing").status, WITHDRAWN)

    def test_consecutive_withdrawals(self):
        self.build_chain()
        # Withdraw 乙 first: d1/d2 go, 丙 becomes effective, mark stays.
        w1 = self.feed.withdraw_batch("yi")
        rows = self.rows()
        self.assertIn("mark", rows)
        self.assertNotIn("d1", rows)
        self.assertIn("review", rows)
        self.assertEqual(
            {s.batch_id: s.status for s in w1.statuses}["bing"], REGISTERED
        )
        # Then withdraw 甲: mark goes too; 丙 already registered stays, since
        # it no longer depends on 甲's key (it gates on d1, not mark).
        w2 = self.feed.withdraw_batch("jia")
        rows = self.rows()
        self.assertNotIn("mark", rows)
        self.assertIn("review", rows)
        statuses = {s.batch_id: s.status for s in w2.statuses}
        self.assertEqual(statuses["jia"], WITHDRAWN)
        # bing is outside jia's key closure, so it is reported via the journal
        # query rather than the withdrawal's own status list; it stays effective.
        self.assertEqual(self.feed.batch_status("bing").status, REGISTERED)
        self.assertNotIn("bing", statuses)

    def test_withdrawn_delete_reverts_to_earlier_value(self):
        # A overwrites k, B overwrites it again, C deletes it gated on B's
        # value.  Withdrawing B makes C skip, so k reverts to A's value rather
        # than disappearing.
        self.feed.submit_batch(
            BatchRequest("A", (), (RowSpec("k", 1, {"ver": 1}),))
        )
        self.feed.submit_batch(
            BatchRequest("B", (), (RowSpec("k", 1, {"ver": 2}),))
        )
        self.feed.submit_batch(
            BatchRequest(
                "C",
                (Condition.matches("k", 1, {"ver": 2}),),
                (RowSpec.delete_pk("k"),),
            )
        )
        self.assertNotIn("k", self.rows())
        self.feed.withdraw_batch("B")
        self.assertEqual(self.rows()["k"], (1, '{"ver":1}'))

    def test_withdrawn_delete_reverts_to_baseline(self):
        # The key exists in the enable-time baseline; a post-enable overwrite
        # and a gated delete both disappear when the overwrite is withdrawn.
        feed = Feed(os.path.join(self.tmp.name, "baseline.db"))
        try:
            feed.apply([RowSpec("k", 1, {"base": True})])
            feed.enable_batches()
            feed.submit_batch(
                BatchRequest("seed", (), (RowSpec("k", 1, {"v": 1}),))
            )
            feed.submit_batch(
                BatchRequest(
                    "rm",
                    (Condition.matches("k", 1, {"v": 1}),),
                    (RowSpec.delete_pk("k"),),
                )
            )
            feed.withdraw_batch("seed")
            rows = {
                r["pk"]: (r["route"], r["payload"])
                for r in feed.conn.execute(
                    "SELECT pk, route, payload FROM records WHERE pk = 'k'"
                )
            }
            self.assertEqual(rows["k"], (1, '{"base":true}'))
        finally:
            feed.close()

    def test_withdrawn_id_keeps_idempotent_and_conflict_rules(self):
        self.feed.submit_batch(
            BatchRequest("b", (), (RowSpec("k", 1, {"v": 1}),))
        )
        self.feed.withdraw_batch("b")
        # An identical retry is a no-op: stays withdrawn, writes nothing.
        retry = self.feed.submit_batch(
            BatchRequest("b", (), (RowSpec("k", 1, {"v": 1}),))
        )
        self.assertFalse(retry.registered)
        self.assertFalse(retry.applied)
        self.assertEqual(self.feed.batch_status("b").status, WITHDRAWN)
        self.assertNotIn("k", self.rows())
        # Different content under the same id still conflicts and changes
        # nothing.
        with self.assertRaises(ValueError):
            self.feed.submit_batch(
                BatchRequest("b", (), (RowSpec("k", 2, {"v": 2}),))
            )

    def test_effective_batch_can_become_skipped_and_vice_versa(self):
        # b writes k=2 only if k==1; a later c overwrites k unconditionally.
        self.feed.submit_batch(
            BatchRequest("a", (), (RowSpec("k", 1, {"v": 1}),))
        )
        b = self.feed.submit_batch(
            BatchRequest("b", (Condition.matches("k", 1, {"v": 1}),),
                         (RowSpec("k", 2, {"v": 2}),))
        )
        self.assertTrue(b.applied)
        self.assertEqual(self.rows()["k"], (2, '{"v":2}'))
        # Withdrawing a removes the value b needed: b flips to skipped and the
        # table loses k entirely (no later writer).
        w = self.feed.withdraw_batch("a")
        self.assertEqual(
            {s.batch_id: s.status for s in w.statuses}["b"], SKIPPED
        )
        self.assertNotIn("k", self.rows())

    def test_no_visible_difference_publishes_nothing(self):
        # Two batches write the same final value to a key; withdrawing the
        # first leaves the second's identical value in place -> no row diff.
        self.feed.submit_batch(
            BatchRequest("a", (), (RowSpec("k", 1, {"v": 1}),))
        )
        self.feed.submit_batch(
            BatchRequest("b", (), (RowSpec("k", 1, {"v": 1}),))
        )
        head = self.feed.head()
        w = self.feed.withdraw_batch("a")
        self.assertTrue(w.changed)              # statuses changed
        self.assertIsNone(w.notify_seq)         # but subscribers see nothing
        self.assertEqual(self.feed.head(), head)
        self.assertEqual(self.rows()["k"], (1, '{"v":1}'))

    def test_only_affected_batches_are_read(self):
        # Many unrelated batches on disjoint keys, plus a two-batch chain.
        for i in range(200):
            self.feed.submit_batch(
                BatchRequest(f"ind-{i}", (),
                             (RowSpec(f"k{i}", 500 + i, {"i": i}),))
            )
        self.feed.submit_batch(
            BatchRequest("hot-0", (), (RowSpec("hot", 1, {"v": 0}),))
        )
        self.feed.submit_batch(
            BatchRequest("hot-1",
                         (Condition.matches("hot", 1, {"v": 0}),),
                         (RowSpec("hot", 1, {"v": 1}),))
        )
        w = self.feed.withdraw_batch("hot-0")
        self.assertEqual(set(w.audit.read_batches), {"hot-0", "hot-1"})
        self.assertFalse(
            any(b.startswith("ind-") for b in w.audit.read_batches)
        )
        # All unrelated rows survive untouched.
        self.assertEqual(len(self.rows()), 1 + 200)  # base + independent keys

    def test_range_moves_and_start_view_survive_withdrawal(self):
        self.build_chain()
        self.feed.apply(moves=[MoveSpec("s", 0, 50)])
        start_seq = self.feed.subscription("s")["start_seq"]
        acked = self.feed.subscription("s")["acked_seq"]
        self.feed.withdraw_batch("jia")
        sub = self.feed.subscription("s")
        self.assertEqual(sub["range"], [0, 50])
        self.assertEqual(sub["start_seq"], start_seq)
        self.assertEqual(sub["acked_seq"], acked)
        # Start view is still replayed verbatim if not yet acked (here acked).
        self.assertEqual(
            self.feed.start_view("s").rows,
            [{"pk": "base", "route": 1, "payload": '{"k":0}'}],
        )

    def test_condition_only_key_tracks_outside_writers(self):        # An unrelated (unaffected) batch writes a key that 乙 later reads as a
        # precondition.  Withdrawing 甲 must replay that outside writer rather
        # than assume the key stayed at its baseline value.
        self.feed.submit_batch(
            BatchRequest("writer", (), (RowSpec("gate", 9, {"g": 1}),))
        )
        self.feed.submit_batch(
            BatchRequest("jia", (), (RowSpec("mark", 5, {}),))
        )
        yi = self.feed.submit_batch(
            BatchRequest(
                "yi",
                (Condition.matches("mark", 5, {}),
                 Condition.matches("gate", 9, {"g": 1})),
                (RowSpec("out", 6, {}),),
            )
        )
        self.assertTrue(yi.applied)
        w = self.feed.withdraw_batch("jia")
        # 乙 flips to skipped because mark is gone; the writer batch is read
        # as a fixed event and remains registered.
        statuses = {s.batch_id: s.status for s in w.statuses}
        self.assertEqual(statuses["yi"], SKIPPED)
        self.assertIn("writer", w.audit.read_batches)
        self.assertEqual(self.feed.batch_status("writer").status, REGISTERED)
        self.assertEqual(self.rows()["gate"], (9, '{"g":1}'))


if __name__ == "__main__":
    unittest.main()
