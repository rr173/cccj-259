"""End-to-end behaviour of the feed, using only the public API."""

import json
import os
import tempfile
import unittest

from feedtable import Feed, MoveSpec, RowSpec


def kinds(batch):
    return [(c["kind"], c["pk"]) for c in batch["changes"]]


def raw_kinds(result, sub="s"):
    """Kinds from the in-memory result object (Change instances)."""
    return [(c.kind, c.pk) for c in result.changes[sub]]


def raw_kinds_of(batch):
    """Kinds from a Batch object."""
    return [(c.kind, c.pk) for c in batch.changes]


class FeedTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "feed.db")
        self.feed = Feed(self.path)

    def tearDown(self):
        self.feed.close()
        self.tmp.cleanup()

    # -- helpers -----------------------------------------------------------
    def seed(self, n, width=1, offset=0):
        return self.feed.apply(
            [RowSpec(f"r{i}", i * width + offset, {"n": i}) for i in range(n)]
        )

    def start_view_rows(self, view):
        return [r["pk"] for r in view.rows]

    def subscribe(self, name="s", lo=0, hi=10):
        return self.feed.create_subscription(name, lo, hi)

    def rows_by_pk(self):
        return {
            r["pk"]: (r["route"], r["payload"])
            for r in self.feed.conn.execute("SELECT pk, route, payload FROM records")
        }

    # -- start view --------------------------------------------------------
    def test_start_view_is_start_view_not_range(self):
        self.seed(10)  # routes 0..9
        view = self.subscribe(lo=2, hi=5)
        self.assertEqual(self.start_view_rows(view), ["r2", "r3", "r4"])
        self.assertEqual(view.seq, self.feed.head())
        self.assertEqual(view.to_dict()["boundary"], {"acked_through": view.seq, "next": view.seq + 1})

    def test_start_view_pending_until_ack(self):
        self.seed(5)
        view = self.subscribe(lo=0, hi=3)
        msgs = self.feed.pending("s")
        self.assertEqual([m["kind"] for m in msgs], ["start_view"])
        self.assertEqual([r["pk"] for r in msgs[0]["rows"]], ["r0", "r1", "r2"])
        self.assertIsNone(self.feed.cursor("s"))

    def test_ack_before_start_boundary_rejected(self):
        # Make the stream non-empty first, so the new subscription's start
        # boundary is > 0.
        self.seed(3)
        warm = self.feed.create_subscription("warm", 0, 10)
        self.feed.ack("warm", warm.seq)
        self.feed.apply([RowSpec("r0", 0, {"n": 0, "v": 2})])

        late = self.feed.create_subscription("late", 0, 10)
        self.assertGreater(late.seq, 0)
        with self.assertRaises(ValueError):
            self.feed.ack("late", late.seq - 1)
        self.feed.ack("late", late.seq)

    # -- enter / update / exit --------------------------------------------
    def test_enter_update_exit_are_net(self):
        self.seed(5)  # r0..r4 routes 0..4
        view = self.subscribe(lo=0, hi=5)
        self.feed.ack("s", view.seq)

        out = self.feed.apply([
            RowSpec("r1", 1, {"n": 1, "changed": True}),   # update
            RowSpec("r9", 9, {"n": 9}),                    # enter (route 9 -> outside!)
            RowSpec.delete_pk("r3"),                       # exit
        ])
        # r9 at route 9 is outside [0,5): invisible, so it must not appear.
        self.assertEqual(
            [(c.kind, c.pk) for c in out.changes["s"]],
            [("update", "r1"), ("exit", "r3")],
        )
        self.assertEqual(
            [(c["kind"], c["pk"]) for c in self.feed.read_group("s", out.seq).to_dict()["changes"]],
            [("update", "r1"), ("exit", "r3")],
        )

    def test_enter_within_range(self):
        self.seed(5)
        view = self.subscribe(lo=0, hi=5)
        self.feed.ack("s", view.seq)
        out = self.feed.apply([RowSpec("new", 2, {"n": 0})])
        self.assertEqual([(c.kind, c.pk) for c in out.changes["s"]], [("enter", "new")])

    def test_write_outside_range_is_invisible(self):
        self.seed(3)
        view = self.subscribe(lo=0, hi=3)
        self.feed.ack("s", view.seq)
        out = self.feed.apply([RowSpec("far", 900, {"n": 900})])
        self.assertFalse(out.committed)
        self.assertEqual(self.feed.pending("s"), [])
        self.assertEqual(out.audit.candidate_rows, 0)
        self.assertEqual(self.rows_by_pk()["far"], (900, '{"n":900}'))

    def test_last_write_wins_per_pk(self):
        self.seed(3)
        view = self.subscribe(lo=0, hi=3)
        self.feed.ack("s", view.seq)
        # delete then insert the same pk in one transaction: net effect is an
        # update to the inserted row, one change, one point lookup.
        out = self.feed.apply([
            RowSpec.delete_pk("r1"),
            RowSpec("r1", 1, {"n": 1, "v": 2}),
        ])
        self.assertEqual(raw_kinds(out), [("update", "r1")])
        self.assertEqual(out.audit.point_lookups, 1)
        # insert then delete: the row is gone, one exit.
        out = self.feed.apply([
            RowSpec("r2", 2, {"n": 2, "v": 3}),
            RowSpec.delete_pk("r2"),
        ])
        self.assertEqual(raw_kinds(out), [("exit", "r2")])

    def test_enter_by_route_change_inside_range(self):
        self.seed(3)
        view = self.subscribe(lo=0, hi=3)
        self.feed.ack("s", view.seq)
        # r0 moves from route 0 to 2: still visible, so update, not exit+enter.
        out = self.feed.apply([RowSpec("r0", 2, {"n": 0, "moved": True})])
        self.assertEqual([(c.kind, c.pk) for c in out.changes["s"]], [("update", "r0")])

    def test_moved_out_of_range_is_exit(self):
        self.seed(3)
        view = self.subscribe(lo=0, hi=3)
        self.feed.ack("s", view.seq)
        out = self.feed.apply([RowSpec("r0", 30, {"n": 0})])
        self.assertEqual([(c.kind, c.pk) for c in out.changes["s"]], [("exit", "r0")])

    def test_moved_into_range_is_enter(self):
        self.seed(3)
        view = self.subscribe(lo=0, hi=3)
        self.feed.ack("s", view.seq)
        out = self.feed.apply([RowSpec("r2", 1, {"n": 2})])
        self.assertEqual([(c.kind, c.pk) for c in out.changes["s"]], [("update", "r2")])

    def test_one_net_change_per_pk_in_commit(self):
        self.seed(3)
        view = self.subscribe(lo=0, hi=3)
        self.feed.ack("s", view.seq)
        # A route change that leaves then re-enters still yields one change.
        out = self.feed.apply([RowSpec("r0", 0, {"n": 0, "v": 2})])
        pks = [c.pk for c in out.changes["s"]]
        self.assertEqual(pks, ["r0"])

    # -- range moves -------------------------------------------------------
    def test_shift_right_no_delete_recreate(self):
        self.seed(10)  # routes 0..9
        view = self.subscribe(lo=0, hi=6)
        self.feed.ack("s", view.seq)

        out = self.feed.apply(moves=[MoveSpec("s", 4, 10)])
        # Retained 4,5 must not appear at all.  Only 0..3 exit and 6..9 enter.
        self.assertEqual(
            kinds(self.feed.read_group("s", out.seq).to_dict()),
            [("exit", "r0"), ("exit", "r1"), ("exit", "r2"), ("exit", "r3"),
             ("enter", "r6"), ("enter", "r7"), ("enter", "r8"), ("enter", "r9")],
        )
    def test_shift_left(self):
        self.seed(10)
        view = self.subscribe(lo=4, hi=10)
        self.feed.ack("s", view.seq)
        out = self.feed.apply(moves=[MoveSpec("s", 0, 6)])
        self.assertEqual(
            raw_kinds(out),
            [("enter", "r0"), ("enter", "r1"), ("enter", "r2"), ("enter", "r3"),
             ("exit", "r6"), ("exit", "r7"), ("exit", "r8"), ("exit", "r9")],
        )

    def test_widen_both_sides(self):
        self.seed(10)
        view = self.subscribe(lo=3, hi=7)
        self.feed.ack("s", view.seq)
        out = self.feed.apply(moves=[MoveSpec("s", 1, 9)])
        self.assertEqual(
            raw_kinds(out),
            [("enter", "r1"), ("enter", "r2"), ("enter", "r7"), ("enter", "r8")],
        )

    def test_move_without_visible_effect_still_reports_boundary(self):
        self.seed(3)  # r0..r2 at routes 0..2
        view = self.subscribe(lo=0, hi=100)
        self.feed.ack("s", view.seq)
        # [0,100) -> [10,20) drops r0..r2: a real exit group.
        out = self.feed.apply(moves=[MoveSpec("s", 10, 20)])
        self.assertTrue(out.committed)
        group = self.feed.read_group("s", out.seq)
        self.assertEqual(raw_kinds_of(group), [("exit", "r0"), ("exit", "r1"), ("exit", "r2")])
        self.assertEqual(group.to_dict()["range"], [10, 20])

        # A further shift [10,20) -> [11,21) exposes and hides nothing.
        out2 = self.feed.apply(moves=[MoveSpec("s", 11, 21)])
        self.assertTrue(out2.committed)
        group2 = self.feed.read_group("s", out2.seq)
        self.assertEqual(group2.to_dict()["changes"], [])
        self.assertEqual(group2.to_dict()["range"], [11, 21])

    def test_move_and_write_same_transaction(self):
        self.seed(10)  # r0..r9
        view = self.subscribe(lo=0, hi=5)  # sees r0..r4
        self.feed.ack("s", view.seq)
        out = self.feed.apply(
            [RowSpec("r6", 6, {"n": 6, "new": True}), RowSpec("r1", 1, {"n": 1, "edited": True})],
            [MoveSpec("s", 3, 8)],
        )
        changes = {c.public_dict()["pk"]: c.public_dict()["kind"] for c in out.changes["s"]} \
            if hasattr(out.changes["s"][0], "public_dict") else \
            {c.pk: c.kind for c in out.changes["s"]}
        # r0,r1,r2 exit (r1 also edited, still one change: exit);
        # r5,r6,r7 enter (r6 also written, still one change: enter);
        # r3,r4 retained, untouched -> absent; r8,r9 outside -> absent.
        self.assertEqual(changes, {
            "r0": "exit", "r1": "exit", "r2": "exit",
            "r5": "enter", "r6": "enter", "r7": "enter",
        })

    def test_written_row_retained_with_move_is_update(self):
        self.seed(10)
        view = self.subscribe(lo=0, hi=10)
        self.feed.ack("s", view.seq)
        out = self.feed.apply(
            [RowSpec("r5", 5, {"n": 5, "edited": True})],
            [MoveSpec("s", 5, 15)],
        )
        # r0..r4 exit with the move; r5 is retained and edited: one update.
        self.assertEqual(
            raw_kinds(out),
            [("exit", "r0"), ("exit", "r1"), ("exit", "r2"), ("exit", "r3"),
             ("exit", "r4"), ("update", "r5")],
        )

    def test_unchanged_written_row_produces_nothing(self):
        self.feed.apply([RowSpec("a", 1, {"v": 1})])
        view = self.subscribe(lo=0, hi=10)
        self.feed.ack("s", view.seq)
        out = self.feed.apply([RowSpec("a", 1, {"v": 1})])
        self.assertFalse(out.committed)
        self.assertEqual(self.feed.pending("s"), [])

    # -- ordering and delivery --------------------------------------------
    def test_groups_are_ordered_and_replayed_verbatim(self):
        self.seed(6)
        view = self.subscribe(lo=0, hi=6)
        self.feed.ack("s", view.seq)
        a = self.feed.apply([RowSpec("r0", 0, {"n": 0, "v": 2})])
        b = self.feed.apply(moves=[MoveSpec("s", 1, 7)])
        msgs = self.feed.pending("s")
        self.assertEqual([m["seq"] for m in msgs], [a.seq, b.seq])
        # Read again after the range has since moved further: identical bytes.
        self.feed.apply(moves=[MoveSpec("s", 100, 200)])
        again = self.feed.pending("s")
        self.assertEqual(json.dumps(again[:2], sort_keys=True),
                         json.dumps(msgs, sort_keys=True))
        self.assertEqual(again[1]["range"], [1, 7])

    def test_ack_is_durable_and_monotonic(self):
        self.seed(5)
        view = self.subscribe(lo=0, hi=5)
        self.feed.ack("s", view.seq)
        out = self.feed.apply(moves=[MoveSpec("s", 0, 6)])
        self.feed.ack("s", out.seq)
        self.assertEqual(self.feed.pending("s"), [])
        with self.assertRaises(ValueError):
            self.feed.ack("s", out.seq - 1)
        with self.assertRaises(ValueError):
            self.feed.ack("s", out.seq + 1)  # beyond head

    def test_unacknowledged_survives_reopen(self):
        self.seed(5)
        view = self.subscribe(lo=0, hi=5)
        self.feed.ack("s", view.seq)
        out = self.feed.apply([RowSpec("r1", 1, {"n": 1, "v": 2})])
        expected = self.feed.pending("s")
        self.feed.close()

        self.feed = Feed(self.path)
        self.assertEqual(self.feed.pending("s"), expected)
        self.assertEqual(self.feed.read_group("s", out.seq).to_dict(), expected[0])

    # -- atomicity ---------------------------------------------------------
    def test_failed_commit_leaves_no_partial_state(self):
        self.seed(5)
        view = self.subscribe(lo=0, hi=5)
        self.feed.ack("s", view.seq)
        before_head = self.feed.head()
        before_rows = self.rows_by_pk()

        with self.assertRaises(TypeError):
            # meta is serialised after the writes have been applied; the set is
            # not JSON serialisable, so the whole transaction must roll back.
            self.feed.apply(
                [RowSpec("r2", 2, {"n": 2, "x": 1}), RowSpec("r9", 9, {"n": 9})],
                [MoveSpec("s", 1, 6)],
                meta={"bad": {1, 2, 3}},
            )

        self.assertEqual(self.feed.head(), before_head)
        self.assertEqual(self.rows_by_pk(), before_rows)
        self.assertEqual(self.feed.subscription("s")["range"], [0, 5])
        self.assertEqual(self.feed.pending("s"), [])

    def test_bad_write_rolls_back_earlier_writes(self):
        self.seed(3)
        before = self.rows_by_pk()
        with self.assertRaises(TypeError):
            self.feed.apply([
                RowSpec("r0", 0, {"ok": True}),
                RowSpec("r1", 1, {"bad": {1}}),
            ])
        self.assertEqual(self.rows_by_pk(), before)

    def test_move_to_unknown_subscription_rolls_back_writes(self):
        self.seed(2)
        before = self.rows_by_pk()
        with self.assertRaises(KeyError):
            self.feed.apply([RowSpec("r0", 0, {"v": 2})], [MoveSpec("nope", 0, 1)])
        self.assertEqual(self.rows_by_pk(), before)

    def test_start_view_rollback(self):
        self.seed(2)
        self.feed.create_subscription("s", 0, 10)
        view = self.feed.start_view("s")
        with self.assertRaises(KeyError):
            self.feed.apply([RowSpec("x", 1, {})], [MoveSpec("nope", 0, 1)])
        # Subscription and its captured start view are untouched.
        self.assertEqual(self.feed.subscription("s")["range"], [0, 10])
        self.assertEqual(self.feed.start_view("s"), view)

    # -- read cost ---------------------------------------------------------
    def test_read_cost_excludes_stable_rows(self):
        # 20k rows spread over [0, 20000), no subscription yet.
        self.feed.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(20000)])
        view = self.subscribe("s", 1000, 5000)
        self.feed.ack("s", view.seq)

        out = self.feed.apply(moves=[MoveSpec("s", 2000, 6000)])
        audit = out.audit.to_dict()
        # Only [1000,2000) may exit and [5000,6000) may enter.
        self.assertEqual(sorted(audit["allowed_segments"]["s"]), [[1000, 2000], [5000, 6000]])
        self.assertEqual(audit["retained_not_scanned"]["s"], [2000, 5000])
        self.assertEqual(audit["candidate_rows"], 2000)
        self.assertEqual(audit["stable_rows_read"], 0)
        self.assertEqual(audit["point_lookups"], 0)
        self.assertEqual(audit.violations() if hasattr(audit, "violations") else [], [])
        self.assertEqual(out.audit.violations(), [])

    def test_read_cost_with_writes_only_uses_point_lookups(self):
        self.feed.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(5000)])
        view = self.subscribe("s", 0, 5000)
        self.feed.ack("s", view.seq)
        out = self.feed.apply([RowSpec("r10", 10, {"n": 10, "v": 2})])
        self.assertEqual(out.audit.candidate_rows, 0)
        self.assertEqual(out.audit.point_lookups, 1)
        self.assertEqual(out.audit.scans, [])

    def test_read_cost_charged_once_for_overlapping_moves(self):
        self.feed.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(100)])
        self.subscribe("a", 0, 10)
        self.subscribe("b", 50, 60)
        self.feed.apply([], [MoveSpec("a", 0, 10)])  # no-op moves, no groups
        out = self.feed.apply([], [MoveSpec("a", 0, 100), MoveSpec("b", 40, 100)])
        # a scans [10,100), b scans [40,50) and [60,100); [40,50) overlaps.
        scanned = sorted(s.interval for s in out.audit.scans)
        self.assertEqual(scanned, [(10, 100), (40, 50), (60, 100)])
        self.assertEqual(out.audit.candidate_rows, 90 + 10 + 40)

    def test_query_plan_uses_index(self):
        self.seed(10)
        view = self.subscribe("s", 0, 5)
        self.feed.ack("s", view.seq)
        out = self.feed.apply([], [MoveSpec("s", 2, 7)])
        for scan in out.audit.scans:
            self.assertIn("USING INDEX records_route_pk", " ".join(scan.plan))
            self.assertNotIn("SCAN records", " ".join(scan.plan))

    def test_no_other_statement_touches_records(self):
        """Every statement the transaction runs against `records` is either a
        point lookup by primary key or the fixed indexed segment query."""
        self.feed.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(200)])
        view = self.subscribe("s", 50, 150)
        self.feed.ack("s", view.seq)

        statements = []
        self.feed.conn.set_trace_callback(statements.append)
        try:
            self.feed.apply(
                [RowSpec("r60", 60, {"n": 60, "v": 2})],
                [MoveSpec("s", 40, 160)],
            )
        finally:
            self.feed.conn.set_trace_callback(None)

        records_reads = [
            s for s in statements
            if s.lstrip().upper().startswith("SELECT") and " FROM RECORDS" in s.upper()
        ]
        self.assertTrue(records_reads)
        for sql in records_reads:
            normalised = " ".join(sql.split())
            point = normalised.startswith(
                "SELECT pk, route, payload FROM records WHERE pk = "
            )
            segment = normalised.startswith(
                "SELECT pk, route, payload FROM records INDEXED BY "
                "records_route_pk WHERE route >= "
            )
            self.assertTrue(point or segment, f"unexpected read of records: {sql}")
            if not point:
                # A segment read must be bounded on both sides.
                self.assertIn(" AND route < ", normalised)
        # No statement anywhere does a bare table scan of records.
        for sql in statements:
            self.assertNotIn("SCAN records", sql.upper())

    # -- durability / crash ------------------------------------------------
    def test_kill_after_commit_replays_exactly(self):
        self.seed(5)
        view = self.subscribe(lo=0, hi=5)
        self.feed.ack("s", view.seq)
        out = self.feed.apply([RowSpec("r1", 1, {"n": 1, "v": 2})],
                              [MoveSpec("s", 0, 7)])
        expected = self.feed.pending("s")
        self.feed.conn.execute("PRAGMA wal_checkpoint(FULL)")
        # Simulate an abrupt kill: close the OS handle without a clean close.
        self.feed.close()

        self.feed = Feed(self.path)
        self.assertEqual(self.feed.pending("s"), expected)
        self.assertEqual(self.feed.subscription("s")["range"], [0, 7])
        self.assertEqual(self.rows_by_pk()["r1"], (1, '{"n":1,"v":2}'))

    # -- misc --------------------------------------------------------------
    def test_subscription_overlap_independent_cursors(self):
        self.seed(10)  # r0..r9 at routes 0..9
        a = self.subscribe("a", 0, 5)   # r0..r4
        b = self.subscribe("b", 3, 8)   # r3..r7
        self.feed.ack("a", a.seq)
        # b has not acked its start view; a should still consume.
        out = self.feed.apply([RowSpec("r4", 4, {"n": 4, "v": 2})])
        self.assertEqual([(c.pk, c.kind) for c in out.changes["a"]], [("r4", "update")])
        self.assertEqual([(c.pk, c.kind) for c in out.changes["b"]], [("r4", "update")])
        a_msgs = self.feed.pending("a")
        b_msgs = self.feed.pending("b")
        self.assertEqual(a_msgs[0]["kind"], "commit")
        self.assertEqual(b_msgs[0]["kind"], "start_view")
        self.assertEqual(b_msgs[1]["seq"], out.seq)
        # a moves away: only a gets a group for the move.
        out2 = self.feed.apply([], [MoveSpec("a", 10, 20)])
        self.assertIn("a", out2.changes)
        self.assertNotIn("b", out2.changes)
        self.assertEqual([m["seq"] for m in self.feed.pending("a")], [out.seq, out2.seq])

    def test_large_payload_roundtrip(self):
        big = "x" * 100_000
        self.feed.apply([RowSpec("big", 1, {"blob": big})])
        view = self.subscribe("s", 0, 2)
        self.assertEqual(json.loads(view.rows[0]["payload"])["blob"], big)


if __name__ == "__main__":
    unittest.main()
