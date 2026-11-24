"""Behaviour specific to the operator workflow: reslicing before the start
view was consumed, several consumers, and concurrent committers."""

import os
import tempfile
import threading
import unittest

from feedtable import Feed, MoveSpec, RowSpec


class ResliceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "f.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_reslice_before_consuming_start_view(self):
        feed = Feed(self.path)
        try:
            feed.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(10)])
            view = feed.create_subscription("s", 0, 5)   # r0..r4
            out = feed.apply([], [MoveSpec("s", 3, 8)])  # r3,r4 retained; r5..r7 enter

            messages = feed.pending("s")
            self.assertEqual([m["kind"] for m in messages], ["start_view", "commit"])
            # The start view is the range as it was when the subscription was
            # created - not the new range.
            self.assertEqual([r["pk"] for r in messages[0]["rows"]],
                             ["r0", "r1", "r2", "r3", "r4"])
            self.assertEqual(messages[0]["range"], [0, 5])
            # The move is expressed relative to that start view.
            self.assertEqual([(c["kind"], c["pk"]) for c in messages[1]["changes"]],
                             [("exit", "r0"), ("exit", "r1"), ("exit", "r2"),
                              ("enter", "r5"), ("enter", "r6"), ("enter", "r7")])
            self.assertEqual(messages[1]["range"], [3, 8])

            # A consumer that applies both ends up with exactly [3,8).
            held = {r["pk"] for r in messages[0]["rows"]}
            for ch in messages[1]["changes"]:
                if ch["kind"] == "enter":
                    held.add(ch["pk"])
                else:
                    held.remove(ch["pk"])
            self.assertEqual(held, {"r3", "r4", "r5", "r6", "r7"})

            feed.ack("s", view.seq)
            self.assertEqual([m["kind"] for m in feed.pending("s")], ["commit"])
            feed.ack("s", out.seq)
            self.assertEqual(feed.pending("s"), [])
        finally:
            feed.close()

    def test_two_consumers_of_one_subscription_are_independent(self):
        feed = Feed(self.path)
        try:
            feed.apply([RowSpec("a", 1, {"v": 1})])
            view = feed.create_subscription("s", 0, 10)
            feed.ack("s", view.seq)
            out = feed.apply([RowSpec("a", 1, {"v": 2})])
            self.assertEqual([m["seq"] for m in feed.pending("s")], [out.seq])
            feed.ack("s", out.seq)
            self.assertEqual(feed.pending("s"), [])
            # The stored group is still readable by seq after acknowledgement.
            self.assertEqual(
                [(c.kind, c.pk) for c in feed.read_group("s", out.seq).changes],
                [("update", "a")],
            )
        finally:
            feed.close()

    def test_many_subscriptions_see_only_their_own_groups(self):
        feed = Feed(self.path)
        try:
            feed.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(20)])
            names = [f"s{i}" for i in range(5)]
            for i, name in enumerate(names):
                v = feed.create_subscription(name, i * 4, i * 4 + 4)
                feed.ack(name, v.seq)
            out = feed.apply([RowSpec("r0", 0, {"n": 0, "v": 2})])
            self.assertEqual(list(out.changes), ["s0"])
            for name in names:
                groups = [m for m in feed.pending(name) if m["kind"] == "commit"]
                if name == "s0":
                    self.assertEqual([g["seq"] for g in groups], [out.seq])
                else:
                    self.assertEqual(groups, [])
        finally:
            feed.close()


class ConcurrencyTest(unittest.TestCase):
    """Several writers, one database: sequence numbers must stay dense and
    no group may be torn."""

    def test_parallel_committers(self):
        tmp = tempfile.TemporaryDirectory()
        path = os.path.join(tmp.name, "f.db")
        try:
            setup = Feed(path)
            setup.apply([RowSpec(f"r{i}", i, {"n": i}) for i in range(64)])
            view = setup.create_subscription("s", 0, 64)
            setup.ack("s", view.seq)
            # The consumer acknowledges the start view, so it starts from it.
            initial = {r["pk"]: r["payload"] for r in setup.start_view("s").rows}
            setup.close()

            errors = []
            barrier = threading.Barrier(4)

            def worker(k):
                try:
                    feed = Feed(path)
                    barrier.wait()
                    for i in range(10):
                        pk = f"r{(k * 10 + i) % 64}"
                        feed.apply([RowSpec(pk, (k * 10 + i) % 64, {"w": k, "i": i})])
                    feed.close()
                except BaseException as exc:  # pragma: no cover - reported below
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(k,)) for k in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(errors, [])
            feed = Feed(path)
            try:
                head = feed.head()
                groups = [m for m in feed.pending("s") if m["kind"] == "commit"]
                seqs = [g["seq"] for g in groups]
                self.assertEqual(seqs, list(range(1, head + 1)))
                # Replaying everything yields the exact current visible state.
                held = dict(initial)
                for g in groups:
                    for ch in g["changes"]:
                        if ch["kind"] == "enter":
                            self.assertNotIn(ch["pk"], held)
                            held[ch["pk"]] = ch["payload"]
                        elif ch["kind"] == "update":
                            self.assertIn(ch["pk"], held)
                            held[ch["pk"]] = ch["payload"]
                        else:
                            self.assertIn(ch["pk"], held)
                            del held[ch["pk"]]
                truth = {
                    r["pk"]: r["payload"]
                    for r in feed.conn.execute(
                        "SELECT pk, payload FROM records WHERE route >= 0 AND route < 64"
                    )
                }
                self.assertEqual(held, truth)
            finally:
                feed.close()
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
