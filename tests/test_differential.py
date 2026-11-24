"""Randomised differential test.

A trivial in-memory reference implementation recomputes the before/after
visible sets by full scan on every transaction.  The real feed must produce
exactly the same net change list, and a consumer that only ever applies the
delivered start view and commit groups must end up holding exactly the rows
visible in the subscription's current range.
"""

import json
import os
import random
import tempfile
import unittest

from feedtable import Feed, MoveSpec, RowSpec


class Reference:
    """O(n) oracle: keeps full snapshots and diffs them."""

    def __init__(self):
        self.records = {}      # pk -> (route, payload)
        self.subs = {}         # name -> [lo, hi]

    def snapshot(self):
        return dict(self.records)

    @staticmethod
    def vis(snapshot, rng):
        lo, hi = rng
        return {pk: row for pk, row in snapshot.items() if lo <= row[0] < hi}

    @staticmethod
    def diff(before, after):
        changes = []
        for pk in sorted(set(before) | set(after)):
            b, a = before.get(pk), after.get(pk)
            if b is None:
                changes.append(("enter", pk, a))
            elif a is None:
                changes.append(("exit", pk, b))
            elif b != a:
                changes.append(("update", pk, a))
        return changes


def canonical(payload):
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


class Consumer:
    """Only ever uses what the feed delivers."""

    def __init__(self):
        self.held = {}
        self.last_seq = None

    def consume(self, messages):
        for msg in messages:
            if msg["kind"] == "start_view":
                for row in msg["rows"]:
                    assert row["pk"] not in self.held, f"duplicate start row {row['pk']}"
                    self.held[row["pk"]] = (row["route"], row["payload"])
                self.last_seq = msg["seq"]
                continue
            for ch in msg["changes"]:
                pk = ch["pk"]
                if ch["kind"] == "enter":
                    assert pk not in self.held, f"enter for held {pk}"
                    self.held[pk] = (ch["route"], ch["payload"])
                elif ch["kind"] == "update":
                    assert pk in self.held, f"update for unheld {pk}"
                    self.held[pk] = (ch["route"], ch["payload"])
                else:
                    assert pk in self.held, f"exit for unheld {pk}"
                    del self.held[pk]
            self.last_seq = msg["seq"]


class DifferentialTest(unittest.TestCase):
    def run_case(self, seed, steps=120, routes=25, subs=3, move_prob=0.5):
        rng = random.Random(seed)
        tmp = tempfile.TemporaryDirectory()
        path = os.path.join(tmp.name, "f.db")
        feed = Feed(path)
        ref = Reference()
        consumers = {}
        try:
            for s in range(subs):
                lo = rng.randrange(routes)
                hi = rng.randrange(lo, routes + 1)
                feed.create_subscription(f"s{s}", lo, hi)
                ref.subs[f"s{s}"] = [lo, hi]
            names = sorted(ref.subs)

            counter = 0
            for step in range(steps):
                before = ref.snapshot()
                old_ranges = {n: list(v) for n, v in ref.subs.items()}

                writes = []
                for _ in range(rng.randrange(0, 4)):
                    if ref.records and rng.random() < 0.25:
                        pk = rng.choice(list(ref.records))
                        writes.append(RowSpec.delete_pk(pk))
                        ref.records.pop(pk, None)
                    else:
                        pk = f"p{rng.randrange(40)}"
                        route = rng.randrange(routes)
                        payload = {"v": rng.randrange(5), "n": counter}
                        counter += 1
                        writes.append(RowSpec(pk, route, payload))
                        ref.records[pk] = (route, canonical(payload))

                moves = []
                for name in names:
                    if rng.random() < move_prob:
                        lo = rng.randrange(routes)
                        hi = rng.randrange(lo, routes + 1)
                        moves.append(MoveSpec(name, lo, hi))
                        ref.subs[name] = [lo, hi]

                result = feed.apply(writes, moves)
                after = ref.snapshot()

                # 1. Net changes must match the oracle exactly.
                expected_any = False
                range_changed = False
                for name in names:
                    want = Reference.diff(
                        Reference.vis(before, old_ranges[name]),
                        Reference.vis(after, ref.subs[name]),
                    )
                    if old_ranges[name] != ref.subs[name]:
                        range_changed = True
                    got = [(c.kind, c.pk) for c in result.changes.get(name, [])]
                    self.assertEqual(
                        got, [(k, pk) for k, pk, _ in want],
                        f"seed {seed} step {step} sub {name}",
                    )
                    if want:
                        expected_any = True
                    self.assertEqual(len(got), len({pk for _, pk in got}))

                # A transaction is committed iff it changes anything a
                # subscription observes, or it moves a range (empty group
                # carrying the new boundary).
                self.assertEqual(
                    result.committed, expected_any or range_changed,
                    f"seed {seed} step {step} committed flag",
                )

                # 2. Read cost: no scan may touch a retained/irrelevant region.
                self.assertEqual(result.audit.violations(), [],
                                 f"seed {seed} step {step} audit")

                # 3. Consumers fed only from the feed converge on the truth.
                for name in names:
                    consumer = consumers.setdefault(name, Consumer())
                    consumer.consume(feed.pending(name))
                    for msg in feed.pending(name):
                        feed.ack(name, msg["seq"])
                    self.assertEqual(feed.pending(name), [])
                    want = {
                        pk: (row[0], row[1])
                        for pk, row in Reference.vis(after, ref.subs[name]).items()
                    }
                    self.assertEqual(consumer.held, want,
                                     f"seed {seed} step {step} consumer {name}")
        finally:
            feed.close()
            tmp.cleanup()

    def test_random_small(self):
        for seed in range(12):
            with self.subTest(seed=seed):
                self.run_case(seed, steps=60, routes=15, subs=2)

    def test_random_wider(self):
        for seed in range(4):
            with self.subTest(seed=seed):
                self.run_case(1000 + seed, steps=150, routes=40, subs=4)


if __name__ == "__main__":
    unittest.main()
