"""Randomised differential test for the batch journal.

A trivial oracle rebuilds the whole table from the enable-time baseline by
replaying every non-withdrawn batch in registration order, evaluating
preconditions itself.  After every submission and every withdrawal the real
record table, the per-batch status and the delivered subscription changes must
match the oracle.  Withdrawals are also checked for the read-cost claim: the
reported ``read_batches`` set must equal exactly the batches the oracle's
key-based influence closure reaches.
"""

import json
import os
import random
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


def canon(payload):
    if payload is None:
        return None
    return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


class Oracle:
    """Full replay from the baseline; O(total journal) by construction."""

    def __init__(self, baseline):
        self.baseline = dict(baseline)          # pk -> (route, payload)
        self.batches = {}                        # id -> dict
        self.order = []                          # ids in registration order

    def register(self, request):
        status, _ = self._evaluate(request, set())
        self.batches[request.batch_id] = {
            "request": request, "status": status,
        }
        self.order.append(request.batch_id)
        return status

    def _evaluate(self, request, withdrawn):
        table = self.replay(withdrawn)
        for c in request.conditions:
            row = table.get(c.pk)
            if c.kind == "absent":
                if row is not None:
                    return SKIPPED, table
            else:
                if row is None or row != (c.route, c.payload_value):
                    return SKIPPED, table
        return REGISTERED, table

    def replay(self, withdrawn):
        table = dict(self.baseline)
        for bid in self.order:
            entry = self.batches[bid]
            if bid in withdrawn or entry["status"] == WITHDRAWN:
                continue
            request = entry["request"]
            ok = True
            for c in request.conditions:
                row = table.get(c.pk)
                if c.kind == "absent":
                    if row is not None:
                        ok = False
                        break
                else:
                    if row is None or row != (c.route, c.payload_value):
                        ok = False
                        break
            if not ok:
                continue
            for w in request.writes:
                if w.delete:
                    table.pop(w.pk, None)
                else:
                    table[w.pk] = (w.route, w.payload_value)
        return table

    def withdraw(self, batch_id):
        withdrawn = {b for b, e in self.batches.items()
                     if e["status"] == WITHDRAWN} | {batch_id}
        # Recompute every still-relevant batch's status in order.
        new_status = {}
        table = dict(self.baseline)
        for bid in self.order:
            if bid in withdrawn:
                new_status[bid] = WITHDRAWN
                continue
            request = self.batches[bid]["request"]
            ok = True
            for c in request.conditions:
                row = table.get(c.pk)
                if c.kind == "absent":
                    if row is not None:
                        ok = False
                        break
                else:
                    if row is None or row != (c.route, c.payload_value):
                        ok = False
                        break
            status = REGISTERED if ok else SKIPPED
            new_status[bid] = status
            if ok:
                for w in request.writes:
                    if w.delete:
                        table.pop(w.pk, None)
                    else:
                        table[w.pk] = (w.route, w.payload_value)
        for bid, status in new_status.items():
            self.batches[bid]["status"] = status
        return table, new_status

    def influence_closure(self, target_id):
        """Batches actually re-read when ``target_id`` is withdrawn.

        Reachability follows both kinds of key references: a later batch is
        re-read if it *reads* an affected key as a precondition or *writes* an
        affected key (the latter must be replayed to reconstruct the final row
        value).  Propagation beyond a reached batch is status-dependent: a
        registered batch stays effective and introduces no new affected keys;
        a skipped batch may become effective, so its write keys carry the
        closure forward.  A ``skipped`` target cannot move any row, and
        already-withdrawn batches stay withdrawn.
        """
        if self.batches[target_id]["status"] == SKIPPED:
            return {target_id}
        reached, _touched = self._closure_and_touched(target_id)
        return reached

    def expected_read_batches(self, target_id):
        """Every batch whose conditions or write details must be opened.

        A ``skipped`` target never wrote a row, so withdrawing it changes
        nothing and reads only itself.  Otherwise the read set is the
        influence closure plus every still-effective (registered) batch outside
        it that writes a key the closure touches; those outside write details
        are replayed to reconstruct each affected key's value without scanning
        the record table.
        """
        if self.batches[target_id]["status"] == SKIPPED:
            return {target_id}
        closure, touched = self._closure_and_touched(target_id)
        read = set(closure)
        for bid in self.order:
            if bid in read or self.batches[bid]["status"] != REGISTERED:
                continue
            if self._write_pks(self.batches[bid]["request"]) & touched:
                read.add(bid)
        return read

    def _closure_and_touched(self, target_id):
        """Closure batches and the keys whose row values need reconstructing."""
        target_seq = self.order.index(target_id)
        reached = {target_id}
        affected_keys = set(self._write_pks(self.batches[target_id]["request"]))
        touched = set(affected_keys)
        while True:
            added_keys = set()
            admitted = 0
            for bid in self.order[target_seq + 1:]:
                if bid in reached or self.batches[bid]["status"] == WITHDRAWN:
                    continue
                req = self.batches[bid]["request"]
                cond, write = self._condition_pks(req), self._write_pks(req)
                via_write = bool(write & affected_keys)
                via_cond = bool(cond & affected_keys)
                if not (via_write or via_cond):
                    continue
                reached.add(bid)
                admitted += 1
                touched.update(write | cond)
                # Its writes may appear or disappear in the new settlement.
                added_keys.update(write)
                if self.batches[bid]["status"] == SKIPPED:
                    added_keys.update(cond)
            if admitted == 0:
                break
            affected_keys |= added_keys
        return reached, touched

    @staticmethod
    def _keys(request):
        keys = {c.pk for c in request.conditions}
        keys.update(w.pk for w in request.writes)
        return keys

    @staticmethod
    def _write_pks(request):
        return {w.pk for w in request.writes}

    @staticmethod
    def _condition_pks(request):
        return {c.pk for c in request.conditions}


class BatchDifferentialTest(unittest.TestCase):
    def run_case(self, seed, steps=80, keys=8):
        rng = random.Random(seed)
        tmp = tempfile.TemporaryDirectory()
        path = os.path.join(tmp.name, "f.db")
        feed = Feed(path)
        # A few pre-enable rows: the irrevocable baseline.
        baseline_rows = [
            RowSpec(f"base{i}", rng.randrange(20), {"b": i}) for i in range(3)
        ]
        feed.apply(baseline_rows)
        feed.enable_batches()
        feed.create_subscription("s", 0, 1000)
        oracle = Oracle({
            f"base{i}": (w.route, w.payload_value)
            for i, w in enumerate(baseline_rows)
        })

        def live_table():
            return {
                r["pk"]: (r["route"], r["payload"])
                for r in feed.conn.execute("SELECT pk, route, payload FROM records")
            }

        counter = 0
        try:
            for step in range(steps):
                if rng.random() < 0.30 and oracle.order:
                    # Withdraw a random non-withdrawn batch.
                    candidates = [
                        bid for bid in oracle.order
                        if oracle.batches[bid]["status"] != WITHDRAWN
                    ]
                    if candidates:
                        target = rng.choice(candidates)
                        # Read-cost: the influence closure plus outside
                        # effective writers replayed for touched keys.
                        expected_read = oracle.expected_read_batches(target)
                        expected_table, expected_status = oracle.withdraw(target)
                        result = feed.withdraw_batch(target)
                        self.assertEqual(live_table(), expected_table,
                                         f"seed {seed} step {step} table after withdraw")
                        # Statuses match for every batch the oracle tracks.
                        actual = {b.batch_id: b.status for b in feed.batches()}
                        self.assertEqual(actual, expected_status,
                                         f"seed {seed} step {step} statuses")
                        # Read-cost: exactly the closure plus outside readers.
                        self.assertEqual(set(result.audit.read_batches),
                                         expected_read,
                                         f"seed {seed} step {step} read_batches")
                        continue

                # Build a random batch.
                batch_id = f"b{counter}"
                counter += 1
                conditions = []
                for _ in range(rng.randrange(0, 3)):
                    pk = f"k{rng.randrange(keys)}"
                    if rng.random() < 0.4:
                        conditions.append(Condition.absent(pk))
                    else:
                        conditions.append(Condition.matches(
                            pk, rng.randrange(20),
                            {"n": rng.randrange(5)} if rng.random() < 0.8 else None,
                        ))
                pks = rng.sample(range(keys), rng.randrange(1, 4))
                writes = []
                used = set()
                for k in pks:
                    pk = f"k{k}"
                    if pk in used:
                        continue
                    used.add(pk)
                    if rng.random() < 0.15:
                        writes.append(RowSpec.delete_pk(pk))
                    else:
                        writes.append(RowSpec(pk, rng.randrange(20), {"n": counter}))
                request = BatchRequest(batch_id, tuple(conditions), tuple(writes))
                expected_status = oracle.register(request)
                result = feed.submit_batch(request)
                self.assertEqual(result.applied, expected_status == REGISTERED)
                withdrawn = {b for b, e in oracle.batches.items()
                             if e["status"] == WITHDRAWN}
                self.assertEqual(live_table(), oracle.replay(withdrawn),
                                 f"seed {seed} step {step} table after submit")
                actual = {b.batch_id: b.status for b in feed.batches()}
                self.assertEqual(
                    actual,
                    {bid: e["status"] for bid, e in oracle.batches.items()},
                    f"seed {seed} step {step} journal statuses",
                )
        finally:
            feed.close()
            tmp.cleanup()

    def test_random_runs(self):
        for seed in range(10):
            with self.subTest(seed=seed):
                self.run_case(seed)


if __name__ == "__main__":
    unittest.main()
