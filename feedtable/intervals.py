"""Half-open integer interval helpers.

A subscription's route selector is the half-open interval ``[lo, hi)``:
integers only, ``lo <= route < hi``.

The set operations at the bottom are the core of "only read rows whose
visibility actually changed".  When a subscription moves from ``old`` to
``new``:

* ``subtract(new, old)`` is the *added* region - only rows there can enter.
* ``subtract(old, new)`` is the *removed* region - only rows there can exit.
* the intersection is *retained*: rows there are visible both before and
  after, so a range move alone can never enter or exit them.

A range move can add or remove up to two disjoint segments (for example,
``[0, 100) -> [80, 130)`` adds ``[100, 130)`` and removes ``[0, 80)``).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class Interval:
    lo: int
    hi: int

    def __post_init__(self):
        if isinstance(self.lo, bool) or isinstance(self.hi, bool):
            raise TypeError("interval bounds must be integers, not booleans")
        if not isinstance(self.lo, int) or not isinstance(self.hi, int):
            raise TypeError("interval bounds must be integers")
        if self.lo > self.hi:
            raise ValueError(f"invalid interval [{self.lo}, {self.hi}): lo > hi")

    @property
    def empty(self) -> bool:
        return self.lo == self.hi

    def contains(self, value: int) -> bool:
        return self.lo <= value < self.hi

    def as_list(self) -> list[int]:
        return [self.lo, self.hi]

    def __len__(self) -> int:
        return self.hi - self.lo


def subtract(a: Interval, b: Interval) -> list[Interval]:
    """Return ``a \\ b`` as a list of zero, one or two disjoint intervals."""
    if a.empty:
        return []
    # No overlap at all.
    if a.hi <= b.lo or b.hi <= a.lo:
        return [a]
    segments: list[Interval] = []
    if a.lo < b.lo:
        lo, hi = a.lo, min(a.hi, b.lo)
        if lo < hi:
            segments.append(Interval(lo, hi))
    if a.hi > b.hi:
        lo, hi = max(a.lo, b.hi), a.hi
        if lo < hi:
            segments.append(Interval(lo, hi))
    return segments


def intersect(a: Interval, b: Interval) -> Interval:
    lo = max(a.lo, b.lo)
    hi = min(a.hi, b.hi)
    return Interval(lo, hi) if lo <= hi else Interval(0, 0)
