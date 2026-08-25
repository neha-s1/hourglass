"""Cutting a failure down to the part that matters.

A seed that breaks the system is a reproduction, but not yet an explanation.
The run that produced it had five clients, a hundred operations, two network
partitions and a crashed replica, and almost none of that was necessary. The
useful artifact is the smallest version that still fails.

The sequence minimiser here is delta debugging -- Zeller and Hildebrandt's
ddmin. Rather than removing one element at a time, it removes progressively
finer slices: first halves, then quarters, then eighths. When a large removal
succeeds it undoes a lot of work at once; when everything fails it narrows
and tries again. That is what makes it practical on inputs where each test is
expensive.

Nothing here knows what it is shrinking. It is handed a list and a question --
"does this still fail?" -- and it answers with the shortest list for which the
answer is still yes.
"""

from __future__ import annotations

from typing import Callable, Sequence, TypeVar

T = TypeVar("T")

#: How many times the predicate may be evaluated before giving up. Each call
#: re-runs a whole simulation, so this is the real cost control.
DEFAULT_BUDGET = 300


class Budget:
    """A shared count of how many test runs are left."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.used = 0

    def spend(self) -> bool:
        if self.used >= self.limit:
            return False
        self.used += 1
        return True

    @property
    def exhausted(self) -> bool:
        return self.used >= self.limit


def minimise_sequence(
    items: Sequence[T],
    still_fails: Callable[[list[T]], bool],
    budget: Budget | None = None,
) -> list[T]:
    """Return the shortest sublist of ``items`` for which ``still_fails`` holds.

    ``still_fails`` must already be true for ``items``; the result is never
    longer than the input, and is usually far shorter.
    """
    budget = budget or Budget(DEFAULT_BUDGET)
    current = list(items)
    granularity = 2

    while len(current) >= 2:
        chunk_size = max(1, len(current) // granularity)
        chunks = [current[i : i + chunk_size] for i in range(0, len(current), chunk_size)]

        reduced = False
        for index in range(len(chunks)):
            # Everything except this chunk.
            candidate = [item for j, chunk in enumerate(chunks) if j != index for item in chunk]
            if not candidate:
                continue
            if not budget.spend():
                return current
            if still_fails(candidate):
                current = candidate
                # A successful cut means the input just got much smaller;
                # go back to coarse slices rather than staying fine-grained.
                granularity = max(granularity - 1, 2)
                reduced = True
                break

        if reduced:
            continue
        if granularity >= len(current):
            break
        granularity = min(granularity * 2, len(current))

    return current


def minimise_count(
    start: int,
    floor: int,
    still_fails: Callable[[int], bool],
    budget: Budget | None = None,
) -> int:
    """Find the smallest count at or above ``floor`` that still fails.

    Deliberately not a binary search. Shrinking a simulated run is not
    monotone: halving the number of operations does not simply remove work,
    it produces an entirely different run, which may happen not to trip the
    bug. So candidates are tried smallest-first and the first that still
    fails wins, with the original returned if none do.
    """
    budget = budget or Budget(DEFAULT_BUDGET)

    candidates: list[int] = []
    value = floor
    while value < start:
        candidates.append(value)
        value = max(value + 1, value * 2)

    for candidate in candidates:
        if not budget.spend():
            break
        if still_fails(candidate):
            return candidate
    return start
