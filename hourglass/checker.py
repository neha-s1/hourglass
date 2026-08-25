"""Deciding whether a history could have come from a correct database.

The question a linearizability checker answers is deliberately narrow:

    Is there **some** order in which these operations could have happened,
    one at a time, that explains every value that was returned?

If such an order exists, the system behaved acceptably. Its internals may
have been chaotic -- messages reordered, replicas disagreeing, writes landing
out of sequence -- but no client could tell, and that is all anyone is owed.

If no such order exists, the system returned something impossible, and the
proof is exhaustive rather than a guess.

Two rules constrain the order:

1. **Real time.** If one operation returned before another was invoked, it
   must come first. Operations that overlap in time may be placed in either
   order -- that freedom is what makes concurrency legal.
2. **Semantics.** A read must return the value of the write most recently
   placed before it.

The search is the Wing and Gong algorithm: repeatedly pick an operation that
nothing else is forced to precede, apply it, and recurse. Two things keep it
from exploding -- memoising states already proven hopeless, and checking each
key separately, since operations on different keys cannot affect each other.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from hourglass.history import GET, MISSING, History, Operation

#: How many search states to explore per key before giving up. A checker that
#: reported "violation" when it merely ran out of patience would be worse than
#: no checker at all, so exhaustion is reported as its own verdict.
DEFAULT_BUDGET = 500_000


class Verdict(Enum):
    OK = "ok"
    VIOLATION = "violation"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return self.value


class _BudgetExhausted(Exception):
    pass


@dataclass
class KeyReport:
    """The verdict for one key's slice of the history."""

    key: str
    verdict: Verdict
    operations: list[Operation]
    linearization: list[tuple[int, bool]] = field(default_factory=list)
    states_explored: int = 0
    #: For a violation: the shortest prefix that is already impossible.
    witness: list[Operation] = field(default_factory=list)

    def render(self) -> str:
        head = f"key {self.key!r}: {self.verdict} ({len(self.operations)} ops, {self.states_explored} states)"
        if self.verdict is not Verdict.VIOLATION:
            return head
        lines = [head, "  smallest set of operations with no valid ordering:"]
        for op in self.witness:
            marker = "  <-- impossible" if op is self.witness[-1] else ""
            lines.append(f"    {op.describe()}{marker}")
        return "\n".join(lines)


@dataclass
class CheckReport:
    verdict: Verdict
    keys: list[KeyReport]

    @property
    def violations(self) -> list[KeyReport]:
        return [report for report in self.keys if report.verdict is Verdict.VIOLATION]

    @property
    def states_explored(self) -> int:
        return sum(report.states_explored for report in self.keys)

    def render(self) -> str:
        return "\n".join(report.render() for report in self.keys)


# ---------------------------------------------------------------------------
# The search
# ---------------------------------------------------------------------------


def check_key(operations: list[Operation], budget: int = DEFAULT_BUDGET) -> KeyReport:
    """Decide whether one key's operations admit a valid ordering."""
    ops = sorted(operations, key=lambda op: (op.invoked_ns, op.index))
    key = ops[0].key if ops else ""

    hopeless: set[tuple[frozenset[int], Any]] = set()
    explored = 0

    def search(remaining: frozenset[int], value: Any, order: list[tuple[int, bool]]):
        nonlocal explored

        if not remaining:
            return list(order)

        state = (remaining, value)
        if state in hopeless:
            return None
        hopeless.add(state)

        explored += 1
        if explored > budget:
            raise _BudgetExhausted

        # An operation may go next only if nothing else is forced before it:
        # that is, only if it was invoked before the earliest return still
        # outstanding. Pending writes have no return time, so they never
        # force anything.
        earliest_return = min(ops[index].effective_return_ns for index in remaining)

        for index in sorted(remaining):
            op = ops[index]
            if op.invoked_ns > earliest_return:
                continue

            # Possibility one: this operation takes effect here.
            if op.kind == GET:
                if op.result != value:
                    continue  # would have had to return something else
                next_value = value
            else:
                next_value = op.value

            order.append((index, True))
            found = search(remaining - {index}, next_value, order)
            if found is not None:
                return found
            order.pop()

            # Possibility two: a write that timed out never landed at all.
            if op.pending:
                order.append((index, False))
                found = search(remaining - {index}, value, order)
                if found is not None:
                    return found
                order.pop()

        return None

    try:
        linearization = search(frozenset(range(len(ops))), MISSING, [])
    except _BudgetExhausted:
        return KeyReport(key=key, verdict=Verdict.UNKNOWN, operations=ops, states_explored=explored)

    if linearization is not None:
        return KeyReport(
            key=key,
            verdict=Verdict.OK,
            operations=ops,
            linearization=linearization,
            states_explored=explored,
        )

    return KeyReport(
        key=key,
        verdict=Verdict.VIOLATION,
        operations=ops,
        states_explored=explored,
        witness=_localise(ops, budget),
    )


def _localise(ops: list[Operation], budget: int) -> list[Operation]:
    """Find the shortest prefix of ``ops`` that is already impossible.

    Linearizability is inherited by subsets: if the whole history has a valid
    ordering, so does every part of it. So impossibility is monotone in the
    prefix length, and the smallest failing prefix can be found by binary
    search rather than by trying every subset.

    The result is the useful half of a bug report -- typically three or four
    operations instead of thirty.
    """

    def fails(count: int) -> bool:
        prefix = ops[:count]
        if not prefix:
            return False
        try:
            return _search_only(prefix, budget) is None
        except _BudgetExhausted:
            return False

    low, high = 1, len(ops)
    while low < high:
        middle = (low + high) // 2
        if fails(middle):
            high = middle
        else:
            low = middle + 1
    return ops[:low]


def _search_only(ops: list[Operation], budget: int):
    """The same search, without reporting. Used by the prefix localiser."""
    hopeless: set[tuple[frozenset[int], Any]] = set()
    explored = 0

    def search(remaining: frozenset[int], value: Any):
        nonlocal explored
        if not remaining:
            return True
        state = (remaining, value)
        if state in hopeless:
            return None
        hopeless.add(state)
        explored += 1
        if explored > budget:
            raise _BudgetExhausted

        earliest_return = min(ops[index].effective_return_ns for index in remaining)
        for index in sorted(remaining):
            op = ops[index]
            if op.invoked_ns > earliest_return:
                continue
            if op.kind == GET:
                if op.result != value:
                    continue
                next_value = value
            else:
                next_value = op.value
            if search(remaining - {index}, next_value):
                return True
            if op.pending and search(remaining - {index}, value):
                return True
        return None

    return search(frozenset(range(len(ops))), MISSING)


def check(history: History, budget: int = DEFAULT_BUDGET) -> CheckReport:
    """Check a whole history, one key at a time."""
    checkable = history.checkable()
    reports = [check_key(ops, budget) for _key, ops in sorted(checkable.by_key().items())]

    if any(report.verdict is Verdict.VIOLATION for report in reports):
        verdict = Verdict.VIOLATION
    elif any(report.verdict is Verdict.UNKNOWN for report in reports):
        verdict = Verdict.UNKNOWN
    else:
        verdict = Verdict.OK

    return CheckReport(verdict=verdict, keys=reports)
