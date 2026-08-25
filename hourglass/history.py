"""What actually happened, recorded so it can be judged afterwards.

A history is the list of operations a run performed: who issued each one,
when it was invoked, when it returned, and what it returned. Nothing about
the internals -- no replica state, no message order. Only what a user of the
database could have observed from outside.

That restriction is the point. If a history could not have been produced by a
single correct database serving one request at a time, then the system is
broken in a way no user should have to excuse, and it does not matter how
defensible the internal reason was.

Two subtleties shape everything downstream:

* An operation that **timed out** did not necessarily fail. A write that got
  two acknowledgements out of three still reached two replicas. Its effect
  may land at any later time, or never. It is recorded as *pending*, with no
  return time at all.
* A **read** that timed out told us nothing, so it constrains nothing and is
  dropped before checking.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable, Iterator

PUT = "put"
GET = "get"

#: Value of a key that has never been written.
MISSING: Any = None


@dataclass(frozen=True)
class Operation:
    """One client operation, as seen from outside the system."""

    index: int
    process: str
    kind: str
    key: str
    invoked_ns: int
    returned_ns: int
    ok: bool
    value: Any = None  # what a put wrote
    result: Any = None  # what a get returned

    @property
    def pending(self) -> bool:
        """Did this operation return without telling us whether it took effect?

        Only writes can be pending. A read that timed out is simply discarded;
        a write that timed out may still be sitting on some replicas.
        """
        return self.kind == PUT and not self.ok

    @property
    def effective_return_ns(self) -> float:
        """When this operation must have taken effect by.

        A pending write has no deadline: its effect can surface at any later
        point, so it never forces another operation to be ordered after it.
        """
        return math.inf if self.pending else float(self.returned_ns)

    def describe(self) -> str:
        if self.kind == PUT:
            outcome = "ok" if self.ok else "PENDING"
            return f"{self.process} put({self.key}, {self.value!r}) -> {outcome}"
        return f"{self.process} get({self.key}) -> {self.result!r}"

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<{self.describe()} [{self.invoked_ns}, {self.returned_ns}]>"


class History:
    """An ordered record of operations, sliceable by key."""

    def __init__(self, operations: Iterable[Operation]) -> None:
        self.operations = sorted(operations, key=lambda op: (op.invoked_ns, op.index))

    @classmethod
    def from_records(cls, records: Iterable[dict[str, Any]]) -> "History":
        """Build a history from the dicts the client emits."""
        operations = []
        for index, record in enumerate(records):
            operations.append(
                Operation(
                    index=index,
                    process=record["process"],
                    kind=record["op"],
                    key=record["key"],
                    invoked_ns=record["invoked_ns"],
                    returned_ns=record["returned_ns"],
                    ok=record["ok"],
                    value=record.get("value"),
                    result=record.get("result"),
                )
            )
        return cls(operations)

    # -- views -------------------------------------------------------------

    def checkable(self) -> "History":
        """Drop operations that carry no information.

        A read that timed out never learned a value, so it cannot contradict
        anything. Keeping it would only enlarge the search.
        """
        return History(op for op in self.operations if not (op.kind == GET and not op.ok))

    def keys(self) -> list[str]:
        return sorted({op.key for op in self.operations})

    def by_key(self) -> dict[str, list[Operation]]:
        """Split into independent per-key sub-histories.

        Operations on different keys never interact, so the whole history is
        linearizable exactly when each key's slice is. This is what makes
        checking a hundred operations tractable: three searches over thirty
        operations instead of one search over a hundred.
        """
        buckets: dict[str, list[Operation]] = {key: [] for key in self.keys()}
        for op in self.operations:
            buckets[op.key].append(op)
        return buckets

    def concurrency(self) -> int:
        """The largest number of operations ever in flight at once.

        Measured from when each operation was issued to when the client
        stopped waiting -- including operations that timed out, which did
        stop being in flight even though their effect is still undecided.
        Treating a pending write as in flight forever would count every one
        that ever timed out, and report a concurrency far above the number
        of clients.
        """
        events: list[tuple[int, int]] = []
        for op in self.operations:
            events.append((op.invoked_ns, +1))
            events.append((op.returned_ns, -1))
        events.sort()
        current = peak = 0
        for _time, delta in events:
            current += delta
            peak = max(peak, current)
        return peak

    # -- plumbing ----------------------------------------------------------

    def __len__(self) -> int:
        return len(self.operations)

    def __iter__(self) -> Iterator[Operation]:
        return iter(self.operations)

    def __getitem__(self, index: int) -> Operation:
        return self.operations[index]

    def render(self, limit: int | None = None) -> str:
        lines = []
        for op in self.operations[:limit]:
            end = "..." if op.pending else f"{op.returned_ns / 1e6:.2f}ms"
            lines.append(f"  [{op.invoked_ns / 1e6:>8.2f}ms -> {end:>10}]  {op.describe()}")
        if limit is not None and len(self.operations) > limit:
            lines.append(f"  ... {len(self.operations) - limit} more")
        return "\n".join(lines)

    def summary(self) -> str:
        pending = sum(1 for op in self.operations if op.pending)
        return (
            f"{len(self.operations)} operations, {len(self.keys())} keys, "
            f"peak concurrency {self.concurrency()}, {pending} pending writes"
        )
