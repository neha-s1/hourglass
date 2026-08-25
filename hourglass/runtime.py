"""The deterministic runtime: a scheduler that owns time and ordering.

A concurrent program is unpredictable because two things vary between runs:
*when* things happen, and *in what order* two ready tasks get to run. Real
programs get both from the operating system, which is why a bug that depends
on unlucky timing shows up once in five hundred runs and never again.

This module takes both away from the operating system:

  * **Time is virtual.** ``Simulator.now_ns`` is just an integer the scheduler
    increments. Nothing ever really waits, so ``await sleep(3600)`` costs
    microseconds and a full simulated hour fits inside a unit test.
  * **Ordering is seeded.** When several tasks are ready at the same instant,
    :meth:`Simulator._pick_runnable` asks a seeded RNG which one goes next.

Consequence: one integer describes one entire universe. Seed 84213 produces
the same trace today, tomorrow, and on someone else's laptop. A bug found
under a seed can be replayed forever.
"""

from __future__ import annotations

import heapq
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Iterator

NANOS_PER_SECOND = 1_000_000_000

# Time is stored as an integer count of nanoseconds rather than a float.
# Floats accumulate rounding error as the clock advances, and two runs that
# disagree in the last bit of a float would print different traces -- which
# would defeat the whole point.


# ---------------------------------------------------------------------------
# Suspension requests
#
# A task pauses itself by awaiting one of these. The object travels out to the
# scheduler, which decides when (and whether) to resume the task.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Sleep:
    """Resume this task once the virtual clock reaches ``deadline_ns``."""

    deadline_ns: int


@dataclass(frozen=True)
class YieldNow:
    """Stay runnable, but let the scheduler consider other tasks first."""


class Suspend:
    """The bridge between ``await`` and the scheduler.

    ``await Suspend(request)`` hands ``request`` to whoever is driving this
    coroutine with ``.send()``, and evaluates to whatever value is sent back.
    Every pause in the system goes through here, which is what makes the
    scheduler the single point of control.
    """

    __slots__ = ("request",)

    def __init__(self, request: Any) -> None:
        self.request = request

    def __await__(self) -> Iterator[Any]:
        return (yield self.request)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

RUNNABLE = "runnable"
SLEEPING = "sleeping"
BLOCKED = "blocked"
DONE = "done"
CRASHED = "crashed"


@dataclass
class Task:
    """One concurrent activity: a coroutine plus the scheduler's bookkeeping."""

    tid: int
    name: str
    coro: Coroutine[Any, Any, Any]
    state: str = RUNNABLE
    send_value: Any = None
    result: Any = None
    error: BaseException | None = None

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<Task {self.tid} {self.name} {self.state}>"


class Deadlock(RuntimeError):
    """Raised when no task can run and no timer will ever fire."""


# ---------------------------------------------------------------------------
# The simulator
# ---------------------------------------------------------------------------


class Simulator:
    """A deterministic scheduler driven by one seed.

    Two runs with the same seed produce byte-identical traces. Two runs with
    different seeds explore different interleavings of the same program, which
    is how the bug hunt in ``sweep.py`` searches the space of possible
    orderings instead of sampling it by accident.
    """

    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.rng = random.Random(seed)
        self.now_ns: int = 0
        self.trace: list[str] = []

        self._tasks: dict[int, Task] = {}
        self._runnable: list[int] = []

        # Scheduled future work, ordered by virtual deadline. Entries are
        # (deadline_ns, seq, callback). A sleeping task and an in-flight
        # network message are the same kind of thing to the scheduler: a
        # callback waiting for the clock to reach it.
        self._events: list[tuple[int, int, Callable[[], None]]] = []

        self._next_tid = 0
        # Monotonic tie-breaker. Two events with the same deadline are ordered
        # by insertion, which keeps the heap totally ordered without ever
        # comparing the callbacks themselves.
        self._seq = 0

        # Set by the network layer (day 2). Kept as a hook so runtime.py has
        # no knowledge of messaging.
        self._request_handlers: dict[type, Callable[[Task, Any], None]] = {}

    # -- clock -------------------------------------------------------------

    @property
    def now(self) -> float:
        """The virtual clock, in seconds. Convenience for humans."""
        return self.now_ns / NANOS_PER_SECOND

    def log(self, kind: str, detail: str) -> None:
        """Append one line to the trace.

        The trace is the artifact the determinism test compares, so everything
        in it must be derived from simulator state -- never from real time,
        memory addresses, or dict iteration order of unsorted keys.
        """
        self.trace.append(f"{self.now_ns:>14} {kind:<9} {detail}")

    # -- task management ---------------------------------------------------

    def spawn(self, coro: Coroutine[Any, Any, Any], name: str | None = None) -> int:
        """Register a coroutine as a task and mark it runnable."""
        tid = self._next_tid
        self._next_tid += 1
        task = Task(tid=tid, name=name or f"task-{tid}", coro=coro)
        self._tasks[tid] = task
        self._runnable.append(tid)
        self.log("spawn", task.name)
        return tid

    def register_handler(self, request_type: type, handler: Callable[[Task, Any], None]) -> None:
        """Let another module (e.g. the network) handle its own request types."""
        self._request_handlers[request_type] = handler

    def wake(self, tid: int, value: Any = None) -> None:
        """Move a blocked task back onto the runnable list."""
        task = self._tasks[tid]
        if task.state in (DONE, CRASHED):
            return
        task.send_value = value
        task.state = RUNNABLE
        self._runnable.append(tid)

    def block(self, task: Task) -> None:
        """Park a task until someone calls :meth:`wake` on it."""
        task.state = BLOCKED

    # -- scheduled callbacks -----------------------------------------------

    def call_at(self, deadline_ns: int, callback: Callable[[], None]) -> None:
        """Run ``callback`` once the virtual clock reaches ``deadline_ns``."""
        self._seq += 1
        heapq.heappush(self._events, (deadline_ns, self._seq, callback))

    def call_later(self, delay_ns: int, callback: Callable[[], None]) -> None:
        """Run ``callback`` after ``delay_ns`` of virtual time."""
        self.call_at(self.now_ns + delay_ns, callback)

    def _wake_sleeper(self, tid: int) -> None:
        task = self._tasks[tid]
        if task.state == SLEEPING:
            task.state = RUNNABLE
            self._runnable.append(tid)

    def task(self, tid: int) -> Task:
        return self._tasks[tid]

    @property
    def tasks(self) -> dict[int, Task]:
        return self._tasks

    # -- the scheduling loop -----------------------------------------------

    def _pick_runnable(self) -> int:
        """Choose which ready task runs next.

        This is the heart of the whole framework. Every other source of
        nondeterminism has been removed, so the interleaving a run explores is
        decided entirely here -- by the seeded RNG. Change the seed and you
        get a different, but equally reproducible, ordering.
        """
        index = self.rng.randrange(len(self._runnable))
        return self._runnable.pop(index)

    def _advance_clock(self) -> None:
        """Nothing can run, so jump time forward to the next scheduled event.

        This is why simulated waiting is free: an hour of ``sleep`` is one
        integer assignment.
        """
        deadline, _seq, callback = heapq.heappop(self._events)
        if deadline > self.now_ns:
            self.now_ns = deadline
        callback()

    def _step(self, tid: int) -> None:
        """Run one task until its next suspension point."""
        task = self._tasks[tid]
        try:
            request = task.coro.send(task.send_value)
        except StopIteration as stop:
            task.state = DONE
            task.result = stop.value
            self.log("done", task.name)
            return
        except Exception as exc:  # a task raising is a finding, not a crash
            task.state = CRASHED
            task.error = exc
            self.log("error", f"{task.name} {type(exc).__name__}: {exc}")
            return

        task.send_value = None
        self._dispatch(task, request)

    def _dispatch(self, task: Task, request: Any) -> None:
        if isinstance(request, Sleep):
            task.state = SLEEPING
            tid = task.tid
            self.call_at(request.deadline_ns, lambda: self._wake_sleeper(tid))
            return
        if isinstance(request, YieldNow):
            task.state = RUNNABLE
            self._runnable.append(task.tid)
            return

        handler = self._request_handlers.get(type(request))
        if handler is None:
            raise TypeError(
                f"{task.name} awaited an unknown request {request!r}. "
                "Register a handler with Simulator.register_handler()."
            )
        handler(task, request)

    def run(self, max_steps: int = 1_000_000) -> None:
        """Run until every task is finished, blocked forever, or the cap hits."""
        steps = 0
        while steps < max_steps:
            if self._runnable:
                self._step(self._pick_runnable())
                steps += 1
                continue
            if self._events:
                self._advance_clock()
                continue
            break
        else:
            raise Deadlock(f"exceeded {max_steps} steps -- probable livelock")

    # -- introspection -----------------------------------------------------

    def unfinished(self) -> list[Task]:
        return [t for t in self._tasks.values() if t.state not in (DONE, CRASHED)]

    def errors(self) -> list[BaseException]:
        return [t.error for t in self._tasks.values() if t.error is not None]


# ---------------------------------------------------------------------------
# The public async API tasks use
# ---------------------------------------------------------------------------

_current: list[Simulator] = []


def current_simulator() -> Simulator:
    if not _current:
        raise RuntimeError("no simulator is running")
    return _current[-1]


class running:
    """Context manager marking which simulator the ``sleep`` helpers target."""

    def __init__(self, sim: Simulator) -> None:
        self.sim = sim

    def __enter__(self) -> Simulator:
        _current.append(self.sim)
        return self.sim

    def __exit__(self, *exc: object) -> None:
        _current.pop()


async def sleep(seconds: float) -> None:
    """Pause this task for ``seconds`` of *virtual* time."""
    sim = current_simulator()
    deadline = sim.now_ns + int(seconds * NANOS_PER_SECOND)
    await Suspend(Sleep(deadline))


async def yield_now() -> None:
    """Offer the scheduler a chance to interleave another task here.

    Sprinkling these through a system under test widens the set of
    interleavings the seed can explore.
    """
    await Suspend(YieldNow())
