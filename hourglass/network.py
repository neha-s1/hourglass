"""A network you control completely.

Messages do not travel over sockets. They are put on the simulator's event
heap with a delay drawn from the seeded RNG, which means every delivery time,
every dropped packet, and every reordering is a function of the seed.

Two knobs matter for finding bugs:

* **Partitions** -- split the nodes into groups that cannot reach each other.
  This is where consensus and quorum protocols go wrong, because each side
  can still make progress internally while believing the other side is dead.
* **Crashes** -- a node that stops answering. Its state survives; it simply
  becomes unreachable, the way a machine behind a failed switch does.

Reachability is re-checked when a message is *delivered*, not only when it is
sent, so a partition that opens while a message is in flight swallows it. That
is the pessimistic reading of what a real network does, and it is the reading
that finds bugs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from hourglass.faults import FaultConfig
from hourglass.runtime import (
    NANOS_PER_SECOND,
    Simulator,
    Suspend,
    Task,
    current_simulator,
)


@dataclass(frozen=True)
class Message:
    """One message in flight or waiting in an inbox."""

    mid: int
    src: str
    dst: str
    payload: Any
    sent_ns: int

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<msg {self.mid} {self.src}->{self.dst} {self.payload!r}>"


@dataclass(frozen=True)
class Recv:
    """Suspension request: block until a message arrives for ``node``."""

    node: str
    deadline_ns: int | None = None


class Network:
    """A simulated network connecting named nodes."""

    def __init__(self, sim: Simulator, config: FaultConfig | None = None) -> None:
        self.sim = sim
        self.config = config or FaultConfig.perfect()

        self._inboxes: dict[str, list[Message]] = {}
        self._waiters: dict[str, list[int]] = {}
        self._partitions: list[frozenset[str]] = []
        self._crashed: set[str] = set()
        self._next_mid = 0

        self.sent = 0
        self.delivered = 0
        self.dropped = 0
        self.duplicated = 0

        sim.register_handler(Recv, self._handle_recv)

    # -- topology ----------------------------------------------------------

    def add_node(self, name: str) -> None:
        self._inboxes.setdefault(name, [])
        self._waiters.setdefault(name, [])

    @property
    def nodes(self) -> list[str]:
        return sorted(self._inboxes)

    def partition(self, groups: Iterable[Iterable[str]]) -> None:
        """Split the network. Nodes in different groups cannot reach each other."""
        self._partitions = [frozenset(g) for g in groups]
        rendered = " | ".join("{" + ",".join(sorted(g)) + "}" for g in self._partitions)
        self.sim.log("partition", rendered)

    def heal(self) -> None:
        """Remove all partitions."""
        if self._partitions:
            self._partitions = []
            self.sim.log("heal", "network fully connected")

    def crash(self, node: str) -> None:
        """Make a node unreachable. Its state is untouched."""
        if node not in self._crashed:
            self._crashed.add(node)
            self.sim.log("crash", node)

    def restart(self, node: str) -> None:
        if node in self._crashed:
            self._crashed.discard(node)
            self.sim.log("restart", node)

    @property
    def crashed(self) -> frozenset[str]:
        return frozenset(self._crashed)

    def reachable(self, src: str, dst: str) -> bool:
        """Can ``src`` currently deliver to ``dst``?"""
        if src in self._crashed or dst in self._crashed:
            return False
        if not self._partitions:
            return True
        for group in self._partitions:
            if src in group:
                return dst in group
        # A node named in no group is isolated rather than silently reachable.
        return False

    # -- sending -----------------------------------------------------------

    def send(self, src: str, dst: str, payload: Any) -> None:
        """Fire a message at ``dst``. Never blocks, may never arrive."""
        self.sent += 1
        mid = self._next_mid
        self._next_mid += 1
        message = Message(mid=mid, src=src, dst=dst, payload=payload, sent_ns=self.sim.now_ns)

        if not self.reachable(src, dst):
            self.dropped += 1
            self.sim.log("drop", f"{src}->{dst} unreachable at send {payload!r}")
            return

        if self.sim.rng.random() < self.config.drop_probability:
            self.dropped += 1
            self.sim.log("drop", f"{src}->{dst} lost in transit {payload!r}")
            return

        self._schedule(message, tag="send")

        if self.sim.rng.random() < self.config.duplicate_probability:
            self.duplicated += 1
            self.sim.log("dup", f"{src}->{dst} will arrive twice {payload!r}")
            self._schedule(message, tag="dup")

    def _latency_ns(self) -> int:
        seconds = self.sim.rng.uniform(self.config.min_latency, self.config.max_latency)
        if self.sim.rng.random() < self.config.slow_probability:
            # A straggler. Later messages will overtake it, which is how
            # reordering happens without anyone explicitly shuffling a queue.
            seconds *= self.config.slow_multiplier
        return int(seconds * NANOS_PER_SECOND)

    def _schedule(self, message: Message, tag: str) -> None:
        delay = self._latency_ns()
        self.sim.log(tag, f"{message.src}->{message.dst} +{delay / 1e6:.1f}ms {message.payload!r}")
        self.sim.call_later(delay, lambda: self._deliver(message))

    # -- delivery ----------------------------------------------------------

    def _deliver(self, message: Message) -> None:
        # Re-check: the network may have split while this was in flight.
        if not self.reachable(message.src, message.dst):
            self.dropped += 1
            self.sim.log("drop", f"{message.src}->{message.dst} unreachable on arrival")
            return

        self.delivered += 1
        self.sim.log("deliver", f"{message.src}->{message.dst} {message.payload!r}")

        waiters = self._waiters[message.dst]
        if waiters:
            tid = waiters.pop(0)
            self.sim.wake(tid, message)
        else:
            self._inboxes[message.dst].append(message)

    # -- receiving ---------------------------------------------------------

    def _handle_recv(self, task: Task, request: Recv) -> None:
        inbox = self._inboxes.setdefault(request.node, [])
        self._waiters.setdefault(request.node, [])

        if inbox:
            self.sim.wake(task.tid, inbox.pop(0))
            return

        self.sim.block(task)
        self._waiters[request.node].append(task.tid)

        if request.deadline_ns is not None:
            node, tid = request.node, task.tid
            self.sim.call_at(request.deadline_ns, lambda: self._expire(node, tid))

    def _expire(self, node: str, tid: int) -> None:
        """Wake a waiter with ``None`` if nothing arrived before its deadline."""
        waiters = self._waiters[node]
        if tid in waiters:
            waiters.remove(tid)
            self.sim.log("timeout", f"{node} gave up waiting")
            self.sim.wake(tid, None)

    def pending(self, node: str) -> int:
        return len(self._inboxes.get(node, []))

    def stats(self) -> dict[str, int]:
        return {
            "sent": self.sent,
            "delivered": self.delivered,
            "dropped": self.dropped,
            "duplicated": self.duplicated,
        }


# ---------------------------------------------------------------------------
# The async API nodes use
# ---------------------------------------------------------------------------


async def recv(node: str, timeout: float | None = None) -> Message | None:
    """Wait for a message addressed to ``node``.

    Returns ``None`` if ``timeout`` seconds of virtual time pass first. A
    protocol that forgets to handle that ``None`` is exactly the kind of bug
    this framework exists to surface.
    """
    sim = current_simulator()
    deadline = None if timeout is None else sim.now_ns + int(timeout * NANOS_PER_SECOND)
    return await Suspend(Recv(node, deadline))
