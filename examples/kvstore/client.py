"""The coordinator: turns one client operation into a quorum of replica calls.

A write is sent to every replica and declared successful once ``write_quorum``
of them acknowledge. A read is sent to every replica and answered from the
first ``read_quorum`` replies, keeping whichever carries the highest timestamp.

With five replicas and quorums of three, any read set and any write set must
share at least one replica -- 3 + 3 > 5. That overlap is the argument for why
a read always sees the most recent write. The argument holds only for writes
that *succeeded*. What happens to a write that reached two replicas and then
timed out is not covered by it, and neither is what the client should do
about one.

Every operation is recorded with the instant it was issued and the instant it
returned. That record is the raw material for the linearizability checker.
"""

from __future__ import annotations

from typing import Any

from hourglass.network import Network, recv
from hourglass.runtime import NANOS_PER_SECOND, current_simulator

from examples.kvstore.node import NEVER

#: Returned by a read when the key has never been written.
MISSING = None


class Client:
    """Issues operations against the replica set."""

    def __init__(
        self,
        name: str,
        index: int,
        net: Network,
        replicas: list[str],
        write_quorum: int,
        read_quorum: int,
        timeout: float,
        history: list[dict[str, Any]] | None = None,
    ) -> None:
        self.name = name
        self.index = index
        self.net = net
        self.replicas = replicas
        self.write_quorum = write_quorum
        self.read_quorum = read_quorum
        self.timeout = timeout
        self.history = history if history is not None else []
        self._request_counter = 0

    # -- helpers -----------------------------------------------------------

    def _next_request_id(self) -> str:
        self._request_counter += 1
        return f"{self.name}-{self._request_counter}"

    def _timestamp(self) -> tuple[int, int]:
        """Order writes by the clock, breaking ties by client index.

        Physical-clock last-write-wins. Widely deployed, and the source of a
        whole genre of data-loss incidents, because the clock tells you when a
        client *issued* a write -- not the order in which replicas saw them.
        """
        return (current_simulator().now_ns, self.index)

    def _record(self, entry: dict[str, Any]) -> None:
        self.history.append(entry)

    async def _collect(self, request_id: str, wanted_kind: str, needed: int) -> list[tuple]:
        """Wait for ``needed`` replies to ``request_id``, or give up.

        Replies to earlier requests can still be in flight; they are dropped
        rather than counted. A timeout leaves the caller with fewer replies
        than it asked for and no idea which replicas did the work.
        """
        sim = current_simulator()
        deadline_ns = sim.now_ns + int(self.timeout * NANOS_PER_SECOND)
        collected: list[tuple] = []

        while len(collected) < needed:
            remaining_ns = deadline_ns - sim.now_ns
            if remaining_ns <= 0:
                break
            message = await recv(self.name, timeout=remaining_ns / NANOS_PER_SECOND)
            if message is None:
                break
            payload = message.payload
            if payload[0] == wanted_kind and payload[1] == request_id:
                collected.append(payload)

        return collected

    # -- operations --------------------------------------------------------

    async def put(self, key: str, value: Any) -> bool:
        """Write ``value`` to ``key``. Returns whether a quorum acknowledged."""
        sim = current_simulator()
        request_id = self._next_request_id()
        timestamp = self._timestamp()
        invoked_ns = sim.now_ns

        for replica in self.replicas:
            self.net.send(self.name, replica, ("put", request_id, key, value, timestamp))

        acks = await self._collect(request_id, "put_ok", self.write_quorum)
        ok = len(acks) >= self.write_quorum

        sim.log("put", f"{self.name} {key}={value!r} {'ok' if ok else 'TIMEOUT'} ({len(acks)} acks)")
        self._record(
            {
                "process": self.name,
                "op": "put",
                "key": key,
                "value": value,
                "invoked_ns": invoked_ns,
                "returned_ns": sim.now_ns,
                "ok": ok,
                "result": None,
                "timestamp": timestamp,
            }
        )
        return ok

    async def get(self, key: str) -> tuple[Any, bool]:
        """Read ``key``. Returns ``(value, ok)``; ``value`` is meaningless if not ok."""
        sim = current_simulator()
        request_id = self._next_request_id()
        invoked_ns = sim.now_ns

        for replica in self.replicas:
            self.net.send(self.name, replica, ("get", request_id, key))

        replies = await self._collect(request_id, "get_ok", self.read_quorum)
        ok = len(replies) >= self.read_quorum

        value: Any = MISSING
        if ok:
            # ("get_ok", request_id, value, timestamp) -- keep the newest.
            best = max(replies, key=lambda reply: reply[3])
            value = best[2] if best[3] != NEVER else MISSING

        sim.log(
            "get",
            f"{self.name} {key} -> {value!r} {'ok' if ok else 'TIMEOUT'} ({len(replies)} replies)",
        )
        self._record(
            {
                "process": self.name,
                "op": "get",
                "key": key,
                "value": None,
                "invoked_ns": invoked_ns,
                "returned_ns": sim.now_ns,
                "ok": ok,
                "result": value,
            }
        )
        return value, ok
