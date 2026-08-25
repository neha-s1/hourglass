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
        read_repair: bool = True,
        count_distinct_replicas: bool = True,
    ) -> None:
        self.name = name
        self.index = index
        self.net = net
        self.replicas = replicas
        self.write_quorum = write_quorum
        self.read_quorum = read_quorum
        self.timeout = timeout
        #: Write back what a read returns before returning it. Off, a read is
        #: a peek: a value can be observed and then un-observed.
        self.read_repair = read_repair
        #: Count answering replicas rather than arriving messages. Off, a
        #: duplicated packet inflates a quorum.
        self.count_distinct_replicas = count_distinct_replicas
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
        """Wait until ``needed`` distinct replicas have answered, or give up.

        Counting *replicas* rather than *replies* is the whole point. The
        network may deliver the same message twice, and a quorum assembled
        from three replies that came from two machines is not a quorum: the
        overlap argument behind ``W + R > N`` counts nodes, so a duplicate
        silently shrinks the read set and lets it miss a committed write.

        Replies to earlier requests can still be in flight; they are ignored.
        A timeout leaves the caller short, with no idea which replicas did
        the work.
        """
        sim = current_simulator()
        deadline_ns = sim.now_ns + int(self.timeout * NANOS_PER_SECOND)
        answered: dict[str, tuple] = {}

        while len(answered) < needed:
            remaining_ns = deadline_ns - sim.now_ns
            if remaining_ns <= 0:
                break
            message = await recv(self.name, timeout=remaining_ns / NANOS_PER_SECOND)
            if message is None:
                break
            payload = message.payload
            if payload[0] == wanted_kind and payload[1] == request_id:
                if self.count_distinct_replicas:
                    answered[message.src] = payload
                else:
                    # The original bug, kept switchable: counting messages
                    # lets one replica answering twice look like two.
                    answered[f"{message.src}#{len(answered)}"] = payload

        return list(answered.values())

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

    async def _repair(self, key: str, value: Any, timestamp: tuple[int, int]) -> bool:
        """Write the value being returned back to a quorum before returning it.

        Without this a read is a peek: it may see a value that reached only
        one replica, report it, and leave the next read to miss it -- so a
        value can be observed and then un-observed, which no correct register
        does.

        The write-back carries the *original* timestamp, so it reorders
        nothing; replicas that already hold something newer ignore it. What it
        guarantees is that by the time this read returns, the value it
        reports is on enough replicas that every later read must see it.

        This is the second phase of the ABD algorithm, and it is why a
        linearizable read costs two round trips rather than one. If the
        write-back cannot reach a quorum, the read has not been made durable
        and must report failure rather than a value it cannot stand behind.
        """
        request_id = self._next_request_id()
        for replica in self.replicas:
            self.net.send(self.name, replica, ("put", request_id, key, value, timestamp))
        acks = await self._collect(request_id, "put_ok", self.write_quorum)
        return len(acks) >= self.write_quorum

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

            if best[3] != NEVER and self.read_repair:
                ok = await self._repair(key, value, best[3])

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
