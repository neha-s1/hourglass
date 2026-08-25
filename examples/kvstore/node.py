"""One replica of the key-value store.

Each replica keeps its own copy of every key and settles conflicts by keeping
whichever write carries the higher timestamp -- "last write wins". This is a
real strategy used by real databases, and it is deliberately written here the
way a competent engineer writes it the first time: correct-looking, easy to
follow, and quietly wrong in ways that only show up when the network
misbehaves.

Nothing is planted. The weaknesses are the ordinary omissions:

* A write that fails to reach a quorum is not rolled back. The replicas that
  did accept it keep it.
* Replicas never compare notes with each other. Nothing repairs a replica that
  missed an update.
* Timestamps come from the clock, so two writes issued at the same instant are
  ordered by a tie-breaker rather than by what actually happened first.

Every one of those is defensible in isolation. Together, under a partition,
they lose data. Finding out exactly how is what days 4 and 5 are for.
"""

from __future__ import annotations

from typing import Any

from hourglass.network import Network, recv
from hourglass.runtime import current_simulator

#: Timestamp for a key that has never been written. Sorts below every real
#: timestamp, so an untouched replica never wins a comparison.
NEVER: tuple[int, int] = (-1, -1)


class Replica:
    """A single storage node.

    It answers two questions and asks none. All coordination -- deciding how
    many replicas must agree, and what to do when they disagree -- lives in
    the client.
    """

    def __init__(self, name: str, net: Network) -> None:
        self.name = name
        self.net = net
        # key -> (value, timestamp)
        self.store: dict[str, tuple[Any, tuple[int, int]]] = {}
        self.puts_accepted = 0
        self.puts_ignored = 0
        self.gets_served = 0

    async def serve(self) -> None:
        """Answer requests forever.

        The task simply blocks when there is nothing to do. A blocked task
        does not keep the simulation alive, so the run ends naturally once
        every client has finished.
        """
        while True:
            message = await recv(self.name)
            if message is None:
                continue
            self._handle(message.src, message.payload)

    def _handle(self, sender: str, payload: tuple[Any, ...]) -> None:
        kind = payload[0]

        if kind == "put":
            _, request_id, key, value, timestamp = payload
            self._apply_put(key, value, timestamp)
            self.net.send(self.name, sender, ("put_ok", request_id))

        elif kind == "get":
            _, request_id, key = payload
            value, timestamp = self.store.get(key, (None, NEVER))
            self.gets_served += 1
            self.net.send(self.name, sender, ("get_ok", request_id, value, timestamp))

    def _apply_put(self, key: str, value: Any, timestamp: tuple[int, int]) -> None:
        """Accept a write only if it is newer than what we already hold.

        The comparison is what makes replicas converge when they all
        eventually see the same set of writes. It is also what silently
        discards a write when two clients pick timestamps that do not reflect
        the real order of events.
        """
        current = self.store.get(key)
        if current is None or timestamp > current[1]:
            self.store[key] = (value, timestamp)
            self.puts_accepted += 1
            current_simulator().log("accept", f"{self.name} {key}={value!r} @{timestamp}")
        else:
            self.puts_ignored += 1
            current_simulator().log("stale", f"{self.name} rejected {key}={value!r} @{timestamp}")

    def snapshot(self) -> dict[str, tuple[Any, tuple[int, int]]]:
        """A copy of everything this replica holds. Used by tests, not by the protocol."""
        return dict(self.store)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<Replica {self.name} keys={len(self.store)}>"
