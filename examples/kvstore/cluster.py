"""Wire a cluster together and run a workload against it.

This is the harness the bug hunt drives. One call to :func:`run` builds five
replicas and a handful of clients, plays out a randomised sequence of reads
and writes, optionally sabotages the network partway through, and hands back
everything needed to judge the result afterwards.

Like everything else here, the whole run is a function of the seed: which
client does what, in which order, against which key, with what network
timing.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Any, Iterable

from hourglass.faults import FaultConfig
from hourglass.network import Network
from hourglass.runtime import Simulator, current_simulator, running, sleep

from examples.kvstore.client import Client
from examples.kvstore.node import Replica


@dataclass(frozen=True)
class ClusterConfig:
    """Shape of the cluster and the workload run against it."""

    replicas: int = 5
    write_quorum: int = 3
    read_quorum: int = 3
    clients: int = 5
    operations_per_client: int = 20
    keys: int = 3
    timeout: float = 0.5
    think_time: float = 0.05
    read_fraction: float = 0.5
    #: Both default to the fixed behaviour. Turning them off restores the
    #: two bugs the sweep originally found, so the checker and the shrinker
    #: keep a real target to be tested against.
    read_repair: bool = True
    count_distinct_replicas: bool = True

    def __post_init__(self) -> None:
        if self.write_quorum + self.read_quorum <= self.replicas:
            raise ValueError(
                f"W + R must exceed N for read and write sets to overlap; "
                f"got {self.write_quorum} + {self.read_quorum} <= {self.replicas}"
            )

    @property
    def total_operations(self) -> int:
        return self.clients * self.operations_per_client


#: A scheduled act of sabotage: (seconds, kind, payload).
#: kind is one of "partition", "heal", "crash", "restart".
FaultEvent = tuple[float, str, Any]


@dataclass
class RunResult:
    seed: int
    history: list[dict[str, Any]]
    trace: list[str]
    network: dict[str, int]
    snapshots: dict[str, dict[str, tuple[Any, tuple[int, int]]]]
    config: ClusterConfig = field(default_factory=ClusterConfig)

    @property
    def operations(self) -> int:
        return len(self.history)

    @property
    def succeeded(self) -> int:
        return sum(1 for entry in self.history if entry["ok"])

    @property
    def failed(self) -> int:
        return self.operations - self.succeeded

    def replicas_agree(self) -> bool:
        """Do all replicas hold the same value for every key?

        Only meaningful once the network has healed and everything has
        settled -- and even then, this protocol has no mechanism to make it
        true. Divergence here is a finding, not a crash.
        """
        keys = {key for snap in self.snapshots.values() for key in snap}
        for key in keys:
            values = {snap.get(key, (None, None))[0] for snap in self.snapshots.values()}
            if len(values) > 1:
                return False
        return True

    def summary(self) -> str:
        return (
            f"seed={self.seed}  ops={self.operations}  "
            f"ok={self.succeeded}  failed={self.failed}  "
            f"net={self.network}  converged={self.replicas_agree()}"
        )


async def _workload(client: Client, config: ClusterConfig) -> None:
    """One client's share of the work: a random mix of reads and writes."""
    sim = current_simulator()
    for operation_number in range(config.operations_per_client):
        await sleep(sim.rng.uniform(0.0, config.think_time))
        key = f"key{sim.rng.randrange(config.keys)}"

        if sim.rng.random() < config.read_fraction:
            await client.get(key)
        else:
            # Values are unique per client and per operation, so a value seen
            # by a reader identifies exactly which write produced it.
            await client.put(key, f"{client.name}#{operation_number}")


async def _injector(net: Network, events: Iterable[FaultEvent]) -> None:
    """Apply scheduled network faults at their appointed virtual times."""
    previous = 0.0
    for at, kind, payload in sorted(events, key=lambda event: event[0]):
        await sleep(max(0.0, at - previous))
        previous = at
        if kind == "partition":
            net.partition(payload)
        elif kind == "heal":
            net.heal()
        elif kind == "crash":
            net.crash(payload)
        elif kind == "restart":
            net.restart(payload)
        else:
            raise ValueError(f"unknown fault {kind!r}")


def run(
    seed: int,
    config: ClusterConfig | None = None,
    faults: FaultConfig | None = None,
    events: Iterable[FaultEvent] = (),
) -> RunResult:
    """Build a cluster, run the workload, and return everything observable."""
    config = config or ClusterConfig()
    sim = Simulator(seed=seed)
    net = Network(sim, faults or FaultConfig.perfect())

    replica_names = [f"r{i}" for i in range(config.replicas)]
    client_names = [f"c{i}" for i in range(config.clients)]
    for name in replica_names + client_names:
        net.add_node(name)

    replicas = [Replica(name, net) for name in replica_names]
    history: list[dict[str, Any]] = []
    clients = [
        Client(
            name=name,
            index=index,
            net=net,
            replicas=replica_names,
            write_quorum=config.write_quorum,
            read_quorum=config.read_quorum,
            timeout=config.timeout,
            history=history,
            read_repair=config.read_repair,
            count_distinct_replicas=config.count_distinct_replicas,
        )
        for index, name in enumerate(client_names)
    ]

    with running(sim):
        for replica in replicas:
            sim.spawn(replica.serve(), name=replica.name)
        for client in clients:
            sim.spawn(_workload(client, config), name=client.name)
        if events:
            sim.spawn(_injector(net, events), name="injector")
        sim.run()

    # History is appended in completion order across interleaved clients; sort
    # it so downstream consumers see a stable, time-ordered record.
    history.sort(key=lambda entry: (entry["invoked_ns"], entry["process"]))

    return RunResult(
        seed=seed,
        history=history,
        trace=sim.trace,
        network=net.stats(),
        snapshots={replica.name: replica.snapshot() for replica in replicas},
        config=config,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--faults", choices=["perfect", "realistic", "hostile"], default="perfect")
    parser.add_argument("--partition", action="store_true", help="split the cluster 3/2 midway")
    parser.add_argument("--trace", action="store_true")
    parser.add_argument("--check", action="store_true", help="run the linearizability checker")
    args = parser.parse_args()

    profiles = {
        "perfect": FaultConfig.perfect,
        "realistic": FaultConfig.realistic,
        "hostile": FaultConfig.hostile,
    }

    events: list[FaultEvent] = []
    if args.partition:
        events = [
            (0.4, "partition", [{"r0", "r1", "r2", "c0", "c1"}, {"r3", "r4", "c2", "c3", "c4"}]),
            (1.2, "heal", None),
        ]

    result = run(args.seed, faults=profiles[args.faults](), events=events)

    if args.trace:
        for line in result.trace:
            print(line)
        print()

    print(result.summary())
    print()
    for name in sorted(result.snapshots):
        rendered = {k: v[0] for k, v in sorted(result.snapshots[name].items())}
        print(f"  {name}: {rendered}")

    if args.check:
        from hourglass.checker import check
        from hourglass.history import History

        history = History.from_records(result.history)
        report = check(history)
        print()
        print(f"  {history.summary()}")
        print(f"  linearizable: {report.verdict}")
        print()
        print(report.render())


if __name__ == "__main__":
    main()
