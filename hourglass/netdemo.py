"""Watch a network split, and watch it heal.

Alice pings Bob once every 100ms and waits 60ms for a pong. Half a second in,
the network partitions and the two of them can no longer reach each other.
A second after that it heals.

    python -m hourglass.netdemo --seed 7

The interesting part is not that pings fail during the partition -- of course
they do. It is that they fail at *exactly* the same rounds every single run,
because the partition, the latencies, and the scheduling are all downstream of
one integer.
"""

from __future__ import annotations

import argparse

from hourglass.faults import FaultConfig
from hourglass.network import Network, recv
from hourglass.runtime import Simulator, current_simulator, running, sleep

ROUNDS = 25
PING_INTERVAL = 0.1
PONG_TIMEOUT = 0.06
PARTITION_AT = 0.5
HEAL_AT = 1.5


async def alice(net: Network, results: list[tuple[int, bool]]) -> None:
    sim = current_simulator()
    for round_number in range(ROUNDS):
        net.send("alice", "bob", ("ping", round_number))
        reply = await recv("alice", timeout=PONG_TIMEOUT)
        ok = reply is not None
        results.append((round_number, ok))
        sim.log("result", f"ping {round_number:>2} {'pong' if ok else 'NO REPLY'}")
        await sleep(PING_INTERVAL)


async def bob(net: Network) -> None:
    while True:
        message = await recv("bob")
        if message is None:
            continue
        kind, number = message.payload
        if kind == "ping":
            net.send("bob", "alice", ("pong", number))


async def splitter(net: Network) -> None:
    await sleep(PARTITION_AT)
    net.partition([{"alice"}, {"bob"}])
    await sleep(HEAL_AT - PARTITION_AT)
    net.heal()


PROFILES = {
    "perfect": FaultConfig.perfect,
    "realistic": FaultConfig.realistic,
    "hostile": FaultConfig.hostile,
}


def simulate(
    seed: int, profile: str = "perfect"
) -> tuple[list[tuple[int, bool]], list[str], dict[str, int]]:
    sim = Simulator(seed=seed)
    net = Network(sim, PROFILES[profile]())
    net.add_node("alice")
    net.add_node("bob")
    results: list[tuple[int, bool]] = []

    with running(sim):
        sim.spawn(bob(net), name="bob")
        sim.spawn(alice(net, results), name="alice")
        sim.spawn(splitter(net), name="splitter")
        sim.run()

    return results, sim.trace, net.stats()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--trace", action="store_true", help="print the full trace")
    parser.add_argument(
        "--faults",
        choices=sorted(PROFILES),
        default="perfect",
        help="how badly the network misbehaves outside the partition",
    )
    args = parser.parse_args()

    results, trace, stats = simulate(args.seed, args.faults)

    if args.trace:
        for line in trace:
            print(line)
        print()

    print(f"seed={args.seed}  faults={args.faults}  ({PROFILES[args.faults]().summary()})")
    print(f"partition {PARTITION_AT}s -> {HEAL_AT}s\n")
    line = "".join("." if ok else "X" for _, ok in results)
    print(f"  round  0{' ' * (ROUNDS - 8)}{ROUNDS - 1}")
    print(f"         {line}      ( . = pong,  X = no reply )")

    failed = [n for n, ok in results if not ok]
    print(f"\n  {len(failed)} of {ROUNDS} pings unanswered: rounds {failed}")
    print(f"  network: {stats}")


if __name__ == "__main__":
    main()
