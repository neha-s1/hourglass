"""A race condition that hides -- and a seed that makes it stop hiding.

Three workers each increment a shared counter three times. Each increment
reads the value, pauses briefly, then writes back what it read plus one: the
classic read-modify-write race. If a second worker reads before the first
writes, one increment is silently lost and the counter ends below nine.

The pause between read and write is deliberately tiny compared to the idle
time between increments, so the workers rarely overlap. About 8 runs in 1000
lose an increment. That is exactly the shape of the bugs that ruin weeks in
real systems: too rare to catch, impossible to reproduce on demand.

Except here it is perfectly reproducible, because the run is a pure function
of the seed::

    python -m hourglass.demo --seed 224     # fine
    python -m hourglass.demo --seed 225     # loses an increment, always
    python -m hourglass.demo --scan 1000    # find every bad seed in a range

Run seed 225 a thousand times, on any machine, and it fails identically a
thousand times.
"""

from __future__ import annotations

import argparse

from hourglass.runtime import Simulator, current_simulator, running, sleep, yield_now

WORKERS = 3
INCREMENTS_PER_WORKER = 3
EXPECTED = WORKERS * INCREMENTS_PER_WORKER

# Idle time between one increment and the next. Large, so workers spend most
# of their time apart and collisions are uncommon.
THINK_TIME_MAX = 0.2

# The dangerous window: how long a worker holds a value it has read before
# writing it back. Small, so overlaps are rare -- which is what makes the bug
# so hard to catch without a tool like this one.
CRITICAL_WINDOW_MAX = 0.0002


async def worker(name: str, state: dict[str, int]) -> None:
    sim = current_simulator()
    for _ in range(INCREMENTS_PER_WORKER):
        # Both durations are drawn from the simulator's seeded RNG, so the
        # whole run -- workload timing included -- is a function of the seed.
        await sleep(sim.rng.uniform(0.0, THINK_TIME_MAX))

        seen = state["counter"]
        sim.log("read", f"{name} sees counter={seen}")

        await yield_now()
        await sleep(sim.rng.uniform(0.0, CRITICAL_WINDOW_MAX))

        state["counter"] = seen + 1
        sim.log("write", f"{name} sets counter={seen + 1}")


def simulate(seed: int) -> tuple[int, list[str]]:
    """Run the demo under one seed. Returns the final counter and the trace."""
    sim = Simulator(seed=seed)
    state = {"counter": 0}
    with running(sim):
        for i in range(WORKERS):
            sim.spawn(worker(f"worker-{i}", state), name=f"worker-{i}")
        sim.run()
    return state["counter"], sim.trace


def scan(limit: int) -> list[int]:
    """Return every seed below ``limit`` that loses at least one increment."""
    return [seed for seed in range(limit) if simulate(seed)[0] != EXPECTED]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=225)
    parser.add_argument("--quiet", action="store_true", help="hide the trace")
    parser.add_argument(
        "--scan", type=int, metavar="N", help="test seeds 0..N-1 and list the failures"
    )
    args = parser.parse_args()

    if args.scan:
        bad = scan(args.scan)
        rate = len(bad) / args.scan
        print(f"scanned {args.scan} seeds, {len(bad)} lose an increment ({rate:.1%})")
        print(f"failing seeds: {bad}")
        return

    final, trace = simulate(args.seed)

    if not args.quiet:
        for line in trace:
            print(line)
        print()

    lost = EXPECTED - final
    verdict = "OK" if lost == 0 else f"LOST {lost} INCREMENT(S)"
    print(f"seed={args.seed}  counter={final}  expected={EXPECTED}  -> {verdict}")


if __name__ == "__main__":
    main()
