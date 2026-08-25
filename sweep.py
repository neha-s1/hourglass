"""Search thousands of universes for one that breaks the database.

Each seed is a complete, self-contained run: a workload, a set of message
delays, and a network disaster, all derived from that one integer. The sweep
plays each of them out, hands the resulting history to the linearizability
checker, and writes down the seeds where the answer came back *impossible*.

    python sweep.py --seeds 2000 --faults hostile      # hunt
    python sweep.py --seed 1337                        # reproduce one
    python sweep.py --seed 1337 --shrink               # cut it down

A found seed is a reproduction. A *shrunk* seed is an explanation: the same
failure with every unnecessary client, operation, key and network fault
stripped away, usually leaving three or four operations and a single
partition.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path

from hourglass.checker import Verdict, check
from hourglass.faults import FaultConfig
from hourglass.history import History
from hourglass.scenarios import FaultEvent, ScenarioConfig, describe, scenario_for_seed
from hourglass.shrink import Budget, minimise_count, minimise_sequence

from examples.kvstore.cluster import ClusterConfig, RunResult, run

FAILURE_DIR = Path(__file__).parent / "failures"

PROFILES = {
    "perfect": FaultConfig.perfect,
    "realistic": FaultConfig.realistic,
    "hostile": FaultConfig.hostile,
}


def node_names(config: ClusterConfig) -> list[str]:
    return [f"r{i}" for i in range(config.replicas)] + [f"c{i}" for i in range(config.clients)]


@dataclass
class Trial:
    """One seed, played out and judged."""

    seed: int
    verdict: Verdict
    result: RunResult
    events: list[FaultEvent]
    report: object

    @property
    def broke(self) -> bool:
        return self.verdict is Verdict.VIOLATION


def play(
    seed: int,
    config: ClusterConfig,
    faults: FaultConfig,
    events: list[FaultEvent] | None = None,
) -> Trial:
    """Run one seed and check the history it produced."""
    if events is None:
        events = scenario_for_seed(seed, node_names(config))
    result = run(seed, config=config, faults=faults, events=events)
    report = check(History.from_records(result.history))
    return Trial(seed=seed, verdict=report.verdict, result=result, events=events, report=report)


def broke(seed: int, config: ClusterConfig, faults: FaultConfig, events: list[FaultEvent]) -> bool:
    """The question the shrinker asks, over and over."""
    return play(seed, config, faults, events).broke


# ---------------------------------------------------------------------------
# Shrinking
# ---------------------------------------------------------------------------


@dataclass
class Shrunk:
    seed: int
    config: ClusterConfig
    events: list[FaultEvent]
    tests_run: int

    def command(self, profile: str) -> str:
        return (
            f"python sweep.py --seed {self.seed} --faults {profile} "
            f"--clients {self.config.clients} --ops {self.config.operations_per_client} "
            f"--keys {self.config.keys}"
        )


def shrink(
    seed: int,
    config: ClusterConfig,
    faults: FaultConfig,
    events: list[FaultEvent],
    limit: int = 400,
) -> Shrunk:
    """Strip a failing run down to the smallest version that still fails.

    Faults go first: they are the part a reader most wants to understand, and
    removing one rarely disturbs the rest. The workload is narrowed after,
    smallest-first, because shrinking a simulation is not monotone -- fewer
    operations means a *different* run, not merely a shorter one, and it may
    simply miss the bug.
    """
    budget = Budget(limit)

    events = minimise_sequence(events, lambda candidate: broke(seed, config, faults, candidate), budget)

    operations = minimise_count(
        config.operations_per_client,
        1,
        lambda n: broke(seed, replace(config, operations_per_client=n), faults, events),
        budget,
    )
    config = replace(config, operations_per_client=operations)

    clients = minimise_count(
        config.clients,
        2,
        lambda n: broke(seed, replace(config, clients=n), faults, events),
        budget,
    )
    config = replace(config, clients=clients)

    keys = minimise_count(
        config.keys,
        1,
        lambda n: broke(seed, replace(config, keys=n), faults, events),
        budget,
    )
    config = replace(config, keys=keys)

    return Shrunk(seed=seed, config=config, events=events, tests_run=budget.used)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def render_failure(trial: Trial) -> str:
    lines = [f"FAILURE  seed={trial.seed}", f"  scenario: {describe(trial.events)}"]
    history = History.from_records(trial.result.history)
    lines.append(f"  history:  {history.summary()}")
    lines.append("")
    for key_report in trial.report.violations:
        lines.append(f"  key {key_report.key!r} -- no ordering of these operations is possible:")
        for op in key_report.witness:
            end = "pending" if op.pending else f"{op.returned_ns / 1e6:.2f}"
            window = f"[{op.invoked_ns / 1e6:>8.2f} ->{end:>9} ]ms"
            marker = "   <-- impossible" if op is key_report.witness[-1] else ""
            lines.append(f"    {window}  {op.describe()}{marker}")
    return "\n".join(lines)


def save_failure(trial: Trial, profile: str) -> Path:
    FAILURE_DIR.mkdir(exist_ok=True)
    path = FAILURE_DIR / f"seed-{trial.seed}.txt"
    path.write_text(render_failure(trial) + "\n")
    (FAILURE_DIR / f"seed-{trial.seed}.json").write_text(
        json.dumps(
            {
                "seed": trial.seed,
                "faults": profile,
                "scenario": describe(trial.events),
                "keys": [k.key for k in trial.report.violations],
            },
            indent=2,
        )
    )
    return path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", type=int, default=1000, help="how many seeds to sweep")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--seed", type=int, help="run one seed instead of sweeping")
    parser.add_argument("--shrink", action="store_true", help="minimise a failing seed")
    parser.add_argument("--faults", choices=sorted(PROFILES), default="hostile")
    parser.add_argument("--clients", type=int, default=5)
    parser.add_argument("--ops", type=int, default=20)
    parser.add_argument("--keys", type=int, default=3)
    parser.add_argument(
        "--broken",
        action="store_true",
        help="switch both known bugs back on (no read repair, count messages not replicas)",
    )
    parser.add_argument("--save", action="store_true", help="write failures/ files")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument(
        "--fail-on-violation",
        action="store_true",
        help="exit non-zero if any seed violates -- the regression gate",
    )
    parser.add_argument(
        "--expect-violations",
        action="store_true",
        help="exit non-zero if NO seed violates -- proves the checker still detects",
    )
    args = parser.parse_args()

    config = ClusterConfig(
        clients=args.clients,
        operations_per_client=args.ops,
        keys=args.keys,
        read_repair=not args.broken,
        count_distinct_replicas=not args.broken,
    )
    faults = PROFILES[args.faults]()

    # -- one seed ----------------------------------------------------------
    if args.seed is not None:
        trial = play(args.seed, config, faults)
        if not trial.broke:
            print(f"seed {args.seed}: {trial.verdict} -- nothing to see")
            return

        print(render_failure(trial))
        if args.save:
            print(f"\n  saved to {save_failure(trial, args.faults)}")

        if args.shrink:
            print("\n  shrinking...")
            started = time.perf_counter()
            small = shrink(args.seed, config, faults, trial.events)
            elapsed = time.perf_counter() - started

            before_faults = len([e for e in trial.events if e[1] in ("partition", "crash")])
            after_faults = len([e for e in small.events if e[1] in ("partition", "crash")])
            print(
                f"\n  before: {config.clients} clients x {config.operations_per_client} ops, "
                f"{config.keys} keys, {before_faults} faults "
                f"({config.total_operations} operations)"
            )
            print(
                f"  after:  {small.config.clients} clients x {small.config.operations_per_client} ops, "
                f"{small.config.keys} keys, {after_faults} faults "
                f"({small.config.total_operations} operations)"
            )
            print(f"  {small.tests_run} runs in {elapsed:.1f}s")
            print(f"\n  minimal scenario: {describe(small.events)}")
            print(f"  reproduce with:   {small.command(args.faults)}")

            minimal = play(args.seed, small.config, faults, small.events)
            print()
            print(render_failure(minimal))
        return

    # -- sweep -------------------------------------------------------------
    seeds = range(args.start, args.start + args.seeds)
    started = time.perf_counter()
    failures: list[Trial] = []
    unknown = 0

    for seed in seeds:
        trial = play(seed, config, faults)
        if trial.broke:
            failures.append(trial)
            if not args.quiet:
                keys = ", ".join(k.key for k in trial.report.violations)
                print(f"  seed {seed:>6}  violation on {keys}")
            if args.save:
                save_failure(trial, args.faults)
        elif trial.verdict is Verdict.UNKNOWN:
            unknown += 1

    elapsed = time.perf_counter() - started
    print()
    print(f"swept {args.seeds} seeds in {elapsed:.1f}s ({elapsed / args.seeds * 1000:.1f}ms each)")
    print(f"  faults:     {args.faults} ({faults.summary()})")
    print(f"  workload:   {config.clients} clients x {config.operations_per_client} ops on {config.keys} keys")
    print(f"  violations: {len(failures)} ({len(failures) / args.seeds:.1%})")
    if unknown:
        print(f"  undecided:  {unknown} (search budget exhausted)")
    if failures:
        print(f"  seeds:      {[t.seed for t in failures][:25]}")

    if args.fail_on_violation and failures:
        print(f"\nFAILED: {len(failures)} seeds produced non-linearizable histories")
        sys.exit(1)

    if args.expect_violations and not failures:
        print("\nFAILED: expected the known bugs to be detected, found nothing")
        sys.exit(1)


if __name__ == "__main__":
    main()
