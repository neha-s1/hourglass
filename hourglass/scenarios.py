"""Randomised network disasters, generated from a seed.

A single hand-written partition finds a single shape of bug. Real failures
arrive at inconvenient moments, split the cluster in uneven ways, and overlap
with machines rebooting. Searching that space means generating the disaster
from the seed too, so the seed continues to describe the entire run: the
workload, the message timing, *and* what went wrong.

The generator uses its own random stream, derived from the seed but separate
from the simulator's. That keeps the two independent -- adding a partition
does not shift every subsequent latency draw, so scenarios stay comparable
while shrinking removes faults one at a time.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any

#: A scheduled fault: (seconds, kind, payload).
FaultEvent = tuple[float, str, Any]

#: Mixed into the seed so the scenario stream is independent of the
#: simulator's. Any odd constant with well-spread bits works; this is the
#: golden-ratio constant used by a lot of hash mixers.
_SCENARIO_SALT = 0x9E3779B97F4A7C15


@dataclass(frozen=True)
class ScenarioConfig:
    """How much havoc to generate."""

    max_partitions: int = 2
    max_crashes: int = 1
    window: float = 1.2
    min_outage: float = 0.05
    max_outage: float = 0.50
    #: Never isolate fewer than this many nodes on either side of a split.
    min_group: int = 2


def scenario_for_seed(
    seed: int,
    nodes: list[str],
    config: ScenarioConfig | None = None,
) -> list[FaultEvent]:
    """Build a deterministic fault schedule for ``seed``."""
    config = config or ScenarioConfig()
    rng = random.Random(seed ^ _SCENARIO_SALT)
    events: list[FaultEvent] = []

    ordered = sorted(nodes)

    for _ in range(rng.randint(1, max(1, config.max_partitions))):
        at = rng.uniform(0.0, config.window)
        shuffled = list(ordered)
        rng.shuffle(shuffled)
        low = config.min_group
        high = len(shuffled) - config.min_group
        if high < low:
            continue
        cut = rng.randint(low, high)
        groups = [set(shuffled[:cut]), set(shuffled[cut:])]
        events.append((at, "partition", groups))
        events.append((at + rng.uniform(config.min_outage, config.max_outage), "heal", None))

    replicas = [node for node in ordered if node.startswith("r")]
    for _ in range(rng.randint(0, max(0, config.max_crashes))):
        if not replicas:
            break
        node = rng.choice(replicas)
        at = rng.uniform(0.0, config.window)
        events.append((at, "crash", node))
        events.append((at + rng.uniform(config.min_outage, config.max_outage), "restart", node))

    return sorted(events, key=lambda event: event[0])


def describe(events: list[FaultEvent]) -> str:
    """A one-line human summary of a fault schedule."""
    if not events:
        return "no faults"
    parts = []
    for at, kind, payload in events:
        if kind == "partition":
            rendered = " | ".join("{" + ",".join(sorted(group)) + "}" for group in payload)
            parts.append(f"{at:.3f}s split {rendered}")
        elif kind == "heal":
            parts.append(f"{at:.3f}s heal")
        else:
            parts.append(f"{at:.3f}s {kind} {payload}")
    return "; ".join(parts)
