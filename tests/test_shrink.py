"""Delta debugging, scenario generation, and the end-to-end bug hunt."""

from __future__ import annotations

import pytest

from hourglass.faults import FaultConfig
from hourglass.scenarios import ScenarioConfig, describe, scenario_for_seed
from hourglass.shrink import Budget, minimise_count, minimise_sequence

from examples.kvstore.cluster import ClusterConfig

NODES = [f"r{i}" for i in range(5)] + [f"c{i}" for i in range(5)]


# ---------------------------------------------------------------------------
# The sequence minimiser
# ---------------------------------------------------------------------------


def test_it_finds_the_single_element_that_matters() -> None:
    items = list(range(50))
    result = minimise_sequence(items, lambda candidate: 37 in candidate)
    assert result == [37]


def test_it_finds_a_pair_that_must_appear_together() -> None:
    items = list(range(40))
    result = minimise_sequence(items, lambda c: 3 in c and 31 in c)
    assert result == [3, 31]


def test_it_preserves_order() -> None:
    items = ["a", "b", "c", "d", "e", "f", "g", "h"]
    result = minimise_sequence(items, lambda c: "b" in c and "g" in c)
    assert result == ["b", "g"]


def test_it_returns_the_input_when_everything_is_needed() -> None:
    items = list(range(8))
    result = minimise_sequence(items, lambda c: len(c) == 8)
    assert result == items


def test_it_never_returns_something_that_passes() -> None:
    items = list(range(30))
    predicate = lambda c: sum(c) > 100  # noqa: E731
    result = minimise_sequence(items, predicate)
    assert predicate(result)


def test_it_respects_its_budget() -> None:
    calls = []

    def counting(candidate):
        calls.append(len(candidate))
        return 5 in candidate

    budget = Budget(4)
    minimise_sequence(list(range(100)), counting, budget)
    assert len(calls) <= 4
    assert budget.used <= 4


def test_an_empty_input_is_returned_unchanged() -> None:
    assert minimise_sequence([], lambda c: True) == []


# ---------------------------------------------------------------------------
# The count minimiser
# ---------------------------------------------------------------------------


def test_count_minimiser_finds_a_smaller_value() -> None:
    assert minimise_count(100, 1, lambda n: n >= 4) == 4


def test_count_minimiser_keeps_the_original_when_nothing_smaller_works() -> None:
    assert minimise_count(20, 1, lambda n: False) == 20


def test_count_minimiser_respects_the_floor() -> None:
    assert minimise_count(50, 8, lambda n: True) == 8


def test_count_minimiser_does_not_assume_monotonicity() -> None:
    """Shrinking a simulation is not monotone; only actual failures count."""
    assert minimise_count(64, 1, lambda n: n == 16) == 16


# ---------------------------------------------------------------------------
# Scenario generation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [0, 1, 17, 91, 1234])
def test_a_scenario_is_a_function_of_its_seed(seed: int) -> None:
    assert describe(scenario_for_seed(seed, NODES)) == describe(scenario_for_seed(seed, NODES))


def test_different_seeds_give_different_disasters() -> None:
    rendered = {describe(scenario_for_seed(seed, NODES)) for seed in range(30)}
    assert len(rendered) > 20


def test_events_are_ordered_in_time() -> None:
    for seed in range(20):
        times = [at for at, _kind, _payload in scenario_for_seed(seed, NODES)]
        assert times == sorted(times)


def test_partitions_never_isolate_a_single_node() -> None:
    config = ScenarioConfig(min_group=2)
    for seed in range(40):
        for _at, kind, payload in scenario_for_seed(seed, NODES, config):
            if kind == "partition":
                assert all(len(group) >= 2 for group in payload)


def test_every_partition_is_eventually_healed() -> None:
    for seed in range(30):
        events = scenario_for_seed(seed, NODES)
        splits = sum(1 for _at, kind, _p in events if kind == "partition")
        heals = sum(1 for _at, kind, _p in events if kind == "heal")
        assert heals == splits


def test_every_crash_is_eventually_restarted() -> None:
    for seed in range(30):
        events = scenario_for_seed(seed, NODES)
        crashes = sum(1 for _at, kind, _p in events if kind == "crash")
        restarts = sum(1 for _at, kind, _p in events if kind == "restart")
        assert restarts == crashes


# ---------------------------------------------------------------------------
# The hunt, end to end
# ---------------------------------------------------------------------------


def test_the_sweep_finds_violations_under_hostile_faults() -> None:
    from sweep import play

    config, faults = ClusterConfig(), FaultConfig.hostile()
    broken = [seed for seed in range(80) if play(seed, config, faults).broke]
    assert len(broken) >= 5, f"expected several failures, found {broken}"


def test_a_perfect_network_produces_no_violations() -> None:
    """The hunt must not cry wolf when nothing is wrong."""
    from sweep import play

    config, faults = ClusterConfig(), FaultConfig.perfect()
    for seed in range(40):
        assert not play(seed, config, faults, events=[]).broke


@pytest.mark.parametrize("seed", [0, 1, 17])
def test_a_known_failing_seed_reproduces_exactly(seed: int) -> None:
    from sweep import play

    config, faults = ClusterConfig(), FaultConfig.hostile()
    first, second = play(seed, config, faults), play(seed, config, faults)

    assert first.broke and second.broke
    assert [k.key for k in first.report.violations] == [k.key for k in second.report.violations]
    assert first.result.history == second.result.history


def test_shrinking_never_makes_a_case_larger() -> None:
    from sweep import shrink

    config, faults = ClusterConfig(), FaultConfig.hostile()
    events = scenario_for_seed(1, NODES)
    small = shrink(1, config, faults, events, limit=120)

    assert len(small.events) <= len(events)
    assert small.config.total_operations <= config.total_operations
    assert small.config.clients >= 2
    assert small.config.keys >= 1


def test_a_shrunk_case_still_fails() -> None:
    """The whole point: the smaller reproduction must reproduce."""
    from sweep import broke, shrink

    config, faults = ClusterConfig(), FaultConfig.hostile()
    events = scenario_for_seed(0, NODES)
    small = shrink(0, config, faults, events, limit=120)

    assert broke(0, small.config, faults, small.events)
