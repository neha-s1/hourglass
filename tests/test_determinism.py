"""The property everything else rests on: a seed is a universe.

If these tests fail, nothing else in Hourglass means anything -- a bug found
under a seed would not be reproducible, which is the entire point of the tool.
"""

from __future__ import annotations

import time

import pytest

from hourglass.demo import EXPECTED, simulate
from hourglass.runtime import NANOS_PER_SECOND, Simulator, running, sleep, yield_now

SEEDS = [0, 1, 7, 42, 225, 999, 31337]


# ---------------------------------------------------------------------------
# The core property
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_same_seed_produces_identical_trace(seed: int) -> None:
    first_result, first_trace = simulate(seed)
    second_result, second_trace = simulate(seed)

    assert first_trace == second_trace
    assert first_result == second_result


@pytest.mark.parametrize("seed", SEEDS)
def test_same_seed_is_stable_across_many_runs(seed: int) -> None:
    """Not just twice -- the tenth run must match the first exactly."""
    baseline, baseline_trace = simulate(seed)
    for _ in range(9):
        result, trace = simulate(seed)
        assert result == baseline
        assert trace == baseline_trace


def test_different_seeds_explore_different_interleavings() -> None:
    """A seed that changed nothing would make the search useless."""
    traces = {seed: tuple(simulate(seed)[1]) for seed in range(40)}
    assert len(set(traces.values())) > 1, "every seed produced the same trace"


def test_seeds_produce_a_range_of_outcomes() -> None:
    """The interleaving must reach the program's state, not just its log."""
    outcomes = {simulate(seed)[0] for seed in range(1000)}
    assert len(outcomes) > 1, "the seed never changed the result"


# ---------------------------------------------------------------------------
# A known bug, pinned
# ---------------------------------------------------------------------------


def test_seed_225_always_loses_exactly_one_increment() -> None:
    """The whole promise of the tool, as a regression test.

    Seed 225 is the first seed under 1000 whose interleaving loses an update.
    If this ever stops failing in exactly this way, either the runtime lost
    determinism or the demo changed.
    """
    for _ in range(5):
        assert simulate(225)[0] == EXPECTED - 1


def test_seed_224_is_clean() -> None:
    assert simulate(224)[0] == EXPECTED


# ---------------------------------------------------------------------------
# Virtual time
# ---------------------------------------------------------------------------


def test_simulated_waiting_costs_no_real_time() -> None:
    """A simulated day should run in milliseconds."""
    sim = Simulator(seed=1)

    async def patient() -> None:
        await sleep(86_400.0)  # one day

    started = time.perf_counter()
    with running(sim):
        sim.spawn(patient(), name="patient")
        sim.run()
    elapsed = time.perf_counter() - started

    assert sim.now_ns == 86_400 * NANOS_PER_SECOND
    assert elapsed < 0.5, f"a simulated day took {elapsed:.3f}s of real time"


def test_clock_only_moves_forward() -> None:
    sim = Simulator(seed=3)
    readings: list[int] = []

    async def sampler(name: str) -> None:
        for _ in range(20):
            readings.append(sim.now_ns)
            await sleep(sim.rng.uniform(0, 1.0))
            await yield_now()

    with running(sim):
        for i in range(4):
            sim.spawn(sampler(f"s{i}"), name=f"s{i}")
        sim.run()

    assert readings == sorted(readings), "the virtual clock went backwards"


def test_clock_starts_at_zero() -> None:
    assert Simulator(seed=0).now_ns == 0


# ---------------------------------------------------------------------------
# Scheduler mechanics
# ---------------------------------------------------------------------------


def test_all_tasks_run_to_completion() -> None:
    sim = Simulator(seed=11)
    finished: list[str] = []

    async def job(name: str) -> None:
        await sleep(0.01)
        finished.append(name)

    with running(sim):
        for i in range(5):
            sim.spawn(job(f"job-{i}"), name=f"job-{i}")
        sim.run()

    assert sorted(finished) == [f"job-{i}" for i in range(5)]
    assert sim.unfinished() == []


def test_task_returning_a_value_records_its_result() -> None:
    sim = Simulator(seed=5)

    async def answer() -> int:
        await sleep(0.1)
        return 42

    with running(sim):
        tid = sim.spawn(answer(), name="answer")
        sim.run()

    assert sim.task(tid).result == 42


def test_task_raising_is_recorded_not_swallowed() -> None:
    sim = Simulator(seed=5)

    async def boom() -> None:
        await sleep(0.1)
        raise ValueError("intentional")

    with running(sim):
        sim.spawn(boom(), name="boom")
        sim.run()

    errors = sim.errors()
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)


def test_unknown_request_is_rejected_loudly() -> None:
    sim = Simulator(seed=5)

    class Mystery:
        pass

    async def confused() -> None:
        from hourglass.runtime import Suspend

        await Suspend(Mystery())

    with running(sim):
        sim.spawn(confused(), name="confused")
        with pytest.raises(TypeError, match="unknown request"):
            sim.run()
