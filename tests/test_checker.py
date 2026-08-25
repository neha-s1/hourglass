"""The linearizability checker.

Two kinds of test here. The hand-written histories pin down what the checker
believes, independently of the simulator -- including the awkward cases where
the correct answer is "that is allowed", which is where a naive checker cries
wolf. The cluster tests confirm it says nothing is wrong when nothing is.
"""

from __future__ import annotations

import pytest

from hourglass.checker import DEFAULT_BUDGET, Verdict, check, check_key
from hourglass.history import History

from examples.kvstore.cluster import run


def rec(process, op, key, t0, t1, ok=True, value=None, result=None):
    return dict(
        process=process,
        op=op,
        key=key,
        invoked_ns=t0,
        returned_ns=t1,
        ok=ok,
        value=value,
        result=result,
    )


def verdict(records) -> Verdict:
    return check(History.from_records(records)).verdict


# ---------------------------------------------------------------------------
# Histories that are fine
# ---------------------------------------------------------------------------


def test_empty_history_is_linearizable() -> None:
    assert verdict([]) is Verdict.OK


def test_write_then_read_it_back() -> None:
    assert (
        verdict(
            [
                rec("c0", "put", "k", 0, 10, value="a"),
                rec("c0", "get", "k", 20, 30, result="a"),
            ]
        )
        is Verdict.OK
    )


def test_reading_an_unwritten_key_returns_nothing() -> None:
    assert verdict([rec("c0", "get", "k", 0, 10, result=None)]) is Verdict.OK


def test_overlapping_operations_may_be_ordered_either_way() -> None:
    """A read that overlaps a write may see the old value or the new one.

    This is the case a naive checker gets wrong, by assuming the write
    happened at the moment it was issued.
    """
    before = [
        rec("c0", "put", "k", 0, 100, value="a"),
        rec("c1", "get", "k", 10, 20, result=None),
    ]
    after = [
        rec("c0", "put", "k", 0, 100, value="a"),
        rec("c1", "get", "k", 10, 20, result="a"),
    ]
    assert verdict(before) is Verdict.OK
    assert verdict(after) is Verdict.OK


def test_a_timed_out_write_may_have_landed() -> None:
    assert (
        verdict(
            [
                rec("c0", "put", "k", 0, 10, ok=False, value="a"),
                rec("c1", "get", "k", 20, 30, result="a"),
            ]
        )
        is Verdict.OK
    )


def test_a_timed_out_write_may_equally_have_vanished() -> None:
    assert (
        verdict(
            [
                rec("c0", "put", "k", 0, 10, ok=False, value="a"),
                rec("c1", "get", "k", 20, 30, result=None),
            ]
        )
        is Verdict.OK
    )


def test_a_timed_out_write_may_land_much_later() -> None:
    """A pending write has no deadline; its effect can surface at any point."""
    assert (
        verdict(
            [
                rec("c0", "put", "k", 0, 10, ok=False, value="late"),
                rec("c1", "get", "k", 20, 30, result=None),
                rec("c1", "get", "k", 40, 50, result="late"),
            ]
        )
        is Verdict.OK
    )


def test_a_read_that_timed_out_constrains_nothing() -> None:
    """It never learned a value, so it cannot contradict anything."""
    assert (
        verdict(
            [
                rec("c0", "put", "k", 0, 10, value="a"),
                rec("c1", "get", "k", 20, 30, ok=False, result=None),
                rec("c1", "get", "k", 40, 50, result="a"),
            ]
        )
        is Verdict.OK
    )


def test_keys_are_checked_independently() -> None:
    """Nonsense on one key must not be excused by another key's operations."""
    records = [
        rec("c0", "put", "x", 0, 10, value="a"),
        rec("c0", "put", "y", 0, 10, value="b"),
        rec("c1", "get", "x", 20, 30, result="a"),
        rec("c1", "get", "y", 20, 30, result="b"),
    ]
    assert verdict(records) is Verdict.OK


# ---------------------------------------------------------------------------
# Histories that are not fine
# ---------------------------------------------------------------------------


def test_a_value_cannot_travel_backwards() -> None:
    """Once a value is observed, an older one may not reappear."""
    assert (
        verdict(
            [
                rec("c0", "put", "k", 0, 10, value="a"),
                rec("c1", "get", "k", 20, 30, result="a"),
                rec("c2", "get", "k", 40, 50, result=None),
            ]
        )
        is Verdict.VIOLATION
    )


def test_a_read_cannot_invent_a_value() -> None:
    assert (
        verdict(
            [
                rec("c0", "put", "k", 0, 10, value="a"),
                rec("c1", "get", "k", 20, 30, result="ghost"),
            ]
        )
        is Verdict.VIOLATION
    )


def test_a_stale_read_after_a_fresh_one_is_a_violation() -> None:
    """The exact shape the partitioned cluster produces."""
    assert (
        verdict(
            [
                rec("c0", "put", "k", 0, 10, value="old"),
                rec("c0", "put", "k", 20, 30, value="new"),
                rec("c1", "get", "k", 40, 50, result="new"),
                rec("c2", "get", "k", 60, 70, result="old"),
            ]
        )
        is Verdict.VIOLATION
    )


def test_a_violation_on_one_key_condemns_the_history() -> None:
    records = [
        rec("c0", "put", "x", 0, 10, value="a"),
        rec("c1", "get", "x", 20, 30, result="a"),
        rec("c1", "get", "y", 40, 50, result="never-written"),
    ]
    assert verdict(records) is Verdict.VIOLATION


# ---------------------------------------------------------------------------
# The witness
# ---------------------------------------------------------------------------


def test_the_witness_is_a_minimal_impossible_subset() -> None:
    """Not merely the shortest failing prefix -- the smallest failing subset.

    Two operations suffice here: a write that was acknowledged, and a later
    read that saw nothing. The reads in between are irrelevant to the proof.
    """
    records = [
        rec("c0", "put", "k", 0, 10, value="a"),
        rec("c1", "get", "k", 20, 30, result="a"),
        rec("c2", "get", "k", 40, 50, result=None),
        rec("c3", "get", "k", 60, 70, result=None),
        rec("c4", "get", "k", 80, 90, result=None),
    ]
    (failure,) = check(History.from_records(records)).violations

    assert len(failure.witness) == 2, failure.render()
    assert failure.witness[0].kind == "put"
    assert failure.witness[-1].result is None


def test_the_witness_is_much_smaller_than_the_history() -> None:
    """A bug report of thirty operations is not a bug report."""
    records = [rec("c0", "put", "k", 0, 10, value="a")]
    records.append(rec("c1", "get", "k", 20, 30, result="a"))
    records.append(rec("c2", "get", "k", 40, 50, result=None))
    for i in range(30):
        t = 100 + i * 20
        records.append(rec("c3", "get", "k", t, t + 10, result=None))

    (failure,) = check(History.from_records(records)).violations
    assert len(failure.operations) == 33
    assert len(failure.witness) == 2


# ---------------------------------------------------------------------------
# Honesty about giving up
# ---------------------------------------------------------------------------


def test_running_out_of_budget_reports_unknown_not_violation() -> None:
    """Exhaustion must never be mistaken for a proof of impossibility."""
    records = []
    for i in range(40):
        records.append(rec(f"c{i % 5}", "put", "k", 0, 10_000, value=f"v{i}"))
    for i in range(40):
        records.append(rec(f"c{i % 5}", "get", "k", 0, 10_000, result=f"v{i}"))

    report = check(History.from_records(records), budget=50)
    assert report.verdict is Verdict.UNKNOWN
    assert not report.violations


def test_a_generous_budget_still_decides_ordinary_histories() -> None:
    assert check(History.from_records(run(3).history), budget=DEFAULT_BUDGET).verdict is Verdict.OK


# ---------------------------------------------------------------------------
# Against the real cluster
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [0, 1, 7, 42, 999])
def test_a_healthy_cluster_is_always_linearizable(seed: int) -> None:
    report = check(History.from_records(run(seed).history))
    assert report.verdict is Verdict.OK, report.render()


@pytest.mark.parametrize("seed", [0, 1, 7])
def test_checking_is_deterministic(seed: int) -> None:
    history = History.from_records(run(seed).history)
    first, second = check(history), check(history)
    assert first.verdict is second.verdict
    assert first.states_explored == second.states_explored


def test_history_splits_by_key_and_counts_concurrency() -> None:
    history = History.from_records(run(1).history)
    assert len(history) == 100
    assert history.keys() == ["key0", "key1", "key2"]
    assert sum(len(ops) for ops in history.by_key().values()) == 100
    assert 1 < history.concurrency() <= 5


def test_checking_a_hundred_operations_is_fast() -> None:
    import time

    history = History.from_records(run(2).history)
    started = time.perf_counter()
    check(history)
    assert time.perf_counter() - started < 1.0


def test_a_witness_never_reads_a_value_nothing_in_it_wrote() -> None:
    """Minimisation must leave an explanation, not just a proof.

    Deleting the write that produced a value makes the read of it
    unorderable for a trivial reason. That is sound but useless, so the
    minimiser refuses to remove a write some retained read depends on.
    """
    records = [
        rec("c0", "put", "k", 0, 10, value="first"),
        rec("c1", "put", "k", 20, 30, value="second"),
        rec("c2", "get", "k", 40, 50, result="second"),
        rec("c3", "get", "k", 60, 70, result="first"),
    ]
    (failure,) = check(History.from_records(records)).violations

    written = {op.value for op in failure.witness if op.kind == "put"}
    for op in failure.witness:
        if op.kind == "get" and op.result is not None:
            assert op.result in written, failure.render()


def test_witnesses_from_the_real_cluster_stay_small_and_explanatory() -> None:
    """Across many real failures, every witness must be readable and honest."""
    from hourglass.faults import FaultConfig
    from sweep import play

    from examples.kvstore.cluster import ClusterConfig

    checked = 0
    for seed in range(60):
        trial = play(seed, ClusterConfig(), FaultConfig.hostile())
        for key_report in trial.report.violations:
            checked += 1
            assert 1 < len(key_report.witness) <= 8, key_report.render()
            written = {op.value for op in key_report.witness if op.kind == "put"}
            for op in key_report.witness:
                if op.kind == "get" and op.result is not None:
                    assert op.result in written, key_report.render()
    assert checked >= 5, f"only {checked} violations found to inspect"
