"""Regression tests for the two bugs the hunt found.

Both were found by sweep.py, reproduced from a single seed, and shrunk to a
handful of operations before being understood. These tests pin the fixes so
they cannot quietly come back.
"""

from __future__ import annotations

import pytest

from hourglass.faults import FaultConfig
from hourglass.network import Network
from hourglass.runtime import Simulator, running
from hourglass.scenarios import scenario_for_seed

from examples.kvstore.client import Client
from examples.kvstore.cluster import ClusterConfig
from examples.kvstore.node import Replica

NODES = [f"r{i}" for i in range(5)] + [f"c{i}" for i in range(5)]


def scenario(seed, faults, body, replica_count=5, timeout=0.5):
    """Run one hand-built scenario against a fresh cluster."""
    sim = Simulator(seed=seed)
    net = Network(sim, faults)
    names = [f"r{i}" for i in range(replica_count)]
    for name in names + ["c0"]:
        net.add_node(name)
    replicas = [Replica(name, net) for name in names]

    with running(sim):
        for replica in replicas:
            sim.spawn(replica.serve(), name=replica.name)
        sim.spawn(body(net, names, timeout), name="c0")
        sim.run()
    return net, replicas


# ---------------------------------------------------------------------------
# Bug 1: a duplicated reply inflated the quorum
# ---------------------------------------------------------------------------


def test_duplicate_replies_do_not_make_a_quorum() -> None:
    """Two replicas answering twice is not three replicas answering.

    The overlap argument behind W + R > N counts machines. Counting messages
    instead lets a duplicated packet shrink the read set, which is how an
    acknowledged write went missing on seed 17.
    """
    outcome = []

    async def body(net, names, timeout):
        # Only two replicas are reachable, but every reply is duplicated,
        # so a client counting messages would see four and call it a quorum.
        net.partition([{"c0", "r0", "r1"}, {"r2", "r3", "r4"}])
        client = Client("c0", 0, net, names, 3, 3, timeout)
        outcome.append(await client.put("k", "v"))

    scenario(1, FaultConfig(duplicate_probability=1.0), body)
    assert outcome == [False], "a quorum was assembled from two replicas"


def test_counting_messages_instead_of_replicas_fakes_a_quorum() -> None:
    """The same scenario with the fix off, to show the fix is what matters."""
    outcome = []

    async def body(net, names, timeout):
        net.partition([{"c0", "r0", "r1"}, {"r2", "r3", "r4"}])
        client = Client("c0", 0, net, names, 3, 3, timeout, count_distinct_replicas=False)
        outcome.append(await client.put("k", "v"))

    scenario(1, FaultConfig(duplicate_probability=1.0), body)
    assert outcome == [True], "expected the old bug to report a false success"


def test_a_reachable_quorum_still_succeeds_with_duplicates() -> None:
    """The fix must not reject legitimate quorums."""
    outcome = []

    async def body(net, names, timeout):
        client = Client("c0", 0, net, names, 3, 3, timeout)
        outcome.append(await client.put("k", "v"))

    scenario(2, FaultConfig(duplicate_probability=1.0), body)
    assert outcome == [True]


# ---------------------------------------------------------------------------
# Bug 2: a read could observe a value and then un-observe it
# ---------------------------------------------------------------------------


def test_a_read_repairs_replicas_that_missed_the_value() -> None:
    """A read must leave the value it returns on every replica it can reach.

    Three replicas, quorums of two. The value is planted on two of them, as a
    partial write would leave it; the third has never seen it. After one
    read, all three hold it -- so the value cannot be un-observed later.
    Seed 61 was this bug.
    """
    seen = []

    async def body(net, names, timeout):
        from hourglass.runtime import sleep

        # A partial write: it reached r0 and r1 but never r2.
        net.send("c0", "r0", ("put", "planted", "k", "orphan", (1, 0)))
        net.send("c0", "r1", ("put", "planted", "k", "orphan", (1, 0)))
        await sleep(0.2)
        client = Client("c0", 0, net, names, 2, 2, timeout)
        seen.append(await client.get("k"))

    net, replicas = scenario(3, FaultConfig.perfect(), body, replica_count=3)
    by_name = {r.name: r for r in replicas}

    assert seen[0] == ("orphan", True)
    assert by_name["r2"].store.get("k", (None,))[0] == "orphan", (
        "the replica that missed the write was not repaired"
    )


def test_without_read_repair_the_stale_replica_stays_stale() -> None:
    """The same scenario with the fix switched off, to show it is the fix."""
    seen = []

    async def body(net, names, timeout):
        from hourglass.runtime import sleep

        net.send("c0", "r0", ("put", "planted", "k", "orphan", (1, 0)))
        net.send("c0", "r1", ("put", "planted", "k", "orphan", (1, 0)))
        await sleep(0.2)
        client = Client("c0", 0, net, names, 2, 2, timeout, read_repair=False)
        seen.append(await client.get("k"))

    net, replicas = scenario(3, FaultConfig.perfect(), body, replica_count=3)
    by_name = {r.name: r for r in replicas}

    assert seen[0] == ("orphan", True)
    assert by_name["r2"].store.get("k") is None


def test_a_read_that_cannot_be_made_durable_reports_failure() -> None:
    """If the write-back cannot reach a quorum, the read must not claim success."""
    result = []

    async def body(net, names, timeout):
        net.send("c0", "r0", ("put", "planted", "k", "orphan", (1, 0)))
        from hourglass.runtime import sleep

        await sleep(0.1)
        # Enough replicas answer the read, then the cluster splits so the
        # write-back cannot reach a quorum.
        client = Client("c0", 0, net, names, 3, 3, timeout)
        net.partition([{"c0", "r0", "r1"}, {"r2", "r3", "r4"}])
        result.append(await client.get("k"))

    scenario(4, FaultConfig.perfect(), body)
    assert result[0][1] is False, "a read claimed success without being durable"


# ---------------------------------------------------------------------------
# The seeds that used to fail
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [0, 1, 16, 17, 31, 61, 91])
def test_a_previously_failing_seed_is_now_clean(seed: int) -> None:
    from sweep import play

    trial = play(seed, ClusterConfig(), FaultConfig.hostile())
    assert not trial.broke, trial.report.render()


def test_the_sweep_is_clean_under_hostile_faults() -> None:
    """The gate CI runs, in miniature."""
    from sweep import play

    config, faults = ClusterConfig(), FaultConfig.hostile()
    broken = [seed for seed in range(250) if play(seed, config, faults).broke]
    assert broken == [], f"seeds still failing: {broken}"


def test_the_store_still_works(scope="sanity") -> None:
    """Correctness is easy if you reject everything. Check it still serves."""
    from examples.kvstore.cluster import run

    succeeded = total = 0
    for seed in range(10):
        result = run(seed, faults=FaultConfig.realistic())
        succeeded += result.succeeded
        total += result.operations

    assert total == 1000
    assert succeeded / total > 0.95, f"only {succeeded}/{total} operations succeeded"
