"""The replicated key-value store, on a network that behaves.

Today's job is only to show the protocol works when nothing goes wrong. The
interesting question -- whether it still works when things do -- needs the
linearizability checker, which is day 4.
"""

from __future__ import annotations

import pytest

from hourglass.faults import FaultConfig

from examples.kvstore.cluster import ClusterConfig, run

SEEDS = [0, 1, 7, 42, 999]


# ---------------------------------------------------------------------------
# The day 3 bar: 100 operations, five replicas, nothing goes wrong
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_every_operation_succeeds_on_a_healthy_network(seed: int) -> None:
    result = run(seed)

    assert result.operations == 100
    assert result.failed == 0, [e for e in result.history if not e["ok"]]
    assert result.network["dropped"] == 0


@pytest.mark.parametrize("seed", SEEDS)
def test_replicas_converge_when_nothing_goes_wrong(seed: int) -> None:
    result = run(seed)
    assert result.replicas_agree(), result.snapshots


# ---------------------------------------------------------------------------
# Determinism carries through the whole stack
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_a_cluster_run_is_reproducible(seed: int) -> None:
    first, second = run(seed), run(seed)

    assert first.history == second.history
    assert first.trace == second.trace
    assert first.snapshots == second.snapshots
    assert first.network == second.network


def test_different_seeds_produce_different_runs() -> None:
    histories = {seed: str(run(seed).history) for seed in range(8)}
    assert len(set(histories.values())) > 1


# ---------------------------------------------------------------------------
# The protocol actually stores things
# ---------------------------------------------------------------------------


def test_a_read_sees_the_write_that_preceded_it() -> None:
    """The basic promise, with one client and no concurrency to muddy it."""
    config = ClusterConfig(clients=1, operations_per_client=1, keys=1)
    result = run(1, config=config)
    assert result.operations == 1

    # Drive a put then a get by hand so the ordering is unambiguous.
    from hourglass.network import Network
    from hourglass.runtime import Simulator, running

    from examples.kvstore.client import Client
    from examples.kvstore.node import Replica

    sim = Simulator(seed=3)
    net = Network(sim, FaultConfig.perfect())
    names = [f"r{i}" for i in range(5)]
    for name in names + ["c0"]:
        net.add_node(name)
    replicas = [Replica(name, net) for name in names]
    seen = []

    async def scenario() -> None:
        client = Client("c0", 0, net, names, 3, 3, 0.5)
        assert await client.put("k", "hello") is True
        value, ok = await client.get("k")
        seen.append((value, ok))

    with running(sim):
        for replica in replicas:
            sim.spawn(replica.serve(), name=replica.name)
        sim.spawn(scenario(), name="c0")
        sim.run()

    assert seen == [("hello", True)]


def test_reading_a_key_that_was_never_written_returns_none() -> None:
    from hourglass.network import Network
    from hourglass.runtime import Simulator, running

    from examples.kvstore.client import Client
    from examples.kvstore.node import Replica

    sim = Simulator(seed=4)
    net = Network(sim, FaultConfig.perfect())
    names = [f"r{i}" for i in range(5)]
    for name in names + ["c0"]:
        net.add_node(name)
    replicas = [Replica(name, net) for name in names]
    seen = []

    async def scenario() -> None:
        client = Client("c0", 0, net, names, 3, 3, 0.5)
        seen.append(await client.get("never-written"))

    with running(sim):
        for replica in replicas:
            sim.spawn(replica.serve(), name=replica.name)
        sim.spawn(scenario(), name="c0")
        sim.run()

    assert seen == [(None, True)]


def test_every_read_returns_a_value_some_client_actually_wrote() -> None:
    """No read may invent a value out of nowhere."""
    for seed in SEEDS:
        result = run(seed)
        written = {e["value"] for e in result.history if e["op"] == "put"}
        for entry in result.history:
            if entry["op"] == "get" and entry["ok"] and entry["result"] is not None:
                assert entry["result"] in written


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_quorums_that_cannot_overlap_are_rejected() -> None:
    with pytest.raises(ValueError, match="W \\+ R must exceed N"):
        ClusterConfig(replicas=5, write_quorum=2, read_quorum=3)


def test_history_is_ordered_by_invocation_time() -> None:
    history = run(11).history
    times = [entry["invoked_ns"] for entry in history]
    assert times == sorted(times)


def test_every_history_entry_has_the_fields_the_checker_will_need() -> None:
    required = {"process", "op", "key", "invoked_ns", "returned_ns", "ok", "result"}
    for entry in run(5).history:
        assert required <= set(entry)
        assert entry["returned_ns"] >= entry["invoked_ns"]
