"""The simulated network: delivery, loss, reordering, partitions, crashes."""

from __future__ import annotations

import pytest

from hourglass.faults import FaultConfig
from hourglass.netdemo import simulate as simulate_partition
from hourglass.network import Network, recv
from hourglass.runtime import Simulator, current_simulator, running, sleep

SEEDS = [0, 1, 7, 42, 999]


def build(seed: int, config: FaultConfig | None = None, nodes=("a", "b", "c")):
    sim = Simulator(seed=seed)
    net = Network(sim, config or FaultConfig.perfect())
    for node in nodes:
        net.add_node(node)
    return sim, net


# ---------------------------------------------------------------------------
# Determinism, again -- now with a network in the mix
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_partition_demo_is_reproducible(seed: int) -> None:
    first_results, first_trace, first_stats = simulate_partition(seed)
    second_results, second_trace, second_stats = simulate_partition(seed)

    assert first_results == second_results
    assert first_trace == second_trace
    assert first_stats == second_stats


def test_partition_demo_fails_only_during_the_split() -> None:
    """Pings must fail while partitioned and recover afterwards."""
    results, _trace, _stats = simulate_partition(seed=7)
    failed = [n for n, ok in results if not ok]

    assert failed, "the partition never blocked anything"
    assert failed == list(range(failed[0], failed[-1] + 1)), "failures were not contiguous"
    assert results[0][1] is True, "should work before the partition"
    assert results[-1][1] is True, "should recover after healing"


# ---------------------------------------------------------------------------
# Basic delivery
# ---------------------------------------------------------------------------


def test_message_arrives_on_a_healthy_network() -> None:
    sim, net = build(seed=1)
    got = []

    async def listener() -> None:
        got.append(await recv("b"))

    async def sender() -> None:
        net.send("a", "b", "hello")

    with running(sim):
        sim.spawn(listener(), name="listener")
        sim.spawn(sender(), name="sender")
        sim.run()

    assert len(got) == 1
    assert got[0].payload == "hello"
    assert got[0].src == "a" and got[0].dst == "b"


def test_delivery_takes_virtual_time() -> None:
    sim, net = build(seed=1, config=FaultConfig(min_latency=0.01, max_latency=0.01))
    arrival = []

    async def listener() -> None:
        await recv("b")
        arrival.append(sim.now)

    with running(sim):
        sim.spawn(listener(), name="listener")
        sim.spawn(_send_once(net), name="sender")
        sim.run()

    assert arrival[0] == pytest.approx(0.01, abs=1e-6)


async def _send_once(net: Network) -> None:
    net.send("a", "b", "x")


def test_message_sent_before_a_listener_waits_is_queued() -> None:
    """A message that arrives early sits in the inbox rather than vanishing."""
    sim, net = build(seed=2)
    got = []

    async def late_listener() -> None:
        await sleep(1.0)
        got.append(await recv("b"))

    with running(sim):
        sim.spawn(_send_once(net), name="sender")
        sim.spawn(late_listener(), name="listener")
        sim.run()

    assert len(got) == 1


# ---------------------------------------------------------------------------
# Faults
# ---------------------------------------------------------------------------


def test_drop_probability_actually_drops() -> None:
    sim, net = build(seed=3, config=FaultConfig(drop_probability=1.0))

    async def sender() -> None:
        for _ in range(20):
            net.send("a", "b", "doomed")
            await sleep(0.01)

    with running(sim):
        sim.spawn(sender(), name="sender")
        sim.run()

    assert net.delivered == 0
    assert net.dropped == 20


def test_duplicates_arrive_twice() -> None:
    sim, net = build(seed=4, config=FaultConfig(duplicate_probability=1.0))
    got = []

    async def listener() -> None:
        for _ in range(2):
            got.append(await recv("b", timeout=5.0))

    with running(sim):
        sim.spawn(listener(), name="listener")
        sim.spawn(_send_once(net), name="sender")
        sim.run()

    assert [m.payload for m in got if m] == ["x", "x"]
    assert net.duplicated == 1


def test_slow_messages_can_be_overtaken() -> None:
    """Reordering is not shuffled in; it emerges from stragglers."""
    config = FaultConfig(
        min_latency=0.001, max_latency=0.002, slow_probability=0.5, slow_multiplier=100.0
    )
    sim, net = build(seed=11, config=config)
    order = []

    async def listener() -> None:
        for _ in range(12):
            message = await recv("b", timeout=30.0)
            if message is not None:
                order.append(message.payload)

    async def sender() -> None:
        for i in range(12):
            net.send("a", "b", i)
            await sleep(0.0005)

    with running(sim):
        sim.spawn(listener(), name="listener")
        sim.spawn(sender(), name="sender")
        sim.run()

    assert order != sorted(order), "no reordering occurred with 50% stragglers"


def test_timeout_returns_none() -> None:
    sim, net = build(seed=5)
    result = []

    async def listener() -> None:
        result.append(await recv("b", timeout=0.5))

    with running(sim):
        sim.spawn(listener(), name="listener")
        sim.run()

    assert result == [None]
    assert sim.now == pytest.approx(0.5, abs=1e-6)


def test_a_message_beating_the_timeout_cancels_it() -> None:
    sim, net = build(seed=6, config=FaultConfig(min_latency=0.001, max_latency=0.002))
    result = []

    async def listener() -> None:
        result.append(await recv("b", timeout=1.0))

    with running(sim):
        sim.spawn(listener(), name="listener")
        sim.spawn(_send_once(net), name="sender")
        sim.run()

    assert result[0] is not None
    assert result[0].payload == "x"


# ---------------------------------------------------------------------------
# Partitions and crashes
# ---------------------------------------------------------------------------


def test_partition_blocks_across_groups_but_not_within() -> None:
    sim, net = build(seed=7)
    net.partition([{"a", "b"}, {"c"}])

    assert net.reachable("a", "b") is True
    assert net.reachable("b", "a") is True
    assert net.reachable("a", "c") is False
    assert net.reachable("c", "a") is False

    net.heal()
    assert net.reachable("a", "c") is True


def test_partition_swallows_a_message_already_in_flight() -> None:
    """The split is checked on arrival, not only on departure."""
    config = FaultConfig(min_latency=0.10, max_latency=0.10)
    sim, net = build(seed=8, config=config)
    got = []

    async def listener() -> None:
        got.append(await recv("b", timeout=1.0))

    async def sender() -> None:
        net.send("a", "b", "in-flight")
        await sleep(0.01)  # message is on the wire
        net.partition([{"a"}, {"b"}])

    with running(sim):
        sim.spawn(listener(), name="listener")
        sim.spawn(sender(), name="sender")
        sim.run()

    assert got == [None], "a message in flight survived the partition"
    assert net.dropped == 1


def test_crashed_node_is_unreachable_in_both_directions() -> None:
    sim, net = build(seed=9)
    net.crash("b")

    assert net.reachable("a", "b") is False
    assert net.reachable("b", "a") is False

    net.restart("b")
    assert net.reachable("a", "b") is True


def test_crashed_node_receives_nothing() -> None:
    sim, net = build(seed=10)

    async def sender() -> None:
        net.crash("b")
        for _ in range(5):
            net.send("a", "b", "hello?")
            await sleep(0.01)

    with running(sim):
        sim.spawn(sender(), name="sender")
        sim.run()

    assert net.delivered == 0
    assert net.pending("b") == 0


@pytest.mark.parametrize("profile", ["perfect", "realistic", "hostile"])
def test_every_fault_profile_stays_deterministic(profile: str) -> None:
    """Turning the chaos up must not cost reproducibility."""
    from hourglass.netdemo import simulate

    first = simulate(seed=13, profile=profile)
    second = simulate(seed=13, profile=profile)
    assert first == second


def test_harsher_profiles_lose_more_messages() -> None:
    from hourglass.netdemo import simulate

    losses = {}
    for profile in ("perfect", "realistic", "hostile"):
        total = 0
        for seed in range(20):
            _results, _trace, stats = simulate(seed, profile)
            total += stats["dropped"]
        losses[profile] = total

    assert losses["perfect"] < losses["realistic"] < losses["hostile"], losses
