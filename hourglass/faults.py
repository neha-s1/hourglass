"""What the network is allowed to do to your messages.

Real networks lose packets, deliver them out of order, occasionally deliver
the same one twice, and sometimes split in half so that two groups of machines
can each talk internally but not to each other. Distributed systems are
supposed to survive all of it. Most of them have never been asked to.

A :class:`FaultConfig` is the dial. Every decision it drives is drawn from the
simulator's seeded RNG, so a hostile network is exactly as reproducible as a
perfect one.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FaultConfig:
    """Probabilities and latencies governing one simulated network.

    All times are in seconds of virtual time.
    """

    min_latency: float = 0.001
    max_latency: float = 0.030

    #: Chance a message is thrown away entirely.
    drop_probability: float = 0.0

    #: Chance a message is delivered twice. Protocols that assume
    #: at-most-once delivery break here, and many quietly assume it.
    duplicate_probability: float = 0.0

    #: Chance a message is delayed far beyond normal latency. This is what
    #: produces reordering: a slow message overtaken by later ones.
    slow_probability: float = 0.0
    slow_multiplier: float = 25.0

    @classmethod
    def perfect(cls) -> "FaultConfig":
        """A network that never misbehaves. Useful for proving the happy path."""
        return cls(min_latency=0.001, max_latency=0.005)

    @classmethod
    def realistic(cls) -> "FaultConfig":
        """Occasional loss and jitter, the kind a datacenter actually sees."""
        return cls(
            min_latency=0.001,
            max_latency=0.030,
            drop_probability=0.02,
            duplicate_probability=0.01,
            slow_probability=0.05,
        )

    @classmethod
    def hostile(cls) -> "FaultConfig":
        """Loss and reordering turned up until protocols confess their sins."""
        return cls(
            min_latency=0.001,
            max_latency=0.050,
            drop_probability=0.10,
            duplicate_probability=0.05,
            slow_probability=0.20,
            slow_multiplier=40.0,
        )

    def summary(self) -> str:
        return (
            f"latency={self.min_latency * 1000:.0f}-{self.max_latency * 1000:.0f}ms "
            f"drop={self.drop_probability:.0%} "
            f"dup={self.duplicate_probability:.0%} "
            f"slow={self.slow_probability:.0%}"
        )
