"""Hourglass -- deterministic simulation testing for concurrent Python.

Give a simulation one integer seed and it behaves identically every time it
runs, on every machine. Change the seed and it explores a different ordering
of the same program. Bugs that normally appear once in five hundred runs
become permanent, replayable artifacts.
"""

from hourglass.runtime import (
    Deadlock,
    Simulator,
    Sleep,
    Suspend,
    Task,
    YieldNow,
    current_simulator,
    running,
    sleep,
    yield_now,
)

__all__ = [
    "Deadlock",
    "Simulator",
    "Sleep",
    "Suspend",
    "Task",
    "YieldNow",
    "current_simulator",
    "running",
    "sleep",
    "yield_now",
]

__version__ = "0.1.0"
