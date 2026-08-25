# Hourglass

**Deterministic simulation testing for concurrent Python.**

A concurrency bug that appears once in five hundred runs is nearly impossible
to fix, because you cannot make it happen again. Hourglass makes it happen on
demand: it replaces the clock, the scheduler's choices, and the network with
fakes driven by a single seeded random number generator.

One integer describes one entire universe. Seed 225 behaves identically today,
tomorrow, and on someone else's laptop.

```console
$ python -m hourglass.demo --scan 1000
scanned 1000 seeds, 8 lose an increment (0.8%)
failing seeds: [225, 317, 399, 554, 661, 696, 773, 950]

$ python -m hourglass.demo --seed 224 --quiet
seed=224  counter=9  expected=9  -> OK

$ python -m hourglass.demo --seed 225 --quiet
seed=225  counter=8  expected=9  -> LOST 1 INCREMENT(S)
```

That last result is not a coin flip. Run it a thousand times and it fails a
thousand times, in exactly the same way.

The network is simulated too, so partitions are just as reproducible:

```console
$ python -m hourglass.netdemo --seed 7 --faults realistic
seed=7  faults=realistic  (latency=1-30ms drop=2% dup=1% slow=5%)
partition 0.5s -> 1.5s

  round  0                 24
         ....XXXXXXX...X.X........      ( . = pong,  X = no reply )

  9 of 25 pings unanswered: rounds [4, 5, 6, 7, 8, 9, 10, 14, 16]
```

The contiguous block is the partition. The stragglers on either side are
ordinary packet loss — and both are fixed by the seed.

## Catching a real bug

Point it at a five-replica key-value store with quorum reads and writes,
split the network in half, and ask whether the result could have come from a
correct database:

```console
$ python -m examples.kvstore.cluster --seed 8 --partition --faults realistic --check
100 operations, 3 keys, peak concurrency 8, 4 pending writes
linearizable: violation

key 'key1': violation (31 ops, 13 states)
  smallest set of operations with no valid ordering:
    c2 put(key1, 'c2#6') -> PENDING
    c3 get(key1) -> 'c2#6'
    c3 put(key1, 'c3#8') -> PENDING
    c0 get(key1) -> 'c2#0'  <-- impossible
```

A write timed out but reached some replicas. One client read it back. A later
read returned a *much older* value. No single correct database, serving one
request at a time, could ever produce that sequence — and the checker proves
it by exhausting every possible ordering rather than guessing.

## How it works

Three sources of nondeterminism, all removed:

| Source | Real program | Hourglass |
|---|---|---|
| Time | asks the OS | an integer the scheduler owns |
| Task ordering | the OS decides | `rng.choice(runnable)` |
| Network timing | the network decides | seeded delays, drops, partitions |

Because the seed drives all three, the run is a pure function of the seed.
And because time is virtual, a simulated day costs microseconds — the test
suite below simulates 24 hours and finishes in under a second.

## Status

Under active development. Built in the open, one day at a time.

- [x] Day 1 — deterministic runtime: scheduler, virtual clock, seeded ordering
- [x] Day 2 — simulated network: latency, loss, duplication, reordering, partitions, crashes
- [x] Day 3 — a quorum-replicated key-value store to test
- [x] Day 4 — linearizability checker
- [ ] Day 5 — the bug hunt, with automatic shrinking
- [ ] Day 6 — fixes and CI regression gate
- [ ] Day 7 — writeup

## Running it

```console
python -m venv .venv && .venv/bin/pip install pytest
.venv/bin/python -m pytest -q
.venv/bin/python -m hourglass.demo --scan 1000
```

No runtime dependencies. `pytest` is used for the tests only.
