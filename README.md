# Hourglass

[![tests](https://github.com/neha-s1/hourglass/actions/workflows/ci.yml/badge.svg)](https://github.com/neha-s1/hourglass/actions/workflows/ci.yml)

**Deterministic simulation testing for concurrent Python.**

📄 **[Read the writeup](WRITEUP.md)** — what this is, the two data-loss bugs it
found in a database I wrote, and the three mistakes I made building it.

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

## The bug hunt

```console
$ python sweep.py --seeds 2000 --faults hostile
swept 2000 seeds in 32.0s (16.0ms each)
  faults:     hostile (latency=1-50ms drop=10% dup=5% slow=20%)
  workload:   5 clients x 20 ops on 3 keys
  violations: 372 (18.6%)
```

**372 failures out of 2000 — and zero out of 2000 on a healthy network.**
That second number is what makes the first one worth anything: across 200,000
operations of a system behaving correctly, the checker never once cried wolf.

Each failure is one integer. `--shrink` strips away every client, operation,
key and network fault that was not needed, and the checker reduces the
surviving history to the smallest set of operations that cannot be ordered —
typically two or three, with the timings that prove it.

### Three bugs it found

**A write that was acknowledged, then vanished** — `--seed 17`

```
[   14.48 ->    80.82 ]ms  c1 put(key0, 'c1#0') -> ok
[  544.35 ->   561.20 ]ms  c0 get(key0) -> None   <-- impossible
```

The write succeeded and said so. Four hundred milliseconds later — no overlap,
no ambiguity — a read found nothing there.

**A stale read** — `--seed 0`

```
[  411.03 ->   460.75 ]ms  c1 put(key1, 'c1#3') -> ok
[  635.33 ->   692.74 ]ms  c3 put(key1, 'c3#2') -> ok
[  722.28 ->   767.71 ]ms  c2 get(key1) -> 'c1#3'   <-- impossible
```

Both writes completed before the read began, so every valid ordering ends
`c1#3 → c3#2 → get`. The read had to return `'c3#2'`.

**A read that went backwards** — `--seed 1`

```
[  586.98 ->   637.47 ]ms  c4 put(key2, 'c4#2') -> ok
[  630.03 ->  pending ]ms  c1 put(key2, 'c1#2') -> PENDING
[  717.84 ->   776.97 ]ms  c3 get(key2) -> 'c1#2'
[  802.68 ->  1032.04 ]ms  c3 get(key2) -> 'c4#2'   <-- impossible
```

The timed-out write did land — the first read proves it. The second read began
after the first returned, so it cannot see an older world than the first did.

## The two bugs, and what fixed them

Tracing those failures back turned three symptoms into two causes.

### 1. A duplicated packet faked a quorum

The read on seed 17 counted three replies — from two replicas:

```
557123209  deliver r2->c0  ('get_ok','c0-2', None, (-1,-1))   <- r2
559151002  deliver r3->c0  ('get_ok','c0-2', None, (-1,-1))   <- r3
561198224  deliver r2->c0  ('get_ok','c0-2', None, (-1,-1))   <- r2 again
```

The client counted *messages*. The overlap argument behind `W + R > N` counts
*machines*: three writers and three readers out of five must share at least
one. Two readers and three writers need not. The replica holding the value
answered 1.7 seconds too late.

**Fix:** count answering replicas, not arriving messages.

### 2. A read could observe a value and then un-observe it

```console
$ python sweep.py --seed 208 --faults hostile --broken
[  17.22 -> pending ]ms  c3 put(key0, 'c3#0') -> PENDING
[  41.43 ->   75.91 ]ms  c2 get(key0) -> 'c3#0'
[  90.26 ->  151.95 ]ms  c0 get(key0) -> None   <-- impossible
```

A timed-out write reached one replica. One read's quorum happened to include
it; the next read's did not. Reading was a *peek*.

**Fix:** before returning a value, write it back to a quorum — the second
phase of the [ABD algorithm](https://en.wikipedia.org/wiki/Shared_register).
The write-back carries the original timestamp so it reorders nothing; it only
guarantees that what a read reports, every later read must see. This is why a
linearizable read costs two round trips instead of one.

### Each fix, measured

Both are switchable, so the effect of each is separable — and so the checker
keeps a real target to be tested against:

| store | violations in 300 seeds |
|---|---|
| both bugs present | **55** |
| read repair only | 12 |
| distinct-replica counting only | 24 |
| both fixes | **0** |

```console
$ python sweep.py --seeds 2000 --faults hostile
  violations: 0 (0.0%)
```

Two thousand seeds, two hundred thousand operations, ten percent packet loss,
five percent duplication, randomised partitions and crashes. Nothing.

Reads got slower, which is the honest cost: median 66ms, p95 90ms, two round
trips instead of one.

### Kept honest by CI

The gate runs in both directions on every push, because a checker that
silently stopped detecting anything would make a one-directional gate pass
forever while proving nothing:

```yaml
- name: The store must be linearizable
  run: python sweep.py --seeds 1000 --faults hostile --fail-on-violation

- name: The checker must still catch the bugs it caught before
  run: python sweep.py --seeds 300 --faults hostile --broken --expect-violations
```

A nightly job sweeps 20,000 *fresh* seeds, offset by the day of the year —
because a search that only ever runs the same seeds has stopped searching.

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
- [x] Day 5 — the bug hunt, with automatic shrinking
- [x] Day 6 — fixes and CI regression gate
- [x] Day 7 — writeup: [WRITEUP.md](WRITEUP.md)

## Running it

```console
python -m venv .venv && .venv/bin/pip install pytest
.venv/bin/python -m pytest -q
.venv/bin/python -m hourglass.demo --scan 1000
```

No runtime dependencies. `pytest` is used for the tests only.
