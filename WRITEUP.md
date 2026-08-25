# Making a bug happen on purpose

I built a testing framework that turns unreproducible concurrency bugs into
integers, pointed it at a database I wrote, and it found two ways that
database loses data. This is what it does and what I learned.

---

## The bug that isn't there when you look

Every programmer has met this one. Something fails, once, and never again.
You add a log line and it stops happening. You run the test five hundred
times and it passes five hundred times, so you close the ticket, and three
weeks later a customer hits it.

It happens because a program doing several things at once doesn't really do
them at once. The machine switches between them, and *where* it switches
changes on every run — depending on how busy the CPU is, whether the network
hiccuped, what the scheduler felt like. There are billions of possible
orderings. Your bug lives in a handful of them.

So testing becomes a lottery. Each run draws one ordering out of billions and
hopes it's a bad one. When it is, you can't buy the same ticket twice.

## Take the dice away

The idea behind deterministic simulation testing is blunt: if unpredictability
comes from the operating system, stop asking the operating system.

Three things get faked.

**Time.** The clock is an integer the scheduler owns. Nothing ever really
waits, so `await sleep(3600)` costs about a microsecond. A test that simulates
a full day finishes in under half a second — there's one in the suite that
does exactly that.

**Ordering.** When several tasks are ready to run, a seeded random number
generator picks which goes next.

**The network.** Messages go onto a queue with a delay drawn from the same
generator. It also decides which get dropped, which arrive twice, and which
sit on the wire long enough to be overtaken.

All three come from one seed. Which means one integer describes one entire
universe: every timing, every message order, every scheduling decision. Seed
84213 behaves identically today, tomorrow, and on a machine I've never seen.
CI runs that check on Ubuntu against two Python versions on every push.

If you want a single line that is the whole framework, it's this one:

```python
task = self.rng.choice(runnable)
```

Everything else exists so that this line is the only source of variation left.

## What it looks like

The demo is three workers incrementing a shared counter, with a small window
between reading a value and writing it back — the oldest race there is. The
window is deliberately tiny, so collisions are rare:

```console
$ python -m hourglass.demo --scan 1000
scanned 1000 seeds, 8 lose an increment (0.8%)
failing seeds: [225, 317, 399, 554, 661, 696, 773, 950]

$ python -m hourglass.demo --seed 224 --quiet
seed=224  counter=9  expected=9  -> OK

$ python -m hourglass.demo --seed 225 --quiet
seed=225  counter=8  expected=9  -> LOST 1 INCREMENT(S)
```

Eight failures in a thousand runs is the shape of a bug that survives code
review, survives CI, and shows up in production. Here it's seed 225, and seed
225 fails every single time.

## Pointing it at something real

A framework that only breaks its own demo hasn't proved much. So I wrote a
small replicated key-value store: five replicas, writes go to all of them and
succeed once three acknowledge, reads ask all of them and keep the newest
answer. Any three replicas and any other three must share at least one, since
`3 + 3 > 5`, and that overlap is the argument for why a read always sees the
latest write.

I wrote it the way it gets written the first time. No repair of a replica that
missed an update, no rollback of a write that only got halfway, timestamps
taken from the clock. Each omission is defensible on its own.

Then I needed something to decide whether it had misbehaved, and writing
assertions by hand was hopeless — I'd only catch what I already suspected.

So the second half of the project is a **linearizability checker**, which asks
one question:

> Is there *any* order in which these operations could have happened, one at a
> time, that explains every value returned?

If yes, the system behaved acceptably — internally chaotic, maybe, but no
client could tell, and that's all anyone is owed. If no such order exists, it
returned something impossible, and the checker proves it by exhausting every
possibility rather than guessing.

Two rules constrain the search. If one operation finished before another
started, it comes first. And a read must return whatever write was placed most
recently before it. Operations that *overlap* may go in either order — that
freedom is what makes concurrency legal, and assuming otherwise is how a naive
checker starts crying wolf.

## The hunt

```console
$ python sweep.py --seeds 2000 --faults hostile
swept 2000 seeds in 32.0s
  faults:     latency=1-50ms drop=10% dup=5% slow=20%
  violations: 372 (18.6%)
```

Three hundred and seventy-two seeds produced histories no correct database
could have produced. And on a healthy network, across the same 2000 seeds and
200,000 operations: **zero**. That second number is what makes the first one
worth anything. A tool that reports failures is easy; a tool that doesn't
report them when nothing is wrong is the hard part.

Each failure is one integer, so `--shrink` strips away every client,
operation, key and network fault that wasn't needed, and the checker reduces
the surviving history to the smallest set of operations that can't be ordered.
Across 60 failures the median witness is **four operations**, and 70% are four
or fewer — though a stubborn one occasionally refuses to go below twenty.

One honest caveat about shrinking: removing operations doesn't produce a
shorter version of the same run, it produces a *different* run. Usually it
still trips the same bug. Sometimes it trips a different one, and you have to
notice that rather than assume.

## Bug one: a duplicate faked a quorum

Seed 17. A write succeeded and said so. Four hundred and sixty milliseconds
later, a read found nothing there.

The write had reached `r0`, `r1` and `r4`. The read asked all five and counted
three replies. Here's what those three replies actually were:

```
  557123209  deliver  r2 -> c0   ('get_ok', None)     counted (1)
  559151002  deliver  r3 -> c0   ('get_ok', None)     counted (2)
  561198224  deliver  r2 -> c0   ('get_ok', None)     counted (3)   <-- r2 again
```

Three replies from **two machines**. The network had duplicated one packet,
and the client was counting messages.

```
   replicas    r0 ✓    r1 ✓    r2 ·    r3 ·    r4 ✓       ✓ holds the value
                                                          · never saw it
   the read needed 3 of 5, so it had to overlap the 3 that hold it

   r1 ✓  ──── reply delayed 1689ms ─────────────────✗  too late
   r2 ·  ──── reply ────────────────────▶ 1
   r3 ·  ──── reply ────────────────────▶ 2
   r2 ·  ──── the same reply again ─────▶ 3            ← not a new machine

   "quorum" = 3 replies from 2 replicas.
   3 + 2 = 5, which is not > 5. The overlap guarantee is gone.
```

The overlap argument counts *machines*. Counting messages instead lets a
duplicated packet quietly shrink the read set until it can miss a committed
write. The replica that held the value answered 1.7 seconds too late.

**The fix is one word:** count answering replicas, not arriving messages.

## Bug two: reading was a peek

Seed 208. A value was observed, and then un-observed.

```
[  17.22 -> pending ]  c3 put(key0,'c3#0') -> timed out
[  41.43 ->   75.91 ]  c2 get(key0) -> 'c3#0'      <- so it DID land somewhere
[  90.26 ->  151.95 ]  c0 get(key0) -> None        <- and now it is gone
```

The two reads don't even overlap — the first finished at 75.91ms, the second
started at 90.26ms — so there is no ordering trick that excuses this.

A timed-out write isn't a write that didn't happen. This one reached one
replica before giving up. The first read's quorum happened to include that
replica; the next read's didn't.

```
   after the partial write:   r0 ·   r1 ·   r2 ·   r3 ✓   r4 ·

   c2  get() asks {r1, r3, r4} ──▶ finds ✓ ──▶ returns 'c3#0'
                                               and changes nothing

   c0  get() asks {r0, r1, r2} ──▶ all ·   ──▶ returns None
```

Reading was a *peek*. It reported what it saw and left the world exactly as it
found it, so the next reader could see something older.

**The fix** is the second phase of the [ABD
algorithm](https://en.wikipedia.org/wiki/Shared_register): before returning a
value, write it back to a quorum.

```
   c2  get() asks {r1, r3, r4} ──▶ finds ✓
             └──▶ writes it back ──▶ r0 ✓  r1 ✓  r3 ✓
   c0  get() asks {r0, r1, r2} ──▶ finds ✓ ──▶ returns 'c3#0'
```

The write-back carries the *original* timestamp, so it reorders nothing —
replicas holding something newer ignore it. All it guarantees is that whatever
a read reports, every later read must see. That's why a linearizable read
costs two round trips instead of one.

## What the fixes bought, and what they cost

I kept both bugs switchable rather than deleting them, so each one's
contribution is separately measurable — and so the checker keeps something
that genuinely misbehaves to be tested against:

| store | violations in 300 seeds |
| --- | --- |
| both bugs present | **55** |
| read repair only | 12 |
| distinct-replica counting only | 24 |
| both fixes | **0** |

Across the full 2000 seeds under hostile faults: 372 before, **0** after.

The cost is real. Reads went from one round trip to two — median 66ms, p95
90ms. Whether that's worth it depends entirely on whether you'd rather serve a
stale value fast or a correct one slowly, and plenty of systems reasonably
choose the first.

CI gates both directions on every push:

```yaml
- name: The store must be linearizable
  run: python sweep.py --seeds 1000 --faults hostile --fail-on-violation

- name: The checker must still catch the bugs it caught before
  run: python sweep.py --seeds 300 --faults hostile --broken --expect-violations
```

The second one is easy to forget. A checker that quietly stopped detecting
anything would keep the first gate green forever while proving nothing. I
verified both by reintroducing each bug and confirming CI went red — a gate
you've never watched fail isn't a gate.

## Three things I got wrong

**My test framework had the bug it was built to find.** The first time I ran
100 operations on a perfect network that had dropped nothing, four failed. The
cause was mine: a task calling `recv()` repeatedly leaves one scheduled
timeout behind per call, and I was matching them by task id. A deadline from
operation 1 came due partway through operation 8 and handed it a spurious
failure — indistinguishable from a network problem, on a network with no
problems. Waiters now carry a per-call ticket.

**I built a proof that explained nothing.** Minimising a failing history to
its smallest impossible subset once produced a single read. That's sound — a
lone read returning a value nothing wrote *is* impossible — but only because
minimisation had deleted the write that produced it. Sound and explanatory are
different properties, and the minimiser now refuses to remove a write that
some retained read depends on.

**I trusted my own metric over arithmetic.** The tool reported peak
concurrency of 18 for a run with five clients. Five clients cannot have
eighteen operations in flight. I'd been counting timed-out writes as in flight
forever.

## What it doesn't do

It only tests what you point it at, and only along the axes you thought to
fake. Timestamps come from a single global clock, so the whole genre of bugs
caused by clocks disagreeing between machines is invisible here — and in the
real world that genre is where a lot of last-write-wins data loss actually
comes from. The checker searches exhaustively, so it caps out on long
histories; past that it reports `unknown`, never `violation`, because
confusing "I gave up" with "I proved it impossible" would send you hunting
bugs that don't exist.

## Numbers

- ~1,150 lines of framework, 143 tests, **no runtime dependencies** — the
  scheduler, clock, network and checker are all hand-written
- 16ms to simulate and check 100 operations across 5 replicas
- 1.9 seconds of simulated time per run, in about a hundredth of that

## Try it

```bash
git clone https://github.com/neha-s1/hourglass
cd hourglass && python -m venv .venv && .venv/bin/pip install pytest

# a race that hides in 99.2% of runs -- and the eight seeds where it doesn't
.venv/bin/python -m hourglass.demo --scan 1000
.venv/bin/python -m hourglass.demo --seed 224 --quiet    # fine
.venv/bin/python -m hourglass.demo --seed 225 --quiet    # broken, every time

# the two bugs, with the fixes switched off
.venv/bin/python sweep.py --seed 17  --faults hostile --broken   # write vanished
.venv/bin/python sweep.py --seed 208 --faults hostile --broken   # observed, then not

# hunt, then confirm the fixes hold
.venv/bin/python sweep.py --seeds 500 --faults hostile --broken --quiet
.venv/bin/python sweep.py --seeds 500 --faults hostile --quiet
```

The last two are the whole argument: 95 failures with the bugs in, none with
them out.
