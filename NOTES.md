# Notes

My own understanding, written at the end of each day *without looking at the
code*. If I can't write it, I don't understand it yet. This file becomes the
raw material for the Day 7 writeup.

---

## Day 1 — the deterministic runtime

Questions to answer in my own words:

1. Why can't you reproduce a normal concurrency bug?
2. What are the two things the scheduler takes away from the operating system?
3. What does one seed actually describe?
4. Why does `await sleep(86400)` finish instantly, and why is that useful?
5. Which single line in `runtime.py` decides which interleaving a run explores?
6. Why is the clock an integer number of nanoseconds instead of a float?

My answers:

> _(fill in)_

---

## Day 2 — the simulated network

Questions to answer in my own words:

1. Why is a message in flight just a scheduled callback, the same as a sleeping task?
2. Reachability is checked twice — at send and again at delivery. Why does
   checking it a second time find bugs that checking once would miss?
3. Nothing in the code shuffles a queue, yet messages arrive out of order. How?
4. What does `recv(..., timeout=...)` return when nothing arrives, and why is
   that specific return value a good way to catch careless protocol code?
5. What is the difference between a crashed node and a partitioned node here?

My answers:

> _(fill in)_

## Day 3 — the replicated key-value store

Questions to answer in my own words:

1. Five replicas, W=3, R=3. Why is `W + R > N` supposed to guarantee a read
   sees the latest write?
2. That guarantee has a hole. What happens to a write that reached two
   replicas and then timed out — and does the argument cover it?
3. After the network heals, two replicas still hold a different value for
   `key1` than the other three. Why does nothing fix that?
4. The timeout bug I hit today was in the *network*, not the store: a stale
   deadline from operation 1 killed operation 8. Why did matching waiters by
   task id fail, and why does a per-call ticket fix it?
5. Why did I write the store without hardening it?

My answers:

> _(fill in)_

## Day 4 — the linearizability checker

Questions to answer in my own words:

1. What exactly is the question a linearizability checker asks?
2. Why is a read that overlaps a write allowed to return *either* the old or
   the new value? What would go wrong if I assumed writes take effect when
   issued?
3. A write that timed out is called *pending*. Why does it get no return time
   at all, and why must the search try both "it landed" and "it never did"?
4. Why can each key be checked separately, and why does that matter so much
   for speed?
5. Why does the checker report UNKNOWN instead of VIOLATION when it runs out
   of budget? What would break if it did not?
6. Explain the violation it found on seed 8 to someone non-technical.

My answers:

> _(fill in)_

## Day 5 — the bug hunt

Questions to answer in my own words:

1. Why does the sweep generate the *network disaster* from the seed too, and
   not just the workload?
2. What is delta debugging doing that removing one item at a time would not?
3. Shrinking a simulation is not monotone — fewer operations means a
   *different* run, not a shorter one. Why does that rule out binary search?
4. Minimising the witness once produced a single read, which was a sound proof
   but a useless explanation. What went wrong, and what rule fixed it?
5. 372 violations in 2000 seeds, and 0 in 2000 on a healthy network. Why is the
   second number the more important one?
6. Explain the seed 17 bug to someone non-technical in two sentences.

My answers:

> _(fill in)_

## Day 6 — fixes and CI

Questions to answer in my own words:

1. Why is counting replies instead of replicas a bug? Draw the five replicas
   and show why `W + R > N` stops working.
2. Why does writing a value back before returning it stop a value from being
   observed and then un-observed?
3. The write-back reuses the *original* timestamp instead of a new one. Why
   does that matter?
4. What did the fix cost, and would I take that trade in every system?
5. Why does CI check that the checker still *finds* the bugs, not only that
   the store passes? What would break without that second check?
6. Why did I keep both bugs switchable instead of deleting them?

My answers:

> _(fill in)_

## Day 7 — the writeup

> _(fill in)_
