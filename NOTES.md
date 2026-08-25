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

> _(fill in)_

## Day 5 — the bug hunt

> _(fill in)_

## Day 6 — fixes and CI

> _(fill in)_

## Day 7 — the writeup

> _(fill in)_
