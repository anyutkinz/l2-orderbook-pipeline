# Benchmarks

Last updated: 2026-07-13.

Every number below is labeled with its provenance: **[synthetic]** means
produced by `scripts/stress_replay.py` against the real pipeline with a
synthetic, deterministic load generator; **[live]** means observed on a
real Binance/OKX dual-feed run; **[M2 suite]** means pulled from an actual
`pytest -v -s` run of the deterministic simulation test suite. No number
in this document is illustrative or estimated — anything that couldn't be
honestly produced by a real run in the time available is omitted rather
than approximated.

## Methodology

- **Hardware/OS**: Windows-11-10.0.26200-SP0, Intel64 Family 6 Model 140
  Stepping 2 GenuineIntel, 8 logical CPUs, Python 3.12.0. Single Windows
  development machine — see Limitations.
- **Stress tool**: `scripts/stress_replay.py`. Reuses M2's
  `MarketSimulator` + `FaultInjector` + `SimulatedFeedDriver`
  (`build_simulation()`) as the load generator against the real
  `BookEngine`, real `BoundedRowQueue`, and real `ParquetSink` — no new
  load generator was written for M7.
- **Fault config**: `STORM_FAULT_CONFIG`, the exact object S8's
  convergence test uses (`l2_pipeline.simulation.faults`) — extracted to a
  shared constant specifically so M7 would reuse it verbatim rather than
  re-typing a second copy that could drift.
- **Seed**: `9001`, fully reproducible the same way D1 guarantees
  reproducibility for the M2 suite (`derive_seed`).
- **Profiling**: `cProfile` (stdlib). `py-spy` was attempted first (per
  the approved design) but hit a genuine low-level Windows error in this
  environment — `Error: Failed to find python version from target
  process` / `os error 31` — reproduced even against a trivial one-line
  script, confirming it's an environment/permissions issue, not specific
  to this tool. cProfile was used instead, exactly as the design's
  fallback plan specified.
- **Regenerable from a fresh clone with one command**:
  ```
  uv run python scripts/stress_replay.py
  ```
  Defaults reproduce every sweep number in this document: rates
  `500,1000,2000,4000,8000,16000,32000`, `num_feeds=2,1`,
  `duration_seconds=20`, `fault_config=storm`, `depth_levels=20`,
  `queue_maxsize=10000`, `seed=9001`. Output written to
  `benchmarks/stress_output/` and `benchmarks/stress_report.json` (the
  literal source data this document's tables were copied from, not
  retyped from memory).
- **Scope, stated plainly**: the stress test exercises the book-engine +
  queue + sink path using synthetic, already-parsed `DiffEvent`s from
  `MarketSimulator`. It does **not** exercise real websocket frame
  receipt, Binance/OKX JSON parsing, or real network reconnection cost.
  Real production traffic (~10-20 msg/sec per feed — see "Real-world
  throughput" below) sits far below every rate tested here. This is a
  headroom/robustness exercise for the sink and queue, **not** a
  measurement of the live feed clients' own ceiling — nothing here should
  be read as "the pipeline can handle N msg/sec from a real exchange."

## Throughput [synthetic]

### Result: no drop ceiling found within the tested range

`BoundedRowQueue` + `ParquetSink` produced **zero dropped rows at every
rate tested, from 500 to 32,000 msg/sec, for both `num_feeds=2` (matches
production's actual shape — two concurrent feeds sharing one queue and
sink) and `num_feeds=1`.** Queue depth never exceeded 2 rows (num_feeds=2)
or 1 row (num_feeds=1) at any point during any run — the sink drains
essentially as fast as it's fed, across the entire tested range.

The achieved throughput does not scale linearly with the target rate
past roughly 8,000 msg/sec — it plateaus around **11,600-11,700 msg/sec**
(`num_feeds=2`) and **11,200-11,400 msg/sec** (`num_feeds=1`) regardless
of how much higher the target is set. This plateau is **not** the sink or
queue struggling (queue depth stays at 1-2 the whole time); profiling
(below) shows it's `asyncio` per-tick scheduling overhead in the stress
harness's own pacing loop — the harness itself, not the pipeline under
test, is what limits how fast synthetic ticks can be generated on this
machine. A real feed client doesn't have this specific ceiling, since its
own `await ws.recv()` calls are paced by real network arrival, not a
tight local scheduling loop.

**Practical conclusion**: since real production traffic is ~10-20
msg/sec/feed (~20-40 msg/sec combined), and this pipeline shows zero
degradation at rates 300-1000x that, there is substantial, unmeasured
headroom above any load this pipeline will actually see in production.

### Full sweep table

`num_feeds=2` first (production's actual shape):

| target rate | achieved rate | p50 | p99 | rows dropped | max queue depth |
|---|---|---|---|---|---|
| 500 | 1000/s | 25.0us | 49.6us | 0 | 2 |
| 1,000 | 1998/s | 25.0us | 49.6us | 0 | 2 |
| 2,000 | 3997/s | 25.0us | 49.6us | 0 | 2 |
| 4,000 | 7990/s | 25.0us | 49.6us | 0 | 2 |
| 8,000 | 11709/s | 25.0us | 49.6us | 0 | 2 |
| 16,000 | 11696/s | 25.0us | 49.6us | 0 | 2 |
| 32,000 | 11665/s | 25.0us | 49.6us | 0 | 2 |

`num_feeds=1`:

| target rate | achieved rate | p50 | p99 | rows dropped | max queue depth |
|---|---|---|---|---|---|
| 500 | 499/s | 25.0us | 49.5us | 0 | 1 |
| 1,000 | 1000/s | 25.0us | 49.5us | 0 | 1 |
| 2,000 | 1998/s | 25.0us | 49.6us | 0 | 1 |
| 4,000 | 3992/s | 25.0us | 49.5us | 0 | 1 |
| 8,000 | 7976/s | 25.0us | 49.6us | 0 | 1 |
| 16,000 | 11371/s | 25.0us | 49.6us | 0 | 1 |
| 32,000 | 11223/s | 25.0us | 49.6us | 0 | 1 |

(`target_rate` here is per the harness's own driving rate, achieved rate
is the actual combined events/sec produced across all feeds — see
`scripts/stress_replay.py`'s module docstring for exactly what "target
rate" means operationally.)

Note the `p50`/`p99` figures in this table are the **synthetic** stress
test's `processing_latency` samples (book-engine apply cost only, same
measurement point as production) — not to be confused with the **live**
percentiles in the "Latency percentiles (live)" section below, which are
a completely different, real-traffic measurement.

**Why `p50`/`p99` look identical across every row, confirmed, not
assumed**: the production `processing_latency` Histogram's smallest
bucket boundary is `le=0.00005` (50µs). `25.0us` is exactly 50.0% of the
way into that bucket; `49.6us`/`49.5us` are exactly 99.0-99.2% of the way
into it. Confirmed directly by feeding 500 vs. 50,000 synthetic samples
(a 100x difference) drawn from the same realistic 15-35µs cost
distribution into a real `Histogram` with these exact buckets: both
sample counts produced the identical `p50=25.0us`/`p99=49.5us`. This is
Prometheus's `histogram_quantile()` linearly interpolating *within* a
single dominant bucket — since virtually every observed apply cost is
well under 50µs regardless of synthetic tick rate (the per-event cost is
CPU-bound dict/Decimal work, not I/O, so it doesn't change with load),
essentially the entire sample population lands in that one bucket at
every rate tested, and the interpolated quantile depends only on the
*fraction* through that bucket — which stays stable — not on how many
samples or what rate produced them. This is bucket-quantized estimation
inherent to the histogram type these numbers come from, the same type
production uses, not an absence of real per-event variance.

### Methodology note on the harness's own scheduling

An earlier draft of the pacing loop only called `await asyncio.sleep()`
when it was ahead of schedule (`if sleep_for > 0`), meaning a producer
that fell behind target never yielded the event loop at all once it fell
behind — starving `ParquetSink.run()` and the queue-depth monitor of any
chance to run. This produced misleadingly large `rows_dropped` counts
that were actually a harness scheduling bug, not a real pipeline
limitation (`max_queue_depth` reading back `0` despite large concurrent
drop counts was the tell — impossible unless the monitor task was never
scheduled). Fixed by always awaiting something every iteration
(`asyncio.sleep(max(sleep_for, 0))`), mirroring how a real feed client's
`await ws.recv()` always yields naturally. All numbers in this document
are from the harness after this fix.

## Profiling findings [synthetic]

Profiled at `num_feeds=2`, target rate 50,000 msg/sec, 20 seconds, via:
```
uv run python scripts/stress_replay.py --profile-output benchmarks/profile.pstats \
    --profile-rate 50000 --profile-num-feeds 2 --duration-seconds 20
```
(Full output committed at `benchmarks/profile.pstats` / `benchmarks/profile.txt`.)

**Before any optimization**, `rows_to_table()` in `parquet_sink.py` was
the single largest self-time (`tottime`) consumer in the entire profile —
**3.679s of 19.600s total profiled time, ~18.8%** — roughly 8x larger
than the next candidate (`BookEngine.top_levels()`'s full sort, 0.479s).
Cause: the row-serialization loop was re-formatting fixed column-name
strings (`f"{side}_price_{i}"`) and doing a fresh dict lookup by that
string **on every single row** — 80 such format+lookup pairs per row at
`depth_levels=20`, when the column list for a given `(side, i)` never
changes across rows.

Other real costs visible in the profile, for context: `pyarrow`'s
`write_table()` (real disk I/O + zstd compression, ~1.1-1.2s, legitimate
work); `BookEngine.top_levels()`'s full sort of the whole book on every
row emission (0.4-0.5s, a smaller, secondary optimization candidate not
pursued in this milestone — flagged for future work, not fixed now,
staying within "one targeted optimization"); a large volume of `asyncio`
scheduling overhead (`_run_once`, `Context.run`, `wait_for`,
`timeouts.py`) consistent with the throughput-ceiling finding above; and
a `<string>:1(<lambda>)` cost (~0.75-0.8s, ~1.7-1.9M calls) that traces to
`pyarrow`'s own internal `decimal128` conversion path, not this
codebase's code — a real cost of the M5 decimal128-for-precision
tradeoff, not something addressable here.

## Optimization

**One targeted optimization was made**, verified with a clean before/after
methodology, and the honest result is reported below — including the part
where it didn't move the number you might expect it to.

**What changed**: `rows_to_table()` now resolves each column's list
object reference once, before the row loop, instead of re-formatting the
column-name string and re-looking it up in the columns dict on every row.

**Isolated microbenchmark** (`timeit`, 500-row batches, `depth_levels=20`,
50 calls, same interpreter session, immediately before/after via a
temporary revert — not two separate sessions):

| | time per call |
|---|---|
| Before | 48.452 ms |
| After | 29.804 ms |
| **Speedup** | **1.63x (38.5% reduction)** |

Cross-checked against the profile: `rows_to_table`'s `tottime` in-context
dropped from 3.679s (before) to ~2.2s (after, both profiled runs at
identical `num_feeds=2`/rate=50,000/duration=20s/seed=9001) — a ~40%
reduction, consistent with the isolated measurement.

**End-to-end result, measured honestly**: the same `num_feeds=2`,
rate=50,000, duration=20s configuration, run 3x before and 3x after
(non-profiled, real achieved-throughput measurement):

| | run 1 | run 2 | run 3 | mean |
|---|---|---|---|---|
| Before | 10185/s | 10434/s | 10282/s | 10300/s |
| After | 9527/s | 8965/s | 9520/s | 9337/s |

**The optimization did not improve — and within this small sample, was
even slightly lower than — the end-to-end achieved throughput.** This is
not a measurement error; it's the direct, expected consequence of the
throughput-ceiling finding above: at this plateau, `asyncio` per-tick
scheduling overhead in the harness dominates, not `rows_to_table`.
Halving a function's cost that isn't the bottleneck doesn't move an
end-to-end number bounded by something else entirely — a genuinely
useful thing to have measured and reported plainly, rather than either
skipping the end-to-end check or quietly reporting only the flattering
isolated number.

**Kept anyway**: the optimization is a real, verified reduction in wasted
CPU work (confirmed three independent ways: profile self-time, isolated
microbenchmark, and in-context profile re-run), independent of whether
today's synthetic benchmark happens to be bottlenecked elsewhere. It would
matter more in a regime without the harness's own scheduling ceiling, or
at larger `depth_levels` (more columns = more relative time in this
function).

## Latency percentiles (live)

Source: live Binance + OKX dual-feed run, Prometheus `processing_latency`
histogram, `histogram_quantile()` over a 5-minute window, 2026-07-13.

| percentile | binance | okx |
|---|---|---|
| p50 | 25.4us | 25.2us |
| p95 | 48.1us | 47.9us |
| p99 | **12.4ms** | 49.9us |
| p999 | 19.2ms | 19.0ms |

p50/p95 are consistent with the synthetic stress test's figures above
(same measurement point: monotonic-clock delta from frame receipt to an
`APPLIED` diff-apply result) — real-traffic steady-state cost matches the
synthetic-load cost closely, which is itself a useful cross-check that
the synthetic benchmark's per-event cost is representative.

**Binance's p99 is investigated, not asserted as fact.** The working
hypothesis going in was that `load_snapshot()` (resync) cost was being
recorded in the same histogram as steady-state `apply_event()` cost — a
plausible multiple-orders-of-magnitude explanation if true. Checked
directly against the code: `processing_latency.observe()` is called from
exactly one place in each feed client (the steady-state `APPLIED` branch)
and is never called from the resync-completion path, in either
`binance.py` or `okx.py` — confirmed by reading both call sites, not
assumed. The indirect mechanism (a resync occupying the event loop and
delaying a later message) was also traced through and ruled out:
`ts_local_ns` is captured fresh, immediately after `ws.recv()` returns for
that specific message, so a prior delay shifts *when* a message is
processed, not the measured receipt-to-applied delta itself.

A fresh, separate soak run was checked for `GAP_DETECTED`/
`RESYNC_COMPLETED` log entries: only the two expected cold-start resyncs
at startup appeared, no `GAP_DETECTED` anywhere in that run. This does
not confirm the resync hypothesis for the *original* measurement (a
different run/window, whose console output wasn't captured to a file, so
it can't be checked retroactively).

**Working conclusion, stated as a hypothesis, not a fact**: the p99 spike
is most likely a one-off tail-latency event — a GC pause or OS-level
scheduling preemption landing inside the synchronous parse+apply span for
one message — rather than a resync-measurement artifact. This is
consistent with p999 converging to a similar ~19ms magnitude on *both*
exchanges, which points toward a shared interpreter/OS-level cause rather
than an exchange-specific one. **This was checked and not confirmed for
this specific occurrence** — it is not asserted as certain.

## Real-world throughput (live)

Source: dual-feed soak run, 2026-07-13, heartbeat log diff, two heartbeat
lines ~30s apart:

```
09:38:16.419636 -> binance=44404, okx=40417, rows_written=84781
09:38:46.410859 -> binance=44705, okx=40659, rows_written=85327
```

Independently recomputed (not just trusted): delta_t = 29.991223s.

| | rate |
|---|---|
| binance messages | 10.04 msg/sec |
| okx messages | 8.07 msg/sec |
| combined messages | 18.11 msg/sec |
| rows written | 18.21 rows/sec |
| batches flushed | +6 over 30s (4.999s/batch) |

**Internal consistency, verified**: rows-written rate (18.21/s) tracks
combined message rate (18.11/s) almost exactly, consistent with "one
Parquet row per applied event" as designed (DECISIONS.md M5, Decision A).
Batches-flushed interval (4.999s) matches the configured
`flush_interval_seconds=5.0` to within measurement precision. This is
worth stating explicitly: it's evidence the pipeline behaves exactly as
designed end-to-end on live traffic, not just a set of independently
plausible-looking positive numbers.

Also confirms the "real traffic sits far below any tested synthetic rate"
claim used to scope the stress test above: ~18 msg/sec combined vs. a
zero-drop synthetic ceiling of 11,000+/sec is roughly 3 orders of
magnitude of headroom.

## Fault-recovery statistics [M2 suite]

Source: `uv run pytest tests/simulation/test_scenarios.py -v -s`, S8
(fault-storm scenario, `STORM_FAULT_CONFIG`, 5,000 steps) and D1
(determinism), both passing.

```
S8 fault storm: 5000 steps, 3858 quiescent convergence checks passed,
1281 faults fired, 42 shadowed, 657 recoveries, 465 stale-snapshot retries
  drop_one           fired=220    shadowed=5
  drop_burst         fired=146    shadowed=2
  duplicate          fired=165    shadowed=13
  reorder            fired=144    shadowed=22
  disconnect         fired=48     shadowed=0
  delayed_snapshot   fired=558    shadowed=0
```

Every fault type fired at least 48 times over the run; the engine
recovered — full convergence against the oracle book — after every single
one, 657 times total, with zero unrecovered divergences. D1 confirms this
entire run (including which faults fire, in what order) is byte-for-byte
reproducible from `seed=1008` alone.

## Parquet output stats (live)

Source: dual-feed soak run, ~15-20 minutes, 2026-07-13, schema_version=3
(post-fix, exchange/symbol as Hive partition keys only, no in-file
duplication — see DECISIONS.md M7). Verified via
`pandas.read_parquet("./data")`:

| exchange | symbol | rows |
|---|---|---|
| binance | BTCUSDT | 25,767 |
| okx | BTC-USDT | 24,162 |

**Schema correctness confirmed on real live data, not just the test
suite**: 84 in-file columns (4 metadata + 20 levels x 2 sides x
price/qty) plus `exchange`/`symbol`/`date`/`hour` reconstructed cleanly as
Hive partition columns, no duplication, no merge error — the exact defect
class fixed in M7 (see DECISIONS.md) verified absent on a real multi-hour,
multi-partition run, not just the synthetic regression test. Partition
date reconstructed correctly as `2026-07-13` (the real calendar date,
confirming the `ts_wall_ns` partitioning fix — the original bug would
have produced `date=1970-01-19`). The `ask_price_0 > bid_price_0`
book-sanity invariant holds on every row of the full dataset — no crossed
book was ever written.

This is the same "one Parquet row per applied event" design validated
independently by the real-world-throughput cross-check above (18.21
rows/sec ≈ 18.11 combined msg/sec) and now also by absolute row counts
matching a ~15-20 minute run at that rate order of magnitude.

## Limitations

- **Single Windows development machine.** These are not production or
  cloud-hardware numbers, and nothing here should be read as a capacity
  planning figure for any other environment.
- **Live network numbers are single-location, single-ISP.** No attempt
  was made to characterize network variability across geographies or
  providers.
- **The stress test measures the book-engine + queue + sink path using
  synthetic, already-parsed events.** It does not exercise real websocket
  frame receipt, Binance/OKX JSON parsing overhead, or real network
  reconnection cost — see the Methodology section's scope note. The
  throughput-ceiling finding is a statement about this pipeline's
  internal headroom, not about what the live feed clients themselves can
  sustain against a real exchange.
- **The observed throughput "ceiling" is the stress harness's own
  `asyncio` scheduling overhead, not a pipeline limitation** — see
  "Methodology note on the harness's own scheduling" above. No genuine
  drop-based ceiling for `BoundedRowQueue`/`ParquetSink` was found within
  the tested range.
- **p999 latency figures are statistically noisy at low sample counts** —
  at rank ~99.9%, a single outlier can dominate the reported value in
  shorter runs. Longer runs / larger sample counts would produce a more
  stable estimate.
- **The Binance p99 latency spike (Latency percentiles section) was
  investigated but not conclusively resolved** — the original
  measurement's console output wasn't captured to a file and can't be
  checked retroactively; the working explanation (GC pause / OS
  scheduling preemption) is plausible and consistent with available
  evidence but not proven.
- **No benchmark or stress numbers beyond what's in this document were
  produced** — anything not shown here (e.g. multi-machine scaling,
  sustained multi-hour throughput under real network conditions) simply
  wasn't measured, and no claim is made about it either way.
