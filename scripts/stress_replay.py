"""M7 stress-replay tool.

Sweeps target event rates against the REAL BookEngine + REAL BoundedRowQueue
+ REAL ParquetSink, reusing M2's MarketSimulator/FaultInjector/
SimulatedFeedDriver as the load generator -- no new load generator is
written here. Fault config defaults to STORM_FAULT_CONFIG, the exact same
object S8's convergence test uses (see l2_pipeline.simulation.faults).

Scope, stated plainly (see BENCHMARKS.md for the full methodology and
limitations write-up): this measures the book-engine + queue + sink path
using synthetic, already-parsed DiffEvents from MarketSimulator. It does
NOT exercise real websocket frame receipt, Binance/OKX JSON parsing, or
real network reconnection cost. Real production traffic (~10-20 msg/sec
per feed) sits far below every rate tested here -- this is a headroom/
robustness exercise, not a measurement of the live feed clients' own
ceiling.

Entirely regenerable from a fresh clone with one fixed-seed command (see
--seed; determinism guaranteed the same way D1 guarantees it for the M2
suite): the exact command used to produce BENCHMARKS.md's numbers is
recorded in that file's Methodology section.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import platform
import shutil
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from prometheus_client import CollectorRegistry, Histogram

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.book.types import ApplyResult, ApplyStatus
from l2_pipeline.feeds.envelope import InstrumentId, build_snapshot_row
from l2_pipeline.metrics import build_processing_latency_histogram
from l2_pipeline.simulation import (
    STORM_FAULT_CONFIG,
    FaultConfig,
    FaultInjector,
    FaultType,
    SimulatedFeedDriver,
    build_simulation,
    summarize_log,
)
from l2_pipeline.simulation.seeding import derive_seed
from l2_pipeline.sinks.parquet_sink import BoundedRowQueue, ParquetSink

# 100ms -- the top real bucket of the production processing_latency
# Histogram (see DECISIONS.md M6: "a deliberately generous 'something is
# badly wrong' ceiling"). Reused here, not reinvented, as the latency-based
# ceiling trigger so the stress test's pass/fail threshold ties back to a
# decision already made and justified.
LATENCY_CEILING_SECONDS = 0.1

DEFAULT_RATES = (500, 1000, 2000, 4000, 8000, 16000, 32000)
DEFAULT_NUM_FEEDS = (2, 1)  # num_feeds=2 first: matches production's actual shape


@dataclass(frozen=True, slots=True)
class RunResult:
    num_feeds: int
    target_rate: float
    achieved_rate: float
    duration_seconds: float
    rows_dropped: int
    max_queue_depth: int
    rows_written: int
    processing_latency_p50: float
    processing_latency_p95: float
    processing_latency_p99: float
    processing_latency_p999: float
    sample_count: int
    fault_summary: dict[str, list[int]]
    recoveries: int
    breach: str  # "" if clean, else "rows_dropped" or "p99_latency"


class _FeedState:
    """Mutable per-feed scratch space the on_apply hook closes over: the
    step-start timestamp (refreshed right before each run_step() call) and
    the engine (only available after build_simulation() returns, so it's
    filled in after construction -- the hook isn't actually called until
    later, so this ordering is safe)."""

    def __init__(self) -> None:
        self.ts_local_ns = 0
        self.ts_wall_ns = 0
        self.engine: BookEngine | None = None


def _make_on_apply(
    state: _FeedState,
    instrument: InstrumentId,
    processing_latency: Histogram,
    queue: BoundedRowQueue,
) -> Callable[[ApplyResult], None]:
    def on_apply(result: ApplyResult) -> None:
        if result.status is not ApplyStatus.APPLIED:
            return
        elapsed_seconds = (time.monotonic_ns() - state.ts_local_ns) / 1e9
        processing_latency.labels(exchange=instrument.exchange, symbol=instrument.symbol).observe(
            elapsed_seconds
        )
        assert state.engine is not None
        row = build_snapshot_row(
            state.engine, instrument, state.ts_local_ns, state.ts_wall_ns, None
        )
        queue.put(row)

    return on_apply


def _quantile_from_buckets(buckets: list[tuple[float, float]], quantile: float) -> float:
    """Same linear-interpolation algorithm Prometheus's own
    histogram_quantile() PromQL function uses. buckets: [(le, cumulative
    count), ...] ascending by le, last entry's le is +inf. Returns math.inf
    if the estimated quantile falls above the highest finite bucket
    boundary -- reported as such, never faked as a precise number.
    """
    total = buckets[-1][1]
    if total <= 0:
        return math.nan
    target = quantile * total
    prev_le, prev_count = 0.0, 0.0
    for le, count in buckets:
        if count >= target:
            if math.isinf(le):
                return math.inf
            if count == prev_count:
                return le
            fraction = (target - prev_count) / (count - prev_count)
            return prev_le + fraction * (le - prev_le)
        prev_le, prev_count = le, count
    return buckets[-1][0]


def _histogram_quantiles(
    histogram: Histogram, exchange: str, symbol: str, quantiles: list[float]
) -> tuple[list[float], int]:
    family = next(iter(histogram.collect()))
    buckets: list[tuple[float, float]] = []
    count = 0
    for sample in family.samples:
        if sample.labels.get("exchange") != exchange or sample.labels.get("symbol") != symbol:
            continue
        if sample.name.endswith("_bucket"):
            le = math.inf if sample.labels["le"] == "+Inf" else float(sample.labels["le"])
            buckets.append((le, sample.value))
        elif sample.name.endswith("_count"):
            count = int(sample.value)
    buckets.sort(key=lambda pair: pair[0])
    return [_quantile_from_buckets(buckets, q) for q in quantiles], count


async def _monitor_queue_depth(queue: BoundedRowQueue, stop: asyncio.Event) -> int:
    max_depth = 0
    while not stop.is_set():
        max_depth = max(max_depth, queue.qsize())
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.05)
        except TimeoutError:
            pass
    return max_depth


async def _timed_drive(
    driver: SimulatedFeedDriver, state: _FeedState, rate: float, duration_seconds: float
) -> int:
    """Paces driver.run_step() to a fixed schedule (next_tick += interval,
    not a sleep-after-every-step loop) so per-iteration overhead doesn't
    accumulate as drift against the target rate.

    Always awaits something every iteration, even when running behind
    schedule (sleep_for <= 0) -- asyncio.sleep(0) at minimum. Without this,
    a producer that's fallen behind target never yields the event loop at
    all (a plain `if sleep_for > 0: await ...` skips the await entirely),
    which starves ParquetSink.run() and the queue-depth monitor of any
    chance to run for the rest of the burst -- a real bug this tool's
    first draft had, caught by max_queue_depth reading back 0 despite
    massive concurrent rows_dropped counts (impossible unless the monitor
    task was never scheduled). A real feed client never has this problem:
    every message receipt goes through `await ws.recv()`, which yields
    naturally. This mirrors that instead of measuring an artifact of the
    harness's own scheduling.
    """
    interval = 1.0 / rate
    start = time.monotonic()
    deadline = start + duration_seconds
    next_tick = start
    steps = 0
    while time.monotonic() < deadline:
        state.ts_local_ns = time.monotonic_ns()
        state.ts_wall_ns = time.time_ns()
        driver.run_step()
        steps += 1
        next_tick += interval
        sleep_for = next_tick - time.monotonic()
        await asyncio.sleep(max(sleep_for, 0))
    return steps


async def run_one(
    *,
    num_feeds: int,
    rate: float,
    duration_seconds: float,
    fault_config: FaultConfig,
    depth_levels: int,
    queue_maxsize: int,
    seed: int,
    output_dir: Path,
) -> RunResult:
    registry = CollectorRegistry()
    processing_latency = build_processing_latency_histogram(registry)
    queue = BoundedRowQueue(maxsize=queue_maxsize)
    sink = ParquetSink(queue, output_dir, depth_levels=depth_levels)
    sink_task = asyncio.create_task(sink.run())

    exchange, symbol = "stress", f"num_feeds_{num_feeds}"
    instrument = InstrumentId(exchange, symbol)

    feeds: list[tuple[SimulatedFeedDriver, FaultInjector, _FeedState]] = []
    for i in range(num_feeds):
        feed_seed = derive_seed(seed, f"feed{i}")
        state = _FeedState()
        _market, injector, engine, driver = build_simulation(
            seed=feed_seed,
            config=fault_config,
            depth_levels=depth_levels,
            on_apply=_make_on_apply(state, instrument, processing_latency, queue),
        )
        state.engine = engine
        feeds.append((driver, injector, state))

    stop_monitor = asyncio.Event()
    monitor_task = asyncio.create_task(_monitor_queue_depth(queue, stop_monitor))

    start = time.monotonic()
    steps_per_feed = await asyncio.gather(
        *(_timed_drive(driver, state, rate, duration_seconds) for driver, _inj, state in feeds)
    )
    elapsed = time.monotonic() - start

    stop_monitor.set()
    max_queue_depth = await monitor_task

    sink_task.cancel()
    try:
        await sink_task
    except asyncio.CancelledError:
        pass

    quantiles, sample_count = _histogram_quantiles(
        processing_latency, exchange, symbol, [0.50, 0.95, 0.99, 0.999]
    )
    p50, p95, p99, p999 = quantiles

    fault_summary: dict[str, list[int]] = {ft.value: [0, 0] for ft in FaultType}
    total_recoveries = 0
    for driver, injector, _state in feeds:
        counts = summarize_log(injector.log)
        for fault_type in FaultType:
            fired, shadowed = counts[fault_type]
            fault_summary[fault_type.value][0] += fired
            fault_summary[fault_type.value][1] += shadowed
        total_recoveries += len(driver.recoveries)

    sink_stats = sink.get_stats()
    rows_dropped = queue.rows_dropped
    if rows_dropped > 0:
        breach = "rows_dropped"
    elif math.isinf(p99) or p99 > LATENCY_CEILING_SECONDS:
        breach = "p99_latency"
    else:
        breach = ""

    return RunResult(
        num_feeds=num_feeds,
        target_rate=rate,
        achieved_rate=sum(steps_per_feed) / elapsed,
        duration_seconds=elapsed,
        rows_dropped=rows_dropped,
        max_queue_depth=max_queue_depth,
        rows_written=sink_stats["counters"].get("rows_written", 0),
        processing_latency_p50=p50,
        processing_latency_p95=p95,
        processing_latency_p99=p99,
        processing_latency_p999=p999,
        sample_count=sample_count,
        fault_summary=fault_summary,
        recoveries=total_recoveries,
        breach=breach,
    )


def _platform_info() -> dict[str, str]:
    return {
        "platform": platform.platform(),
        "processor": platform.processor() or "unknown",
        "python_version": platform.python_version(),
        "cpu_count": str(os.cpu_count()),
    }


async def main_async(args: argparse.Namespace) -> dict[str, Any]:
    fault_config = STORM_FAULT_CONFIG if args.fault_config == "storm" else FaultConfig()
    output_root = Path(args.output_dir)
    if output_root.exists():
        shutil.rmtree(output_root)

    results: list[RunResult] = []
    for num_feeds in args.num_feeds:
        for rate in args.rates:
            run_dir = output_root / f"feeds{num_feeds}_rate{int(rate)}"
            print(f"running num_feeds={num_feeds} rate={rate} msg/s ...", file=sys.stderr)
            result = await run_one(
                num_feeds=num_feeds,
                rate=rate,
                duration_seconds=args.duration_seconds,
                fault_config=fault_config,
                depth_levels=args.depth_levels,
                queue_maxsize=args.queue_maxsize,
                seed=args.seed,
                output_dir=run_dir,
            )
            results.append(result)
            print(
                f"  achieved={result.achieved_rate:.0f}/s "
                f"p50={result.processing_latency_p50 * 1e6:.1f}us "
                f"p99={result.processing_latency_p99 * 1e6:.1f}us "
                f"dropped={result.rows_dropped} "
                f"max_queue={result.max_queue_depth} "
                f"breach={result.breach or 'none'}",
                file=sys.stderr,
            )

    report = {
        "run_metadata": {
            **_platform_info(),
            "seed": args.seed,
            "fault_config": args.fault_config,
            "depth_levels": args.depth_levels,
            "queue_maxsize": args.queue_maxsize,
            "duration_seconds": args.duration_seconds,
            "rates": list(args.rates),
            "num_feeds": list(args.num_feeds),
            "latency_ceiling_seconds": LATENCY_CEILING_SECONDS,
        },
        "sweep": [asdict(r) for r in results],
    }
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rates", type=lambda s: [float(x) for x in s.split(",")], default=list(DEFAULT_RATES)
    )
    parser.add_argument(
        "--num-feeds",
        type=lambda s: [int(x) for x in s.split(",")],
        default=list(DEFAULT_NUM_FEEDS),
    )
    parser.add_argument("--duration-seconds", type=float, default=20.0)
    parser.add_argument("--fault-config", choices=["storm", "none"], default="storm")
    parser.add_argument("--depth-levels", type=int, default=20)
    parser.add_argument("--queue-maxsize", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=9001)
    parser.add_argument("--output-dir", type=Path, default=Path("benchmarks/stress_output"))
    parser.add_argument("--report-json", type=Path, default=Path("benchmarks/stress_report.json"))
    parser.add_argument(
        "--profile-output",
        type=Path,
        default=None,
        help="If set, skip the sweep and instead run ONE profiled run (via "
        "cProfile) at --profile-rate/--profile-num-feeds, dumping "
        "<path>.pstats and a human-readable <path>.txt top-30-by-"
        "cumulative-time summary.",
    )
    parser.add_argument("--profile-rate", type=float, default=50_000.0)
    parser.add_argument("--profile-num-feeds", type=int, default=2)
    return parser.parse_args(argv)


async def profile_one(args: argparse.Namespace) -> RunResult:
    fault_config = STORM_FAULT_CONFIG if args.fault_config == "storm" else FaultConfig()
    return await run_one(
        num_feeds=args.profile_num_feeds,
        rate=args.profile_rate,
        duration_seconds=args.duration_seconds,
        fault_config=fault_config,
        depth_levels=args.depth_levels,
        queue_maxsize=args.queue_maxsize,
        seed=args.seed,
        output_dir=Path(args.output_dir) / "profile_run",
    )


def main() -> None:
    args = parse_args()

    if args.profile_output is not None:
        import cProfile
        import pstats

        profiler = cProfile.Profile()
        profiler.enable()
        result = asyncio.run(profile_one(args))
        profiler.disable()

        args.profile_output.parent.mkdir(parents=True, exist_ok=True)
        profiler.dump_stats(str(args.profile_output))
        stats = pstats.Stats(profiler)
        stats.sort_stats("cumulative")
        summary_path = args.profile_output.with_suffix(".txt")
        with open(summary_path, "w") as f:
            stats.stream = f  # type: ignore[attr-defined]
            stats.print_stats(30)
        print(
            f"profiled num_feeds={args.profile_num_feeds} rate={args.profile_rate} "
            f"achieved={result.achieved_rate:.0f}/s dropped={result.rows_dropped} "
            f"-- wrote {args.profile_output} and {summary_path}",
            file=sys.stderr,
        )
        return

    report = asyncio.run(main_async(args))
    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    args.report_json.write_text(json.dumps(report, indent=2))
    print(f"wrote {args.report_json}", file=sys.stderr)


if __name__ == "__main__":
    main()
