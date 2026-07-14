from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from l2_pipeline.book.types import PriceLevel
from l2_pipeline.sinks.parquet_sink import (
    SCHEMA_VERSION,
    BoundedRowQueue,
    ParquetSink,
    SnapshotRow,
    _RotatingWriter,
    build_schema,
    rows_to_table,
)


def _ts_ns(iso: str) -> int:
    return int(datetime.fromisoformat(iso).replace(tzinfo=UTC).timestamp() * 1e9)


def _row(
    ts_wall_ns: int,
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
    *,
    exchange: str = "binance",
    symbol: str = "BTCUSDT",
    last_applied_id: int = 1,
) -> SnapshotRow:
    # ts_local_ns (monotonic) plays no role in these tests -- only
    # ts_wall_ns (real epoch time) drives partitioning -- so both fields
    # are set to the same caller-supplied value for simplicity.
    return SnapshotRow(
        exchange=exchange,
        symbol=symbol,
        ts_exchange_ms=ts_wall_ns // 1_000_000,
        ts_local_ns=ts_wall_ns,
        ts_wall_ns=ts_wall_ns,
        last_applied_id=last_applied_id,
        bids=[PriceLevel(Decimal(p), Decimal(q)) for p, q in bids],
        asks=[PriceLevel(Decimal(p), Decimal(q)) for p, q in asks],
    )


class _FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


# P1: schema round-trip
def test_p1_schema_round_trip_byte_exact_prices(tmp_path: Path) -> None:
    depth_levels = 3
    schema = build_schema(depth_levels)
    rows = [
        _row(
            _ts_ns("2026-07-13T10:00:00"),
            bids=[("64175.83000000", "0.11117000"), ("64175.82000000", "0.00074000")],
            asks=[("64175.84000000", "9.38033000")],
            last_applied_id=101,
        ),
        _row(
            _ts_ns("2026-07-13T10:00:01"),
            bids=[],  # thin book: fewer than depth_levels -- must pad with nulls, not zeros
            asks=[("64175.84000000", "0.00000001")],  # smallest representable unit at scale=8
            last_applied_id=102,
        ),
    ]

    table = rows_to_table(rows, depth_levels, schema)
    path = tmp_path / "test.parquet"
    pq.write_table(table, str(path), compression="zstd")

    read_back = pq.read_table(str(path))
    assert read_back.schema.metadata[b"schema_version"] == SCHEMA_VERSION.encode()

    col = read_back.column
    assert col("bid_price_0").to_pylist() == [Decimal("64175.83000000"), None]
    assert col("bid_qty_0").to_pylist() == [Decimal("0.11117000"), None]
    assert col("bid_price_1").to_pylist() == [Decimal("64175.82000000"), None]
    assert col("bid_price_2").to_pylist() == [None, None]  # never populated -- null, not zero
    assert col("ask_price_0").to_pylist() == [Decimal("64175.84000000"), Decimal("64175.84000000")]
    assert col("ask_qty_0").to_pylist() == [Decimal("9.38033000"), Decimal("0.00000001")]
    assert col("last_applied_id").to_pylist() == [101, 102]
    assert col("ts_wall_ns").to_pylist() == [rows[0].ts_wall_ns, rows[1].ts_wall_ns]
    # exchange/symbol are deliberately NOT in-file columns (see build_schema()
    # docstring and DECISIONS.md M7) -- they're Hive partition keys only
    assert "exchange" not in read_back.schema.names
    assert "symbol" not in read_back.schema.names


# P2: rotation & atomicity
def test_p2_partitioning_uses_ts_wall_ns_not_ts_local_ns(tmp_path: Path) -> None:
    """Regression test for a real M5 bug M7's stress test found: partition
    date/hour must come from ts_wall_ns (real epoch time), never
    ts_local_ns (monotonic, undefined reference point -- on this machine,
    interpreting it as epoch time lands in 1970). ts_local_ns here is
    deliberately monotonic-shaped (small, non-epoch) and ts_wall_ns is a
    real 2026 timestamp -- if partitioning ever regresses to reading the
    wrong field, this lands in a wildly wrong directory instead of
    silently looking plausible.
    """
    clock = _FakeClock()
    writer = _RotatingWriter(tmp_path, "binance", "BTCUSDT", build_schema(1), 1, 300.0, clock)
    row = SnapshotRow(
        exchange="binance",
        symbol="BTCUSDT",
        ts_exchange_ms=None,
        ts_local_ns=123_000_000_000,  # ~123s of monotonic uptime -- NOT epoch-like
        ts_wall_ns=_ts_ns("2026-07-13T10:00:00"),
        last_applied_id=1,
        bids=[PriceLevel(Decimal("1"), Decimal("1"))],
        asks=[],
    )
    writer.write_batch([row])
    writer.close()

    expected = (
        tmp_path / "exchange=binance/symbol=BTCUSDT/date=2026-07-13/hour=10/part-0000.parquet"
    )
    assert expected.exists()
    assert not list(tmp_path.rglob("date=1970*"))  # the exact wrong path this bug produced


def test_p2_multi_partition_tree_reads_back_via_default_hive_reader(tmp_path: Path) -> None:
    """Regression test for a real bug found during M7's manual A3/A4
    verification: pq.read_table()/pandas.read_parquet() default to
    partitioning='hive', reconstructing exchange/symbol from the directory
    path as dictionary-encoded columns. Storing exchange/symbol as
    plain-string columns *inside* the file too (the pre-fix schema) creates
    two incompatible representations of the same field name, and reading
    more than one partition together fails with exactly:
    "ArrowTypeError: Unable to merge: Field exchange has incompatible
    types: string vs dictionary<...>". Multiple partitions (binance, okx)
    and multiple file rotations (two hours) are required to reproduce it --
    a single file/partition doesn't trigger the merge at all.
    """
    schema = build_schema(1)
    binance_writer = _RotatingWriter(tmp_path, "binance", "BTCUSDT", schema, 1, 300.0, _FakeClock())
    okx_writer = _RotatingWriter(tmp_path, "okx", "BTC-USDT", schema, 1, 300.0, _FakeClock())

    binance_writer.write_batch(
        [
            _row(
                _ts_ns("2026-07-13T10:00:00"),
                [("1", "1")],
                [],
                exchange="binance",
                symbol="BTCUSDT",
            )
        ]
    )
    binance_writer.write_batch(
        [
            _row(
                _ts_ns("2026-07-13T11:00:00"),
                [("2", "2")],
                [],
                exchange="binance",
                symbol="BTCUSDT",
            )
        ]
    )
    okx_writer.write_batch(
        [_row(_ts_ns("2026-07-13T10:00:00"), [("3", "3")], [], exchange="okx", symbol="BTC-USDT")]
    )
    binance_writer.close()
    okx_writer.close()

    # The exact call that failed before the fix -- default partitioning='hive'
    table = pq.read_table(str(tmp_path))
    assert table.num_rows == 3
    assert sorted(table.column("exchange").to_pylist()) == ["binance", "binance", "okx"]
    assert sorted(table.column("symbol").to_pylist()) == ["BTC-USDT", "BTCUSDT", "BTCUSDT"]


def test_p2_hour_boundary_creates_two_finalized_files(tmp_path: Path) -> None:
    clock = _FakeClock()
    writer = _RotatingWriter(tmp_path, "binance", "BTCUSDT", build_schema(1), 1, 300.0, clock)

    writer.write_batch([_row(_ts_ns("2026-07-13T10:59:58"), [("1", "1")], [])])
    writer.write_batch([_row(_ts_ns("2026-07-13T11:00:02"), [("2", "2")], [])])
    writer.close()

    hour10 = tmp_path / "exchange=binance/symbol=BTCUSDT/date=2026-07-13/hour=10/part-0000.parquet"
    hour11 = tmp_path / "exchange=binance/symbol=BTCUSDT/date=2026-07-13/hour=11/part-0000.parquet"
    assert hour10.exists()
    assert hour11.exists()
    assert not list(tmp_path.rglob("*.tmp"))  # both finalized, nothing orphaned

    assert pq.read_table(str(hour10)).column("bid_price_0").to_pylist() == [Decimal("1")]
    assert pq.read_table(str(hour11)).column("bid_price_0").to_pylist() == [Decimal("2")]


def test_p2_checkpoint_interval_finalizes_within_a_single_hour(tmp_path: Path) -> None:
    clock = _FakeClock()
    checkpoint_interval = 300.0
    writer = _RotatingWriter(
        tmp_path, "binance", "BTCUSDT", build_schema(1), 1, checkpoint_interval, clock
    )
    base = _ts_ns("2026-07-13T10:00:00")

    writer.write_batch([_row(base, [("1", "1")], [])])
    clock.now += checkpoint_interval + 1.0  # elapse past the checkpoint interval, same hour
    writer.write_batch([_row(base + 1_000_000_000, [("2", "2")], [])])
    writer.close()

    part0 = tmp_path / "exchange=binance/symbol=BTCUSDT/date=2026-07-13/hour=10/part-0000.parquet"
    part1 = tmp_path / "exchange=binance/symbol=BTCUSDT/date=2026-07-13/hour=10/part-0001.parquet"
    assert part0.exists()
    assert part1.exists()
    assert pq.read_table(str(part0)).column("bid_price_0").to_pylist() == [Decimal("1")]
    assert pq.read_table(str(part1)).column("bid_price_0").to_pylist() == [Decimal("2")]


def test_p2_ungraceful_crash_loses_at_most_one_checkpoint_interval(tmp_path: Path) -> None:
    """Simulates kill -9 / power loss: the writer is never close()d. Asserts
    the guarantee this design actually provides -- data before the last
    checkpoint survives as a finalized, readable file; only data written
    *after* the last checkpoint (bounded by checkpoint_interval_seconds)
    is at risk, sitting in an orphaned, non-Hive-visible .tmp file."""
    clock = _FakeClock()
    checkpoint_interval = 300.0
    writer = _RotatingWriter(
        tmp_path, "binance", "BTCUSDT", build_schema(1), 1, checkpoint_interval, clock
    )
    base = _ts_ns("2026-07-13T10:00:00")

    # checkpoint 1: written and later superseded by checkpoint 2 -> finalized
    writer.write_batch([_row(base, [("1", "1")], [])])
    clock.now += checkpoint_interval + 1.0
    # this write finalizes checkpoint 1 and opens checkpoint 2
    writer.write_batch([_row(base + 1_000_000_000, [("2", "2")], [])])
    # checkpoint 2 gets more data but the process dies before its own
    # checkpoint interval elapses -- writer.close() is deliberately never called
    writer.write_batch([_row(base + 2_000_000_000, [("3", "3")], [])])

    directory = tmp_path / "exchange=binance/symbol=BTCUSDT/date=2026-07-13/hour=10"
    finalized = sorted(directory.glob("part-*.parquet"))
    orphaned_tmp = sorted(directory.glob("*.tmp"))

    assert len(finalized) == 1  # only checkpoint 1 was ever finalized
    assert pq.read_table(str(finalized[0])).column("bid_price_0").to_pylist() == [Decimal("1")]

    assert len(orphaned_tmp) == 1  # checkpoint 2's data: at risk, but bounded
    # the orphaned .tmp is not a valid standalone Parquet file (no footer
    # written -- ParquetWriter.close() was never called) and is invisible
    # to any Hive-style glob reader (*.parquet), which is the actual
    # guarantee: "at most one checkpoint interval of data at risk, and a
    # partial file never masquerades as a valid final one"
    assert orphaned_tmp[0].suffix == ".tmp"
    assert not list(directory.glob("*.parquet.tmp.parquet"))  # sanity: no naming confusion


# P3: backpressure
def test_p3_backpressure_drops_oldest_and_bounds_enqueue_latency() -> None:
    """No separate async consumer task is needed to simulate "slow sink" --
    put() never awaits anything, so its latency is independent of whether
    or how fast anyone calls get(). Simply never draining during the puts
    below stands in for an arbitrarily slow/stalled writer."""
    queue = BoundedRowQueue(maxsize=3)
    rows = [_row(_ts_ns("2026-07-13T10:00:00"), [], [], last_applied_id=i) for i in range(5)]

    start = time.perf_counter()
    evicted = [queue.put(row) for row in rows]
    elapsed = time.perf_counter() - start

    assert evicted == [False, False, False, True, True]
    assert queue.rows_dropped == 2
    assert queue.qsize() == 3
    assert elapsed < 0.1  # sanity bound: 5 synchronous puts, no I/O, should be near-instant


async def test_p3_backpressure_newest_rows_survive_in_fifo_order() -> None:
    queue = BoundedRowQueue(maxsize=3)
    rows = [_row(_ts_ns("2026-07-13T10:00:00"), [], [], last_applied_id=i) for i in range(5)]
    for row in rows:
        queue.put(row)

    survivors = [await queue.get() for _ in range(3)]
    assert [row.last_applied_id for row in survivors] == [2, 3, 4]


# P7: sink stall / livelock (real production incident, see DECISIONS.md M8)
async def test_p7_consume_recovers_after_a_stall_instead_of_livelocking(tmp_path: Path) -> None:
    """Regression test for a real production hang: `_consume()` used to
    compute `remaining = flush_interval - (clock() - last_flush)` and pass
    `max(remaining, 0.0)` straight to `asyncio.wait_for()`. Once anything
    delayed this coroutine's next tick by more than flush_interval (the
    live incident: the host OS suspending for tens of minutes),
    `remaining` went negative, so `wait_for(..., timeout=0)` fired --
    which cancels and raises TimeoutError *without ever giving
    queue.get() a chance to run*, even with rows already sitting in the
    queue. Because the buffer stayed empty at that call, `last_flush` was
    never updated afterward either, so `remaining` stayed negative
    forever: a permanent livelock where BoundedRowQueue's drop-oldest
    policy silently discarded every row from then on.

    Reproduced here by setting `_last_flush` the way `run()` would at
    startup, then advancing the fake clock by a large amount *before*
    `_consume()` ever gets a single iteration -- exactly the "stall
    between setting last_flush and the coroutine's next scheduled tick"
    shape of the live bug -- with a row already queued.
    """
    clock = _FakeClock(start=1000.0)
    queue = BoundedRowQueue(maxsize=10)
    queue.put(_row(_ts_ns("2026-07-13T10:00:00"), [("1", "1")], []))

    sink = ParquetSink(
        queue, tmp_path, depth_levels=1, batch_size=1, flush_interval_seconds=5.0, clock=clock
    )
    sink._last_flush = clock.now
    sink._last_progress_at = clock.now
    clock.now += 3600.0  # simulate a long stall, far past flush_interval

    consume_task = asyncio.create_task(sink._consume())
    try:
        for _ in range(1000):
            if sink.get_stats()["counters"].get("batches_flushed", 0) >= 1:
                break
            await asyncio.sleep(0)
        else:
            raise AssertionError("sink livelocked: the queued row was never drained")
    finally:
        consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consume_task


async def test_p7_watchdog_raises_when_queue_backs_up_without_progress(tmp_path: Path) -> None:
    """Defense in depth, independent of the fix above: if rows are queued
    but nothing drains the queue for stall_timeout_seconds, the watchdog
    must raise so FeedSupervisor's existing crash-handling (log + full
    shutdown, see DECISIONS.md M5) actually fires for a hang -- instead
    of the pipeline running for hours with feed_states staying "running"
    and rows_dropped climbing silently, exactly as happened live.
    """
    clock = _FakeClock()
    queue = BoundedRowQueue(maxsize=5)
    queue.put(_row(_ts_ns("2026-07-13T10:00:00"), [("1", "1")], []))

    sink = ParquetSink(queue, tmp_path, depth_levels=1, stall_timeout_seconds=2.0, clock=clock)
    sink._last_progress_at = clock.now
    clock.now += 10.0  # far past stall_timeout_seconds, with a row still queued

    with pytest.raises(RuntimeError, match="stalled"):
        await asyncio.wait_for(sink._watchdog(), timeout=5.0)


async def test_p7_watchdog_does_not_raise_when_queue_is_merely_idle(tmp_path: Path) -> None:
    """An empty queue for a long time is legitimate idleness, not a stall
    -- only a *non-empty* queue with no progress is a hang. Guards against
    the watchdog becoming a source of false-positive shutdowns."""
    clock = _FakeClock()
    queue = BoundedRowQueue(maxsize=5)  # deliberately never populated

    sink = ParquetSink(queue, tmp_path, depth_levels=1, stall_timeout_seconds=2.0, clock=clock)
    sink._last_progress_at = clock.now
    clock.now += 10.0  # long idle period, but the queue is empty throughout

    watchdog_task = asyncio.create_task(sink._watchdog())
    try:
        await asyncio.sleep(1.2)  # let at least one check_interval elapse
        assert not watchdog_task.done()
    finally:
        watchdog_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watchdog_task


async def test_p7_run_propagates_a_child_task_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Proves the asyncio.TaskGroup wiring in run(): if the watchdog (or
    _consume) raises, run() itself raises too, rather than swallowing it.
    This is what lets FeedSupervisor._run_sink()'s existing `except
    Exception:` handler (log + request_shutdown()) fire for a stalled
    sink -- no changes needed there, since a hang that raises is exactly
    the "process-critical" failure that path already exists to catch; it
    simply never fired for a hang that raised nothing.
    """
    clock = _FakeClock()
    queue = BoundedRowQueue(maxsize=5)
    sink = ParquetSink(queue, tmp_path, depth_levels=1, clock=clock)

    async def _fake_watchdog() -> None:
        raise RuntimeError("simulated sink stall")

    monkeypatch.setattr(sink, "_watchdog", _fake_watchdog)

    with pytest.raises(ExceptionGroup) as exc_info:
        await asyncio.wait_for(sink.run(), timeout=5.0)
    assert any("simulated sink stall" in str(exc) for exc in exc_info.value.exceptions)
