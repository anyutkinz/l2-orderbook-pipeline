from __future__ import annotations

import asyncio
import contextlib
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from l2_pipeline.book.types import PriceLevel

SCHEMA_VERSION = (
    "3"  # bumped from "2": exchange/symbol dropped as in-file columns, see DECISIONS.md M7
)
DECIMAL_TYPE = pa.decimal128(18, 8)

DEFAULT_BATCH_SIZE = 500
DEFAULT_FLUSH_INTERVAL_SECONDS = 5.0
DEFAULT_CHECKPOINT_INTERVAL_SECONDS = 300.0
DEFAULT_STALL_TIMEOUT_SECONDS = 60.0


@dataclass(frozen=True, slots=True)
class SnapshotRow:
    # exchange/symbol are NOT written as in-file Arrow columns (see
    # build_schema()) -- they stay on this dataclass because ParquetSink
    # still needs them in memory, to group rows and build each file's Hive
    # partition path (exchange=.../symbol=.../...).
    exchange: str
    symbol: str
    ts_exchange_ms: int | None
    ts_local_ns: int  # monotonic -- deltas/latency only, NOT a real timestamp
    ts_wall_ns: int  # real epoch time -- this is what partitioning uses
    last_applied_id: int
    bids: list[PriceLevel]  # already top-N and sorted (best first) by BookEngine.top_levels()
    asks: list[PriceLevel]


def build_schema(depth_levels: int) -> pa.Schema:
    """exchange/symbol are deliberately NOT columns here (see M7,
    DECISIONS.md): they're already Hive partition keys in the directory
    path (exchange=.../symbol=.../...), and pyarrow's readers -- including
    pq.read_table()'s and pandas.read_parquet()'s DEFAULTS, both
    partitioning='hive' -- reconstruct partition-key columns from the path
    automatically as dictionary-encoded columns. Also storing the same
    names as plain-string columns *inside* the file creates two
    conflicting representations of the same field, which a default read
    across multiple partitions fails to merge (a real bug found during
    M7's manual verification: `ArrowTypeError: Unable to merge: Field
    exchange has incompatible types: string vs dictionary<...>`). Standard
    Hive/Spark/Trino convention is to never duplicate a partition column
    inside the file for exactly this reason -- this schema now follows it.
    """
    fields = [
        pa.field("ts_exchange_ms", pa.int64(), nullable=True),
        pa.field("ts_local_ns", pa.int64(), nullable=False),
        pa.field("ts_wall_ns", pa.int64(), nullable=False),
        pa.field("last_applied_id", pa.int64(), nullable=False),
    ]
    for side in ("ask", "bid"):
        for i in range(depth_levels):
            fields.append(pa.field(f"{side}_price_{i}", DECIMAL_TYPE, nullable=True))
            fields.append(pa.field(f"{side}_qty_{i}", DECIMAL_TYPE, nullable=True))
    schema = pa.schema(fields)
    return schema.with_metadata({"schema_version": SCHEMA_VERSION})


def rows_to_table(rows: list[SnapshotRow], depth_levels: int, schema: pa.Schema) -> pa.Table:
    """Profiling (M7, see DECISIONS.md) found the row loop below re-
    formatting f"{side}_price_{i}"-style column-name strings and doing a
    dict lookup by that string on every single row -- 80 of each per row
    at depth_levels=20 -- the single largest CPU cost in the whole
    pipeline under stress (~19% of total profiled time). The column list
    for a given (side, i) never changes across rows, so both the
    formatting and the lookup are resolved once, here, before the loop;
    the loop below appends directly to the resolved list objects.

    Verified in isolation (timeit, 500-row batches, depth_levels=20):
    48.5ms -> 29.8ms per call, a real 1.63x speedup -- but this does NOT
    move the end-to-end stress-test ceiling (see DECISIONS.md M7): at the
    plateau observed there, asyncio per-tick scheduling overhead in the
    harness dominates, not this function. Kept anyway -- it's a genuine,
    verified reduction in wasted work, independent of whether today's
    synthetic benchmark happens to be bottlenecked elsewhere.
    """
    columns: dict[str, list[Any]] = {field.name: [] for field in schema}
    ts_exchange_ms_col = columns["ts_exchange_ms"]
    ts_local_ns_col = columns["ts_local_ns"]
    ts_wall_ns_col = columns["ts_wall_ns"]
    last_applied_id_col = columns["last_applied_id"]
    ask_cols = [(columns[f"ask_price_{i}"], columns[f"ask_qty_{i}"]) for i in range(depth_levels)]
    bid_cols = [(columns[f"bid_price_{i}"], columns[f"bid_qty_{i}"]) for i in range(depth_levels)]

    for row in rows:
        # row.exchange/row.symbol are NOT written -- see build_schema()
        ts_exchange_ms_col.append(row.ts_exchange_ms)
        ts_local_ns_col.append(row.ts_local_ns)
        ts_wall_ns_col.append(row.ts_wall_ns)
        last_applied_id_col.append(row.last_applied_id)
        for side_cols, levels in ((ask_cols, row.asks), (bid_cols, row.bids)):
            num_levels = len(levels)
            for i, (price_col, qty_col) in enumerate(side_cols):
                if i < num_levels:
                    price_col.append(levels[i].price)
                    qty_col.append(levels[i].qty)
                else:
                    price_col.append(None)
                    qty_col.append(None)
    arrays = [pa.array(columns[field.name], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


class BoundedRowQueue:
    """Wraps asyncio.Queue[SnapshotRow] with the drop-oldest overflow
    policy (the M0 config enum's implemented variant) and a shared
    rows_dropped counter, so feed clients don't each reimplement the
    eviction logic. put() is synchronous/non-blocking -- the message
    path never waits on this, let alone on disk I/O.
    """

    def __init__(self, maxsize: int) -> None:
        self._queue: asyncio.Queue[SnapshotRow] = asyncio.Queue(maxsize=maxsize)
        self.rows_dropped = 0

    def put(self, row: SnapshotRow) -> bool:
        """Returns True if a row had to be evicted to make room."""
        try:
            self._queue.put_nowait(row)
            return False
        except asyncio.QueueFull:
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
            self.rows_dropped += 1
            self._queue.put_nowait(row)
            return True

    async def get(self) -> SnapshotRow:
        return await self._queue.get()

    def qsize(self) -> int:
        return self._queue.qsize()


def _hour_key_for(ts_wall_ns: int) -> str:
    """Must be fed real epoch time (SnapshotRow.ts_wall_ns), never
    ts_local_ns -- that field is monotonic (time.monotonic_ns(), undefined
    reference point), and silently produces a nonsense partition date if
    misinterpreted as epoch time instead of erroring. This is exactly the
    bug M7's stress test found live in M5's original code (every run
    partitioning under "date=1970-01-19" instead of the real date); see
    DECISIONS.md M7.
    """
    dt = datetime.fromtimestamp(ts_wall_ns / 1e9, tz=UTC)
    return dt.strftime("%Y-%m-%d/%H")


def _partition_dir(base_dir: Path, exchange: str, symbol: str, hour_key: str) -> Path:
    date_str, hour_str = hour_key.split("/")
    return (
        base_dir
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"date={date_str}"
        / f"hour={hour_str}"
    )


class _RotatingWriter:
    """Owns one (exchange, symbol) pair's output files. Finalizes (renames
    .tmp -> a real, independently-readable file) at the hour boundary OR
    every checkpoint_interval_seconds, whichever comes first, checked
    lazily on each incoming batch -- not a proactive timer, since during
    genuine silence there's nothing new at risk to checkpoint anyway.
    Bounds an ungraceful crash's data loss to at most one checkpoint
    interval, not up to a full hour. Every write targets .tmp; only
    Path.replace() (atomic cross-platform, unlike bare os.rename which
    raises on Windows if the target exists) ever produces the final name,
    so a crash can only ever leave an orphaned .tmp, never a
    partially-written file at the final name.
    """

    def __init__(
        self,
        base_dir: Path,
        exchange: str,
        symbol: str,
        schema: pa.Schema,
        depth_levels: int,
        checkpoint_interval_seconds: float,
        clock: Callable[[], float],
    ) -> None:
        self._base_dir = base_dir
        self._exchange = exchange
        self._symbol = symbol
        self._schema = schema
        self._depth_levels = depth_levels
        self._checkpoint_interval = checkpoint_interval_seconds
        self._clock = clock

        self._writer: pq.ParquetWriter | None = None
        self._tmp_path: Path | None = None
        self._final_path: Path | None = None
        self._hour_key: str | None = None
        self._sequence = 0
        self._opened_at = 0.0

    def write_batch(self, rows: list[SnapshotRow]) -> None:
        """rows must all share the same hour bucket -- ParquetSink groups
        by (exchange, symbol, hour_key) before calling this."""
        if not rows:
            return
        hour_key = _hour_key_for(rows[0].ts_wall_ns)
        now = self._clock()
        needs_rotation = (
            self._writer is None
            or hour_key != self._hour_key
            or (now - self._opened_at) >= self._checkpoint_interval
        )
        if needs_rotation:
            self._finalize()
            self._open(hour_key, now)
        assert self._writer is not None
        self._writer.write_table(rows_to_table(rows, self._depth_levels, self._schema))

    def _open(self, hour_key: str, now: float) -> None:
        directory = _partition_dir(self._base_dir, self._exchange, self._symbol, hour_key)
        directory.mkdir(parents=True, exist_ok=True)
        if hour_key != self._hour_key:
            self._sequence = 0
        self._hour_key = hour_key
        filename = f"part-{self._sequence:04d}.parquet"
        self._sequence += 1
        self._final_path = directory / filename
        self._tmp_path = directory / f"{filename}.tmp"
        self._writer = pq.ParquetWriter(str(self._tmp_path), self._schema, compression="zstd")
        self._opened_at = now

    def _finalize(self) -> None:
        if self._writer is None:
            return
        self._writer.close()
        assert self._tmp_path is not None and self._final_path is not None
        self._tmp_path.replace(self._final_path)
        self._writer = None
        self._tmp_path = None
        self._final_path = None

    def close(self) -> None:
        self._finalize()


class ParquetSink:
    def __init__(
        self,
        queue: BoundedRowQueue,
        output_dir: Path,
        depth_levels: int,
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_interval_seconds: float = DEFAULT_FLUSH_INTERVAL_SECONDS,
        checkpoint_interval_seconds: float = DEFAULT_CHECKPOINT_INTERVAL_SECONDS,
        stall_timeout_seconds: float = DEFAULT_STALL_TIMEOUT_SECONDS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._queue = queue
        self._output_dir = output_dir
        self._depth_levels = depth_levels
        self._batch_size = batch_size
        self._flush_interval = flush_interval_seconds
        self._checkpoint_interval = checkpoint_interval_seconds
        self._stall_timeout = stall_timeout_seconds
        self._clock = clock or time.monotonic
        self._schema = build_schema(depth_levels)
        self._writers: dict[tuple[str, str], _RotatingWriter] = {}
        self._stats: dict[str, int] = defaultdict(int)
        self._buffer: list[SnapshotRow] = []
        self._last_flush = 0.0
        self._last_progress_at = 0.0

    def get_stats(self) -> dict[str, Any]:
        return {
            "counters": dict(self._stats),
            "queue_depth": self._queue.qsize(),
            "rows_dropped": self._queue.rows_dropped,
        }

    async def run(self) -> None:
        self._buffer = []
        self._last_flush = self._clock()
        self._last_progress_at = self._clock()
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._consume())
                tg.create_task(self._watchdog())
        finally:
            if self._buffer:
                self._flush(self._buffer)
                self._buffer = []
            for writer in self._writers.values():
                writer.close()

    async def _consume(self) -> None:
        """The one and only place asyncio.wait_for()'s timeout is computed.

        A real production hang traced to this loop (see DECISIONS.md M8):
        `asyncio.wait_for(coro, timeout=T)` with T <= 0 cancels the wrapped
        coroutine and raises TimeoutError *without ever giving it a chance
        to run* -- ensure_future() always defers actual execution to the
        next loop iteration, so even a queue that already has items waiting
        never gets to yield one. The old code computed `remaining` as
        `flush_interval - (clock() - last_flush)` and clamped it to >= 0,
        so once anything stalled this coroutine's scheduling for longer
        than flush_interval (a long GC pause, and confirmed live: the host
        OS suspending/resuming), `remaining` went negative. Because the
        buffer was still empty at that exact call, `last_flush` was never
        updated afterward either, so `remaining` stayed negative forever:
        a permanent livelock where the queue filled to `maxsize` and
        BoundedRowQueue's drop-oldest policy silently discarded every row
        from then on, with `rows_written`/`batches_flushed` frozen and no
        exception ever raised for FeedSupervisor to notice.

        Fixed by never handing wait_for a timeout <= 0: whenever the
        interval has already elapsed, flush immediately (buffer or not)
        and reset `last_flush` right here, before computing a timeout, so
        the next wait_for call always gets a fresh, strictly positive
        window and queue.get() is guaranteed an actual chance to run.
        """
        while True:
            remaining = self._flush_interval - (self._clock() - self._last_flush)
            if remaining <= 0:
                if self._buffer:
                    self._flush(self._buffer)
                    self._buffer = []
                self._last_flush = self._clock()
                remaining = self._flush_interval

            try:
                row = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                self._buffer.append(row)
                self._last_progress_at = self._clock()
            except TimeoutError:
                pass

            now = self._clock()
            if len(self._buffer) >= self._batch_size or (
                self._buffer and now - self._last_flush >= self._flush_interval
            ):
                self._flush(self._buffer)
                self._buffer = []
                self._last_flush = now
                self._last_progress_at = now

    async def _watchdog(self) -> None:
        """Defense in depth, independent of the fix in `_consume()`: if
        rows are queued but nothing has drained the queue for
        `stall_timeout_seconds` (a future regression in `_consume()`, or a
        genuinely blocking call inside `_flush()` such as a stalled disk
        write), raise here rather than let the pipeline silently drop rows
        forever. Runs as a TaskGroup sibling of `_consume()` so either one
        failing tears down `run()` as a unit; the raised exception then
        propagates out of `run()` to `FeedSupervisor._run_sink()`'s
        existing crash handling, which logs it and calls
        request_shutdown() -- no changes needed there, since a sink hang
        is exactly the "process-critical" failure that path already exists
        to catch, it just never fired for a hang that raised nothing.

        A queue that's merely idle (nothing to drain) is not a stall --
        the check requires the queue to actually have rows waiting.
        """
        check_interval = max(self._stall_timeout / 4, 1.0)
        while True:
            await asyncio.sleep(check_interval)
            stalled_for = self._clock() - self._last_progress_at
            queued = self._queue.qsize()
            if stalled_for >= self._stall_timeout and queued > 0:
                raise RuntimeError(
                    f"ParquetSink stalled: no row consumed or batch flushed "
                    f"for {stalled_for:.1f}s while {queued} rows are queued "
                    f"(stall_timeout_seconds={self._stall_timeout}); treating "
                    "this as a hang, since an idle sink would have an empty "
                    "queue, not a full one."
                )

    def _flush(self, rows: list[SnapshotRow]) -> None:
        groups: dict[tuple[str, str, str], list[SnapshotRow]] = defaultdict(list)
        for row in rows:
            groups[(row.exchange, row.symbol, _hour_key_for(row.ts_wall_ns))].append(row)

        for (exchange, symbol, _hour_key), group_rows in groups.items():
            writer = self._writers.get((exchange, symbol))
            if writer is None:
                writer = _RotatingWriter(
                    self._output_dir,
                    exchange,
                    symbol,
                    self._schema,
                    self._depth_levels,
                    self._checkpoint_interval,
                    self._clock,
                )
                self._writers[(exchange, symbol)] = writer
            writer.write_batch(group_rows)
            self._stats["rows_written"] += len(group_rows)
        self._stats["batches_flushed"] += 1
