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

SCHEMA_VERSION = "1"
DECIMAL_TYPE = pa.decimal128(18, 8)

DEFAULT_BATCH_SIZE = 500
DEFAULT_FLUSH_INTERVAL_SECONDS = 5.0
DEFAULT_CHECKPOINT_INTERVAL_SECONDS = 300.0


@dataclass(frozen=True, slots=True)
class SnapshotRow:
    exchange: str
    symbol: str
    ts_exchange_ms: int | None
    ts_local_ns: int
    last_applied_id: int
    bids: list[PriceLevel]  # already top-N and sorted (best first) by BookEngine.top_levels()
    asks: list[PriceLevel]


def build_schema(depth_levels: int) -> pa.Schema:
    fields = [
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("ts_exchange_ms", pa.int64(), nullable=True),
        pa.field("ts_local_ns", pa.int64(), nullable=False),
        pa.field("last_applied_id", pa.int64(), nullable=False),
    ]
    for side in ("ask", "bid"):
        for i in range(depth_levels):
            fields.append(pa.field(f"{side}_price_{i}", DECIMAL_TYPE, nullable=True))
            fields.append(pa.field(f"{side}_qty_{i}", DECIMAL_TYPE, nullable=True))
    schema = pa.schema(fields)
    return schema.with_metadata({"schema_version": SCHEMA_VERSION})


def rows_to_table(rows: list[SnapshotRow], depth_levels: int, schema: pa.Schema) -> pa.Table:
    columns: dict[str, list[Any]] = {field.name: [] for field in schema}
    for row in rows:
        columns["exchange"].append(row.exchange)
        columns["symbol"].append(row.symbol)
        columns["ts_exchange_ms"].append(row.ts_exchange_ms)
        columns["ts_local_ns"].append(row.ts_local_ns)
        columns["last_applied_id"].append(row.last_applied_id)
        for side_name, levels in (("ask", row.asks), ("bid", row.bids)):
            for i in range(depth_levels):
                if i < len(levels):
                    columns[f"{side_name}_price_{i}"].append(levels[i].price)
                    columns[f"{side_name}_qty_{i}"].append(levels[i].qty)
                else:
                    columns[f"{side_name}_price_{i}"].append(None)
                    columns[f"{side_name}_qty_{i}"].append(None)
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


def _hour_key_for(ts_local_ns: int) -> str:
    dt = datetime.fromtimestamp(ts_local_ns / 1e9, tz=UTC)
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
        hour_key = _hour_key_for(rows[0].ts_local_ns)
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
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._queue = queue
        self._output_dir = output_dir
        self._depth_levels = depth_levels
        self._batch_size = batch_size
        self._flush_interval = flush_interval_seconds
        self._checkpoint_interval = checkpoint_interval_seconds
        self._clock = clock or time.monotonic
        self._schema = build_schema(depth_levels)
        self._writers: dict[tuple[str, str], _RotatingWriter] = {}
        self._stats: dict[str, int] = defaultdict(int)

    def get_stats(self) -> dict[str, Any]:
        return {
            "counters": dict(self._stats),
            "queue_depth": self._queue.qsize(),
            "rows_dropped": self._queue.rows_dropped,
        }

    async def run(self) -> None:
        buffer: list[SnapshotRow] = []
        last_flush = self._clock()
        try:
            while True:
                remaining = self._flush_interval - (self._clock() - last_flush)
                try:
                    row = await asyncio.wait_for(self._queue.get(), timeout=max(remaining, 0.0))
                    buffer.append(row)
                except TimeoutError:
                    pass

                now = self._clock()
                if len(buffer) >= self._batch_size or (
                    buffer and now - last_flush >= self._flush_interval
                ):
                    self._flush(buffer)
                    buffer = []
                    last_flush = now
        finally:
            if buffer:
                self._flush(buffer)
            for writer in self._writers.values():
                writer.close()

    def _flush(self, rows: list[SnapshotRow]) -> None:
        groups: dict[tuple[str, str, str], list[SnapshotRow]] = defaultdict(list)
        for row in rows:
            groups[(row.exchange, row.symbol, _hour_key_for(row.ts_local_ns))].append(row)

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
