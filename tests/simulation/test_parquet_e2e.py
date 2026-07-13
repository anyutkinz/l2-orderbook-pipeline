from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path

import pyarrow.parquet as pq

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.book.types import PriceLevel
from l2_pipeline.feeds.envelope import InstrumentId, build_snapshot_row
from l2_pipeline.simulation import (
    FaultConfig,
    SimulatedFeedDriver,
    assert_converged,
    build_simulation,
)
from l2_pipeline.simulation.market import MarketSimulator
from l2_pipeline.sinks.parquet_sink import BoundedRowQueue, ParquetSink


def _caught_up(engine: BookEngine, market: MarketSimulator) -> bool:
    return engine.last_applied_id == market.update_id


def _drain_until_caught_up(
    driver: SimulatedFeedDriver,
    engine: BookEngine,
    market: MarketSimulator,
    max_extra_steps: int = 200,
) -> None:
    for _ in range(max_extra_steps):
        if _caught_up(engine, market):
            return
        driver.run_step()
    raise AssertionError(f"engine did not catch up within {max_extra_steps} extra steps")


def _oracle_top_n(market: MarketSimulator, n: int) -> tuple[list[PriceLevel], list[PriceLevel]]:
    bids_dict, asks_dict = market.oracle_book()
    bids = [PriceLevel(p, q) for p, q in sorted(bids_dict.items(), reverse=True)[:n]]
    asks = [PriceLevel(p, q) for p, q in sorted(asks_dict.items())[:n]]
    return bids, asks


# P6: end-to-end -- M2 harness through a real BookEngine and a real ParquetSink
async def test_p6_simulation_row_matches_oracle_top_n(tmp_path: Path) -> None:
    depth_levels = 5
    market, injector, engine, driver = build_simulation(
        seed=6001,
        config=FaultConfig(
            drop_one_prob=0.03,
            drop_burst_prob=0.02,
            duplicate_prob=0.03,
            reorder_prob=0.03,
            disconnect_prob=0.01,
            delayed_snapshot_prob=0.1,
        ),
        depth_levels=depth_levels,
    )

    driver.run(500)
    _drain_until_caught_up(driver, engine, market)
    assert_converged(engine, market)  # also asserts engine.state is LIVE
    assert injector.log, "fault-free run would defeat the point of using the M2 harness here"

    row = build_snapshot_row(
        engine,
        InstrumentId("sim", "TESTUSD"),
        ts_local_ns=123_000_000_000,
        ts_exchange_ms=None,
    )
    queue = BoundedRowQueue(maxsize=10)
    queue.put(row)

    sink = ParquetSink(queue, tmp_path, depth_levels=depth_levels, batch_size=1)
    sink_task = asyncio.create_task(sink.run())
    for _ in range(1000):
        if sink.get_stats()["counters"].get("batches_flushed", 0) >= 1:
            break
        await asyncio.sleep(0)
    else:
        raise AssertionError("sink never flushed the queued row")

    # Cancelling is how a real shutdown finalizes the writer (.tmp -> final
    # name) -- the file isn't independently readable until this happens.
    sink_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await sink_task

    files = sorted(tmp_path.rglob("part-*.parquet"))
    assert len(files) == 1
    table = pq.read_table(str(files[0]))
    assert table.num_rows == 1

    expected_bids, expected_asks = _oracle_top_n(market, depth_levels)
    for i, (price, qty) in enumerate(expected_bids):
        assert table.column(f"bid_price_{i}")[0].as_py() == price
        assert table.column(f"bid_qty_{i}")[0].as_py() == qty
    for i, (price, qty) in enumerate(expected_asks):
        assert table.column(f"ask_price_{i}")[0].as_py() == price
        assert table.column(f"ask_qty_{i}")[0].as_py() == qty

    assert table.column("last_applied_id")[0].as_py() == engine.last_applied_id
    assert table.column("exchange")[0].as_py() == "sim"
    assert table.column("symbol")[0].as_py() == "TESTUSD"
