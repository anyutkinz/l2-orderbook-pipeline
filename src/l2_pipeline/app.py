from __future__ import annotations

import argparse
import asyncio
import logging
import random
import signal
from pathlib import Path
from typing import Any, Protocol

from prometheus_client import CollectorRegistry, Gauge, Histogram

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.config import AppConfig, ExchangeConfig, load_config
from l2_pipeline.feeds.binance import BinanceFeedClient
from l2_pipeline.feeds.connection import BackoffPolicy
from l2_pipeline.feeds.okx import OKXFeedClient
from l2_pipeline.logging_setup import configure_logging
from l2_pipeline.metrics import (
    FeedRegistration,
    PipelineCollector,
    build_feed_lag_gauge,
    build_processing_latency_histogram,
    start_metrics_server,
)
from l2_pipeline.sinks.parquet_sink import BoundedRowQueue, ParquetSink
from l2_pipeline.supervisor import FeedSupervisor

logger = logging.getLogger(__name__)

DEFAULT_SUPERVISOR_HEARTBEAT_INTERVAL_SECONDS = 30.0
DEFAULT_MAX_RESTARTS = 5
DEFAULT_RESTART_WINDOW_SECONDS = 300.0


class _FeedClient(Protocol):
    async def run(self) -> None: ...
    def get_stats(self) -> dict[str, Any]: ...


def _build_client(
    exchange: ExchangeConfig,
    engine: BookEngine,
    row_queue: BoundedRowQueue,
    processing_latency: Histogram,
    feed_lag: Gauge,
) -> _FeedClient:
    symbol = exchange.symbols[0]
    if exchange.name == "binance":
        return BinanceFeedClient(
            symbol,
            engine,
            row_queue=row_queue,
            processing_latency=processing_latency,
            feed_lag=feed_lag,
        )
    if exchange.name == "okx":
        return OKXFeedClient(
            symbol,
            engine,
            row_queue=row_queue,
            processing_latency=processing_latency,
            feed_lag=feed_lag,
        )
    raise ValueError(f"unsupported exchange {exchange.name!r}")


async def _supervisor_heartbeat_worker(
    supervisor: FeedSupervisor,
    sink: ParquetSink,
    exchange_names: list[str],
    interval_seconds: float,
) -> None:
    """Pipeline-wide liveness signal, distinct from each feed client's own
    per-connection heartbeat (which stays -- this adds visibility into the
    parts a single feed client can't see: queue backpressure, sink
    throughput, and every other feed's supervisor-tracked state).
    """
    while True:
        await asyncio.sleep(interval_seconds)
        sink_stats = sink.get_stats()
        logger.info(
            "pipeline heartbeat",
            extra={
                "extra_fields": {
                    "heartbeat": True,
                    "feed_states": {
                        name: supervisor.feed_state(name).value for name in exchange_names
                    },
                    "feed_restart_counts": {
                        name: supervisor.restart_count(name) for name in exchange_names
                    },
                    "queue_depth": sink_stats["queue_depth"],
                    "rows_dropped": sink_stats["rows_dropped"],
                    "rows_written": sink_stats["counters"].get("rows_written", 0),
                    "batches_flushed": sink_stats["counters"].get("batches_flushed", 0),
                }
            },
        )


async def _run_pipeline(config: AppConfig) -> None:
    metrics_registry = CollectorRegistry()
    processing_latency = build_processing_latency_histogram(metrics_registry)
    feed_lag = build_feed_lag_gauge(metrics_registry)

    row_queue = BoundedRowQueue(maxsize=config.output.queue_maxsize)
    sink = ParquetSink(row_queue, config.output.parquet_dir, depth_levels=config.book.depth_levels)

    supervisor = FeedSupervisor(
        BackoffPolicy(),
        random.Random(),
        max_restarts=DEFAULT_MAX_RESTARTS,
        restart_window_seconds=DEFAULT_RESTART_WINDOW_SECONDS,
    )
    supervisor.set_sink(sink.run)

    exchange_names = [exchange.name for exchange in config.exchanges]
    feed_registrations: list[FeedRegistration] = []
    for exchange in config.exchanges:
        engine = BookEngine(depth_levels=config.book.depth_levels)
        client = _build_client(exchange, engine, row_queue, processing_latency, feed_lag)
        supervisor.add_feed(exchange.name, client.run)
        feed_registrations.append(FeedRegistration(exchange.name, exchange.symbols[0], client))

    metrics_registry.register(PipelineCollector(feed_registrations, sink, supervisor))
    # Started before any feed task exists (supervisor.run(), below, is what
    # actually creates them) so a scrape during startup never hits a
    # connection refused instead of an empty-but-valid response.
    metrics_server, metrics_thread = start_metrics_server(
        config.metrics.prometheus_port, metrics_registry
    )
    logger.info(
        "metrics server started",
        extra={"extra_fields": {"port": config.metrics.prometheus_port}},
    )

    heartbeat_task = asyncio.create_task(
        _supervisor_heartbeat_worker(
            supervisor, sink, exchange_names, DEFAULT_SUPERVISOR_HEARTBEAT_INTERVAL_SECONDS
        )
    )

    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        logger.info("shutdown requested", extra={"extra_fields": {"exchanges": exchange_names}})
        supervisor.request_shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except NotImplementedError:
            # Windows: add_signal_handler isn't supported (POSIX-only
            # asyncio feature). Ctrl+C raises KeyboardInterrupt instead,
            # caught in main() below; SIGTERM isn't actionable on Windows
            # regardless. On Linux this loop handles both signals cleanly
            # via supervisor.request_shutdown(), which lets ParquetSink
            # finalize its writers instead of being killed mid-write.
            pass

    try:
        await supervisor.run()
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        # shutdown() blocks until serve_forever()'s poll loop notices (up to
        # its poll_interval) -- run off the event loop thread so a graceful
        # shutdown never stalls it, even briefly.
        await loop.run_in_executor(None, metrics_server.shutdown)
        metrics_server.server_close()
        metrics_thread.join(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser(description="L2 order book ingestion pipeline")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/default.yaml"),
        help="Path to config YAML",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    configure_logging(config.logging.level, config.logging.format)
    logger.info(
        "config loaded",
        extra={"extra_fields": {"exchanges": [e.name for e in config.exchanges]}},
    )

    try:
        asyncio.run(_run_pipeline(config))
    except KeyboardInterrupt:
        logger.info("interrupted, shutting down")


if __name__ == "__main__":
    main()
