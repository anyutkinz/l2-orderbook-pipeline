from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from pathlib import Path

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.config import AppConfig, load_config
from l2_pipeline.feeds.binance import BinanceFeedClient
from l2_pipeline.logging_setup import configure_logging

logger = logging.getLogger(__name__)


async def _run_binance_feed(config: AppConfig) -> None:
    exchange = next((e for e in config.exchanges if e.name == "binance"), None)
    if exchange is None:
        raise ValueError("config has no 'binance' exchange entry")
    symbol = exchange.symbols[0]

    engine = BookEngine(depth_levels=config.book.depth_levels)
    client = BinanceFeedClient(symbol, engine)

    task = asyncio.create_task(client.run())
    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        logger.info("shutdown requested", extra={"extra_fields": {"symbol": symbol}})
        task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except NotImplementedError:
            # Windows: add_signal_handler isn't supported (POSIX-only
            # asyncio feature). Ctrl+C raises KeyboardInterrupt instead,
            # caught in main() below; SIGTERM isn't actionable on Windows
            # regardless. On Linux this loop handles both signals cleanly
            # via task cancellation, without relying on KeyboardInterrupt.
            pass

    try:
        await task
    except asyncio.CancelledError:
        pass


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
        asyncio.run(_run_binance_feed(config))
    except KeyboardInterrupt:
        logger.info("interrupted, shutting down")


if __name__ == "__main__":
    main()
