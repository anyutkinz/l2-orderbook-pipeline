from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from pathlib import Path
from typing import Protocol

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.config import AppConfig, ExchangeConfig, load_config
from l2_pipeline.feeds.binance import BinanceFeedClient
from l2_pipeline.feeds.okx import OKXFeedClient
from l2_pipeline.logging_setup import configure_logging

logger = logging.getLogger(__name__)


class _FeedClient(Protocol):
    async def run(self) -> None: ...


def _build_client(exchange: ExchangeConfig, engine: BookEngine) -> _FeedClient:
    symbol = exchange.symbols[0]
    if exchange.name == "binance":
        return BinanceFeedClient(symbol, engine)
    if exchange.name == "okx":
        return OKXFeedClient(symbol, engine)
    raise ValueError(f"unsupported exchange {exchange.name!r}")


async def _run_feeds(config: AppConfig) -> None:
    clients = [
        _build_client(exchange, BookEngine(depth_levels=config.book.depth_levels))
        for exchange in config.exchanges
    ]
    tasks = [asyncio.create_task(client.run()) for client in clients]
    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        logger.info(
            "shutdown requested",
            extra={"extra_fields": {"exchanges": [e.name for e in config.exchanges]}},
        )
        for task in tasks:
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

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for exchange, result in zip(config.exchanges, results, strict=True):
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
            logger.error(
                "feed client exited with an unhandled exception",
                extra={
                    "extra_fields": {
                        "exchange": exchange.name,
                        "error": f"{type(result).__name__}: {result}",
                    }
                },
            )


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
        asyncio.run(_run_feeds(config))
    except KeyboardInterrupt:
        logger.info("interrupted, shutting down")


if __name__ == "__main__":
    main()
