from __future__ import annotations

import asyncio
import contextlib
import random

import pytest
from _fake_transport import FakeHttpClient, FakeHttpResponse, scripted_connector, stalling_connector

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.feeds.binance import BinanceFeedClient
from l2_pipeline.feeds.connection import BackoffPolicy

_EMPTY_SNAPSHOT = FakeHttpResponse(200, {"lastUpdateId": 1, "bids": [], "asks": []})


@pytest.mark.asyncio
async def test_watchdog_trips_on_stalled_stream() -> None:
    engine = BookEngine(depth_levels=20)
    client = BinanceFeedClient(
        "BTCUSDT",
        engine,
        watchdog_timeout_seconds=0.05,
        ws_connector=stalling_connector,
        http_client=FakeHttpClient(default=_EMPTY_SNAPSHOT),
        backoff_policy=BackoffPolicy(base_seconds=0.01, cap_seconds=0.02),
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.3)  # several watchdog-timeout windows
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    stats = client.get_stats()
    assert stats["counters"]["watchdog_tripped"] >= 1
    # proves BACKOFF was actually entered and a reconnect attempted after
    # the trip, without asserting the exact state at an arbitrary sample
    # point (racy: reconnection is near-instant with these tiny delays)
    assert stats["counters"]["ws_reconnected"] >= 2


@pytest.mark.asyncio
async def test_watchdog_does_not_trip_before_timeout() -> None:
    """Sanity check on the fake itself: a connector that delivers messages
    faster than the watchdog timeout must not trip it."""
    messages = [
        '{"stream":"btcusdt@depth@100ms","data":{"U":1,"u":1,"b":[],"a":[]}}',
    ]
    engine = BookEngine(depth_levels=20)
    client = BinanceFeedClient(
        "BTCUSDT",
        engine,
        watchdog_timeout_seconds=5.0,
        ws_connector=scripted_connector(messages),
        http_client=FakeHttpClient(
            default=FakeHttpResponse(200, {"lastUpdateId": 0, "bids": [], "asks": []})
        ),
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.1)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert client.get_stats()["counters"].get("watchdog_tripped", 0) == 0
