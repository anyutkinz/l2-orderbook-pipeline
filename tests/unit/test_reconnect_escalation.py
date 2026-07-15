from __future__ import annotations

import asyncio
import json
import random

import pytest
from _fake_transport import (
    FakeHttpClient,
    FakeHttpResponse,
    always_failing_connector,
    scripted_connector,
)

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.feeds.binance import BinanceFeedClient
from l2_pipeline.feeds.connection import BackoffPolicy, ReconnectBudgetExhausted
from l2_pipeline.feeds.okx import OKXFeedClient
from l2_pipeline.feeds.ratelimit import TokenBucket
from l2_pipeline.supervisor import FeedState, FeedSupervisor

_EMPTY_SNAPSHOT = FakeHttpResponse(200, {"lastUpdateId": 0, "bids": [], "asks": []})


class _ZeroRng:
    """uniform(a, b) always returns the lower bound (0), so every backoff
    delay -- both ConnectionManager's and FeedSupervisor's -- collapses to
    0 and tests can poll with `await asyncio.sleep(0)` instead of racing
    real timers."""

    def uniform(self, a: float, b: float) -> float:
        return a


# M9: FeedSupervisor escalation for a feed whose own reconnect loop never succeeds
@pytest.mark.asyncio
async def test_binance_raises_reconnect_budget_exhausted_after_consecutive_failures() -> None:
    """Reproduces the exact soak-log symptom at the client level: a
    connector that never succeeds (the connect phase itself failing, same
    shape as the live gaierror storm) must not let the feed retry forever
    silently -- run() has to eventually raise so something outside the
    feed can notice."""
    engine = BookEngine(depth_levels=20)
    client = BinanceFeedClient(
        "BTCUSDT",
        engine,
        ws_connector=always_failing_connector(),
        http_client=FakeHttpClient(default=_EMPTY_SNAPSHOT),
        backoff_policy=BackoffPolicy(base_seconds=0.001, cap_seconds=0.002),
        max_consecutive_reconnect_failures=3,
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    with pytest.raises(ReconnectBudgetExhausted):
        await asyncio.wait_for(task, timeout=2.0)

    stats = client.get_stats()["counters"]
    assert stats["ws_disconnected"] == 3
    assert stats["reconnect_budget_exhausted"] == 1


@pytest.mark.asyncio
async def test_okx_raises_reconnect_budget_exhausted_after_consecutive_failures() -> None:
    """Same escalation, OKX side -- proves the fix isn't Binance-only,
    closing the exact asymmetry the incident review flagged."""
    engine = BookEngine(depth_levels=20)
    client = OKXFeedClient(
        "BTC-USDT",
        engine,
        ws_connector=always_failing_connector(),
        backoff_policy=BackoffPolicy(base_seconds=0.001, cap_seconds=0.002),
        max_consecutive_reconnect_failures=3,
        # High-capacity bucket: this test is about the reconnect-failure
        # escalation, not OKX's separate per-IP connection-rate limit.
        connection_bucket=TokenBucket(100.0, 100.0, lambda: 0.0),
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    with pytest.raises(ReconnectBudgetExhausted):
        await asyncio.wait_for(task, timeout=2.0)

    stats = client.get_stats()["counters"]
    assert stats["ws_disconnected"] == 3
    assert stats["reconnect_budget_exhausted"] == 1


def _diff_message(u: int, final_u: int) -> str:
    return json.dumps(
        {"stream": "btcusdt@depth@100ms", "data": {"U": u, "u": final_u, "b": [], "a": []}}
    )


@pytest.mark.asyncio
async def test_supervisor_forces_full_restart_and_feed_recovers_with_fresh_client() -> None:
    """End-to-end reproduction of the M9 fix, mirroring app.py's real
    wiring: FeedSupervisor's factory is called again after
    ReconnectBudgetExhausted, must build a genuinely fresh client (not
    replay run() on the torn-down one), and that fresh client is what
    actually lets the feed recover -- exactly the two-part gap the live
    incident exposed (restart never triggered, and wouldn't have worked
    correctly if it had)."""
    engine = BookEngine(depth_levels=20)
    built_clients: list[BinanceFeedClient] = []

    def _build() -> BinanceFeedClient:
        # First construction is permanently wedged (mirrors the live
        # incident); every subsequent one (i.e. after a forced restart)
        # gets a connector that actually delivers a message, proving the
        # *new* instance -- not the old one retrying -- is what recovers.
        connector = (
            always_failing_connector()
            if not built_clients
            else scripted_connector([_diff_message(1, 1)])
        )
        client = BinanceFeedClient(
            "BTCUSDT",
            engine,
            ws_connector=connector,
            http_client=FakeHttpClient(default=_EMPTY_SNAPSHOT),
            backoff_policy=BackoffPolicy(base_seconds=0.0, cap_seconds=0.0),
            max_consecutive_reconnect_failures=3,
            rng=_ZeroRng(),
        )
        built_clients.append(client)
        return client

    async def factory() -> None:
        await _build().run()

    async def sink() -> None:
        await asyncio.Event().wait()

    supervisor = FeedSupervisor(
        BackoffPolicy(base_seconds=0.0, cap_seconds=0.0),
        _ZeroRng(),
        max_restarts=5,
        restart_window_seconds=9999.0,
    )
    supervisor.add_feed("binance", factory)
    supervisor.set_sink(sink)

    run_task = asyncio.create_task(supervisor.run())

    for _ in range(5000):
        if built_clients and built_clients[-1].get_stats()["counters"].get(
            "messages_received", 0
        ):
            break
        await asyncio.sleep(0)
    else:
        raise AssertionError("feed never recovered after the forced restart")

    assert len(built_clients) == 2  # the wedged instance, then one fresh restart
    assert supervisor.restart_count("binance") == 1
    assert supervisor.feed_state("binance") is FeedState.RUNNING
    # the wedged first instance actually hit escalation, not some other exit
    assert built_clients[0].get_stats()["counters"]["reconnect_budget_exhausted"] == 1

    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=1.0)
