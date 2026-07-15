from __future__ import annotations

import asyncio
import contextlib
import json
import random
from decimal import Decimal

import pytest
from _fake_transport import FakeHttpClient, FakeHttpResponse, scripted_connector

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.book.types import BookState
from l2_pipeline.feeds.binance import BinanceFeedClient


def _diff_message(u: int, final_u: int, bids: list[list[str]], asks: list[list[str]]) -> str:
    return json.dumps(
        {
            "stream": "btcusdt@depth@100ms",
            "data": {
                "e": "depthUpdate",
                "s": "BTCUSDT",
                "U": u,
                "u": final_u,
                "b": bids,
                "a": asks,
            },
        }
    )


@pytest.mark.asyncio
async def test_t5_gap_scenario_converges_with_correct_incidents() -> None:
    # msg1: prev_id=100, final_id=101
    # msg2: prev_id=101, final_id=102
    # msg3: prev_id=103, final_id=104  -- U=103 (final_id=103) is missing
    #       from the wire, simulating a genuinely dropped event
    messages = [
        _diff_message(101, 101, [["100.00", "1.0"]], []),
        _diff_message(102, 102, [], [["101.00", "2.0"]]),
        _diff_message(104, 104, [["99.00", "3.0"]], []),
    ]

    # A 0.1s delay on every REST response is what makes this deterministic:
    # it guarantees all 3 scripted WS messages are already buffered before
    # the first snapshot fetch resolves, rather than racing an unspecified
    # asyncio scheduling order.
    responses = [
        FakeHttpResponse(200, {"lastUpdateId": 100, "bids": [], "asks": []}),
        FakeHttpResponse(200, {"lastUpdateId": 103, "bids": [], "asks": []}),
    ]
    http_client = FakeHttpClient(responses=responses, delay_seconds=0.1)

    engine = BookEngine(depth_levels=20)
    client = BinanceFeedClient(
        "BTCUSDT",
        engine,
        watchdog_timeout_seconds=5.0,
        ws_connector=scripted_connector(messages),
        http_client=http_client,
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.6)  # 2 sequential 0.1s-delayed fetches, generous margin
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    # Converged to the post-msg3 state despite the injected gap. msg1's bid
    # and msg2's ask are legitimately gone, not just msg3's -- detecting
    # the gap on msg3 discards ALL prior state (per M1: full resync only,
    # never patch), and the second snapshot was empty. Only msg3's own
    # effect, applied fresh on top of that empty snapshot, survives.
    assert engine.state is BookState.LIVE
    assert engine.last_applied_id == 104
    bids, asks = engine.full_book()
    assert bids == {Decimal("99.00"): Decimal("3.0")}
    assert asks == {}

    stats = client.get_stats()["counters"]
    assert stats["gap_detected"] == 1
    assert stats["resync_completed"] == 1
    assert len(http_client.calls) == 2
    assert stats.get("watchdog_tripped", 0) == 0
    assert stats.get("malformed_message", 0) == 0


@pytest.mark.asyncio
async def test_t9_network_error_on_snapshot_fetch_does_not_kill_resync_worker() -> None:
    """Regression for the M9 incident: a network-level failure fetching
    the REST snapshot (DNS, connection reset -- the same disconnect class
    that hits the WS side during a real outage) must be retried, not left
    to propagate unhandled out of _fetch_snapshot() -> _perform_resync()
    -> _resync_worker(), which would silently kill that fire-and-forget
    task for the rest of the process's life. Scripts 3 network failures
    then a working response: the resync must still complete."""
    messages = [_diff_message(1, 1, [["100.00", "1.0"]], [])]
    http_client = FakeHttpClient(
        fail_first_n=3,
        default=FakeHttpResponse(200, {"lastUpdateId": 0, "bids": [], "asks": []}),
    )

    engine = BookEngine(depth_levels=20)
    client = BinanceFeedClient(
        "BTCUSDT",
        engine,
        watchdog_timeout_seconds=5.0,
        ws_connector=scripted_connector(messages),
        http_client=http_client,
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.2)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert engine.state is BookState.LIVE
    stats = client.get_stats()["counters"]
    assert stats["resync_completed"] == 1
    assert stats["snapshot_fetch_network_error"] == 3
    assert len(http_client.calls) == 4


@pytest.mark.asyncio
async def test_t5_malformed_message_is_skipped_and_logged() -> None:
    messages = [
        _diff_message(1, 1, [["100.00", "1.0"]], []),
        '{"stream":"btcusdt@depth@100ms","data":{"U":2}}',  # missing 'u', 'b', 'a'
        _diff_message(2, 2, [["100.00", "2.0"]], []),
    ]
    http_client = FakeHttpClient(
        default=FakeHttpResponse(200, {"lastUpdateId": 0, "bids": [], "asks": []})
    )

    engine = BookEngine(depth_levels=20)
    client = BinanceFeedClient(
        "BTCUSDT",
        engine,
        watchdog_timeout_seconds=5.0,
        ws_connector=scripted_connector(messages),
        http_client=http_client,
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.2)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    # the malformed message is skipped; the next valid one (prev_id=1) still
    # chains correctly onto msg1 (final_id=1) with no gap -- proving skip,
    # don't crash, don't spuriously report a gap where the chain is intact
    assert engine.state is BookState.LIVE
    assert engine.last_applied_id == 2
    stats = client.get_stats()["counters"]
    assert stats["malformed_message"] == 1
    assert stats.get("gap_detected", 0) == 0
