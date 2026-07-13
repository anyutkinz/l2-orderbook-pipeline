from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
from decimal import Decimal

import pytest
from _fake_transport import FakeOKXWebSocket, okx_connector

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.book.types import BookState
from l2_pipeline.feeds.connection import BackoffPolicy
from l2_pipeline.feeds.okx import OKXFeedClient
from l2_pipeline.feeds.ratelimit import TokenBucket


def _level(price: str, qty: str) -> list[str]:
    return [price, qty, "0", "1"]


def _snapshot_message(
    seq_id: int,
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
) -> str:
    return json.dumps(
        {
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [
                {
                    "asks": [_level(p, q) for p, q in (asks or [])],
                    "bids": [_level(p, q) for p, q in (bids or [])],
                    "ts": "1783786805808",
                    "checksum": 0,
                    "seqId": seq_id,
                    "prevSeqId": -1,
                }
            ],
        }
    )


def _update_message(
    seq_id: int,
    prev_seq_id: int,
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
) -> str:
    return json.dumps(
        {
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "update",
            "data": [
                {
                    "asks": [_level(p, q) for p, q in (asks or [])],
                    "bids": [_level(p, q) for p, q in (bids or [])],
                    "ts": "1783786805808",
                    "checksum": 0,
                    "seqId": seq_id,
                    "prevSeqId": prev_seq_id,
                }
            ],
        }
    )


async def _cancel(task: asyncio.Task[None]) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# U2: ping/pong keepalive
@pytest.mark.asyncio
async def test_ping_pong_keeps_connection_alive_no_incident() -> None:
    def on_send(msg: str) -> list[str]:
        return ["pong"] if msg == "ping" else []

    ws = FakeOKXWebSocket(on_send=on_send)
    engine = BookEngine(depth_levels=20)
    client = OKXFeedClient(
        "BTC-USDT",
        engine,
        ping_interval_seconds=0.05,
        pong_timeout_seconds=0.05,
        ws_connector=okx_connector(ws),
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.3)
    await _cancel(task)

    stats = client.get_stats()["counters"]
    assert stats.get("watchdog_tripped", 0) == 0
    assert stats.get("malformed_message", 0) == 0
    assert "ping" in ws.sent


@pytest.mark.asyncio
async def test_no_pong_response_trips_watchdog() -> None:
    ws = FakeOKXWebSocket()  # no on_send -- ping sent, never answered

    engine = BookEngine(depth_levels=20)
    client = OKXFeedClient(
        "BTC-USDT",
        engine,
        ping_interval_seconds=0.05,
        pong_timeout_seconds=0.05,
        backoff_policy=BackoffPolicy(base_seconds=0.01, cap_seconds=0.02),
        ws_connector=okx_connector(ws),
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.3)
    await _cancel(task)

    stats = client.get_stats()["counters"]
    assert stats["watchdog_tripped"] >= 1
    assert "ping" in ws.sent


# U3: snapshot delivered over WS (not REST) + mid-stream gap -> resubscribe -> recovery
@pytest.mark.asyncio
async def test_u3_snapshot_over_ws_and_gap_triggers_resubscribe_recovery() -> None:
    resubscribe_calls: list[str] = []

    def on_send(msg: str) -> list[str]:
        parsed = json.loads(msg)
        if parsed["op"] == "subscribe":
            resubscribe_calls.append(msg)
            if len(resubscribe_calls) == 1:
                return [_snapshot_message(100, bids=[("100.0", "1.0")], asks=[])]
            # buffered gap event has prev_id=104, final_id=105 -- straddle
            # requires last_update_id in [104, 105), i.e. exactly 104
            return [_snapshot_message(104, bids=[("99.0", "2.0")], asks=[])]
        return []

    ws = FakeOKXWebSocket(on_send=on_send)
    engine = BookEngine(depth_levels=20)
    client = OKXFeedClient(
        "BTC-USDT",
        engine,
        ping_interval_seconds=5.0,
        pong_timeout_seconds=5.0,
        ws_connector=okx_connector(ws),
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.05)
    assert engine.state is BookState.LIVE
    assert engine.last_applied_id == 100

    ws.enqueue(_update_message(101, 100, bids=[], asks=[("101.0", "1.0")]))
    await asyncio.sleep(0.05)
    assert engine.last_applied_id == 101

    # gap: expects prev=101, this delivers prev=104
    ws.enqueue(_update_message(105, 104, bids=[("98.0", "0.5")], asks=[]))
    await asyncio.sleep(0.1)

    await _cancel(task)

    assert engine.state is BookState.LIVE
    assert engine.last_applied_id == 105
    bids, asks = engine.full_book()
    # msg1's bid (100.0) and msg2's ask (101.0) are discarded on gap (full
    # resync, not patch) -- but the fresh snapshot's own level (99.0) AND
    # the straddling buffered event's own level (98.0) both survive: the
    # first survivor is applied via apply_levels() on top of the snapshot,
    # not a replacement of it (M1's boundary-straddle behavior, not a bug)
    assert bids == {Decimal("99.0"): Decimal("2.0"), Decimal("98.0"): Decimal("0.5")}
    assert asks == {}

    stats = client.get_stats()["counters"]
    assert stats["gap_detected"] == 1
    assert stats["resync_completed"] == 2  # cold start + gap recovery
    assert len(resubscribe_calls) == 2
    assert any(json.loads(m)["op"] == "unsubscribe" for m in ws.sent)


# U4: resubscribe throttling under a tiny TokenBucket
@pytest.mark.asyncio
async def test_u4_resubscribe_throttled_not_dropped() -> None:
    call_count = 0

    def on_send(msg: str) -> list[str]:
        nonlocal call_count
        parsed = json.loads(msg)
        if parsed["op"] != "subscribe":
            return []
        call_count += 1
        if call_count == 1:
            return [_snapshot_message(100, bids=[], asks=[])]  # cold start
        if call_count == 2:
            # matches update(150, 149) below -- straddle needs exactly 149
            return [_snapshot_message(149, bids=[], asks=[])]
        # matches update(200, 199) below -- straddle needs exactly 199
        return [_snapshot_message(199, bids=[], asks=[])]

    ws = FakeOKXWebSocket(on_send=on_send)
    engine = BookEngine(depth_levels=20)
    tiny_bucket = TokenBucket(capacity=2.0, refill_rate_per_sec=20.0, clock=time.monotonic)
    client = OKXFeedClient(
        "BTC-USDT",
        engine,
        ping_interval_seconds=5.0,
        pong_timeout_seconds=5.0,
        resubscribe_bucket=tiny_bucket,
        ws_connector=okx_connector(ws),
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.05)
    assert engine.state is BookState.LIVE  # cold start, doesn't touch resubscribe_bucket

    # first gap-triggered resubscribe: bucket is full (capacity=2, cost=2) -> immediate
    ws.enqueue(_update_message(150, 149, bids=[], asks=[]))
    await asyncio.sleep(0.05)
    assert engine.state is BookState.LIVE
    assert client.get_stats()["counters"].get("resubscribe_throttled", 0) == 0

    # second gap-triggered resubscribe: bucket now empty -> must wait for refill
    ws.enqueue(_update_message(200, 199, bids=[], asks=[]))
    for _ in range(50):
        if client.get_stats()["counters"].get("resubscribe_throttled", 0) > 0:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("expected RESUBSCRIBE_THROTTLED to fire")

    # not dropped -- it still eventually converges
    for _ in range(50):
        if engine.state is BookState.LIVE and engine.last_applied_id == 200:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("throttled resubscribe never completed")

    await _cancel(task)

    assert engine.state is BookState.LIVE
    assert engine.last_applied_id == 200
    assert client.get_stats()["counters"]["resubscribe_throttled"] >= 1


# U5: service notice -> proactive reconnect, standard invalidate+resync cycle
@pytest.mark.asyncio
async def test_u5_service_notice_triggers_proactive_reconnect() -> None:
    subscribe_count = 0

    def on_send(msg: str) -> list[str]:
        nonlocal subscribe_count
        parsed = json.loads(msg)
        if parsed["op"] != "subscribe":
            return []
        subscribe_count += 1
        return [_snapshot_message(100, bids=[("100.0", "1.0")], asks=[])]

    ws = FakeOKXWebSocket(on_send=on_send)
    engine = BookEngine(depth_levels=20)
    client = OKXFeedClient(
        "BTC-USDT",
        engine,
        ping_interval_seconds=5.0,
        pong_timeout_seconds=5.0,
        backoff_policy=BackoffPolicy(base_seconds=0.01, cap_seconds=0.02),
        ws_connector=okx_connector(ws),
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.05)
    assert engine.state is BookState.LIVE
    assert subscribe_count == 1

    notice = json.dumps({"event": "notice", "code": "64008", "msg": "system upgrade"})
    ws.enqueue(notice)
    for _ in range(50):
        if subscribe_count == 2:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("expected a second subscribe after the service notice")
    await asyncio.sleep(0.05)

    await _cancel(task)

    assert engine.state is BookState.LIVE
    stats = client.get_stats()["counters"]
    assert stats["okx_service_notice"] == 1
    # no redundant WS_DISCONNECTED logged for the same event
    assert stats.get("ws_disconnected", 0) == 0


# Explicit retry-counter semantics: reset on APPLIED, incremented per
# failed attempt, forces reconnect at the limit, and -- the specific
# property required -- never leaks across episodes (a resync storm later
# gets its own full budget, not whatever was left over from an earlier one).
@pytest.mark.asyncio
async def test_retry_counter_resets_on_success_and_does_not_leak_across_episodes() -> None:
    call_log: list[str] = []

    def on_send(msg: str) -> list[str]:
        parsed = json.loads(msg)
        if parsed["op"] != "subscribe":
            return []
        call_log.append(msg)
        idx = len(call_log)
        if idx == 1:
            return [_snapshot_message(100, bids=[], asks=[])]  # cold start
        if idx in (2, 3):
            return [_snapshot_message(50, bids=[], asks=[])]  # episode 1: stale x2
        if idx == 4:
            return [_snapshot_message(109, bids=[], asks=[])]  # episode 1: valid, converges
        return [_snapshot_message(50, bids=[], asks=[])]  # episode 2: always stale

    ws = FakeOKXWebSocket(on_send=on_send)
    engine = BookEngine(depth_levels=20)
    client = OKXFeedClient(
        "BTC-USDT",
        engine,
        ping_interval_seconds=5.0,
        pong_timeout_seconds=5.0,
        snapshot_retry_limit=3,
        resubscribe_bucket=TokenBucket(
            100.0, 100.0, time.monotonic
        ),  # generous: not testing throttling here
        backoff_policy=BackoffPolicy(base_seconds=0.01, cap_seconds=0.02),
        ws_connector=okx_connector(ws),
        rng=random.Random(1),
    )

    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.05)
    assert engine.state is BookState.LIVE  # cold start (call 1)

    # episode 1: buffered event prev_id=109, needs 3 attempts (2 stale + 1 valid)
    ws.enqueue(_update_message(110, 109, bids=[("1.0", "1.0")], asks=[]))
    for _ in range(50):
        if len(call_log) >= 4:
            break
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.02)

    assert engine.state is BookState.LIVE
    assert engine.last_applied_id == 110
    assert len(call_log) == 4  # cold start + 3 episode-1 attempts

    # episode 2: a fresh, unrelated gap -- every served snapshot is stale,
    # so this must exhaust its OWN budget of snapshot_retry_limit=3
    ws.enqueue(_update_message(500, 499, bids=[], asks=[]))
    for _ in range(100):
        if ws.closed:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("expected retry-limit exhaustion to close the connection")

    await _cancel(task)

    # exactly 3 more calls in episode 2 -- proving the counter started at 0,
    # not inheriting episode 1's history (which would exhaust in fewer)
    assert len(call_log) == 4 + 3
    stats = client.get_stats()["counters"]
    assert stats["snapshot_retry_limit_exceeded"] == 1
