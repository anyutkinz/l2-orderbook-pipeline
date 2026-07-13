from __future__ import annotations

import asyncio

import pytest

from l2_pipeline.feeds.connection import BackoffPolicy
from l2_pipeline.supervisor import FeedState, FeedSupervisor


class _ZeroRng:
    """uniform(a, b) always returns the lower bound, so full_jitter_delay
    is always 0 -- restart backoff doesn't slow the tests down without
    needing to fake asyncio.sleep itself."""

    def uniform(self, a: float, b: float) -> float:
        return a


# P4: supervisor isolation
async def test_p4_flaky_feed_permanently_fails_without_affecting_sibling() -> None:
    attempts = 0

    async def flaky() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("boom")

    healthy_started = asyncio.Event()

    async def healthy() -> None:
        healthy_started.set()
        await asyncio.Event().wait()  # blocks until cancelled by shutdown

    async def sink() -> None:
        await asyncio.Event().wait()  # blocks until cancelled by shutdown

    supervisor = FeedSupervisor(
        BackoffPolicy(), _ZeroRng(), max_restarts=2, restart_window_seconds=9999.0
    )
    supervisor.add_feed("flaky", flaky)
    supervisor.add_feed("healthy", healthy)
    supervisor.set_sink(sink)

    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(healthy_started.wait(), timeout=1.0)

    for _ in range(1000):
        if supervisor.feed_state("flaky") is FeedState.PERMANENTLY_FAILED:
            break
        await asyncio.sleep(0)
    else:
        raise AssertionError("flaky feed never reached PERMANENTLY_FAILED")

    # initial attempt + 2 restarts = 3 calls; sibling and sink untouched
    assert attempts == 3
    assert supervisor.restart_count("flaky") == 2
    assert supervisor.feed_state("healthy") is FeedState.RUNNING
    assert not run_task.done()

    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=1.0)


async def test_p4_feed_recovers_within_restart_budget() -> None:
    attempts = 0

    async def recovers_on_second_try() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient")
        await asyncio.Event().wait()  # "recovered": now runs until cancelled

    async def sink() -> None:
        await asyncio.Event().wait()

    supervisor = FeedSupervisor(
        BackoffPolicy(), _ZeroRng(), max_restarts=5, restart_window_seconds=9999.0
    )
    supervisor.add_feed("feed", recovers_on_second_try)
    supervisor.set_sink(sink)

    run_task = asyncio.create_task(supervisor.run())

    for _ in range(1000):
        if attempts == 2:
            break
        await asyncio.sleep(0)
    else:
        raise AssertionError("feed never restarted a second time")

    await asyncio.sleep(0)  # let state settle to RUNNING after the restart
    assert supervisor.feed_state("feed") is FeedState.RUNNING
    assert supervisor.restart_count("feed") == 1

    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=1.0)


# P5: sink criticality
async def test_p5_sink_crash_triggers_full_shutdown_of_all_feeds() -> None:
    feed_cancelled = asyncio.Event()

    async def feed() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            feed_cancelled.set()
            raise

    async def failing_sink() -> None:
        raise RuntimeError("disk full")

    supervisor = FeedSupervisor(BackoffPolicy(), _ZeroRng())
    supervisor.add_feed("feed", feed)
    supervisor.set_sink(failing_sink)

    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(run_task, timeout=1.0)

    assert feed_cancelled.is_set()


async def test_p5_sink_is_never_restarted() -> None:
    sink_calls = 0

    async def failing_sink() -> None:
        nonlocal sink_calls
        sink_calls += 1
        raise RuntimeError("disk full")

    async def feed() -> None:
        await asyncio.Event().wait()

    supervisor = FeedSupervisor(BackoffPolicy(), _ZeroRng())
    supervisor.add_feed("feed", feed)
    supervisor.set_sink(failing_sink)

    await asyncio.wait_for(supervisor.run(), timeout=1.0)

    assert sink_calls == 1


async def test_run_raises_if_sink_not_set() -> None:
    supervisor = FeedSupervisor(BackoffPolicy(), _ZeroRng())
    with pytest.raises(RuntimeError):
        await supervisor.run()
