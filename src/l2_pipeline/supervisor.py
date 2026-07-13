from __future__ import annotations

import asyncio
import enum
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from l2_pipeline.feeds.connection import BackoffPolicy, RandomLike, full_jitter_delay

logger = logging.getLogger(__name__)


class FeedState(enum.Enum):
    RUNNING = "running"
    RESTARTING = "restarting"
    PERMANENTLY_FAILED = "permanently_failed"
    STOPPED = "stopped"


@dataclass(slots=True)
class _FeedEntry:
    name: str
    factory: Callable[[], Awaitable[None]]
    state: FeedState = FeedState.RUNNING
    restart_count: int = 0
    restart_times: list[float] = field(default_factory=list)
    task: asyncio.Task[None] | None = None


class FeedSupervisor:
    """Owns every feed task plus the one sink task, with deliberately
    different failure semantics for each -- which is exactly why this
    isn't asyncio.TaskGroup: TaskGroup cancels every sibling the instant
    one task raises, but a single feed crashing (network blip, exchange
    hiccup) should never take down the others.

    Feed failures are expected and get restarted with full-jitter backoff,
    up to max_restarts within a rolling restart_window_seconds window, after
    which the feed is marked PERMANENTLY_FAILED and left dead -- the other
    feeds and the sink keep running (failure isolation is the whole point).

    The sink failing is different: Parquet writes are the only reason this
    process exists, so a sink crash is treated as process-critical. It is
    never restarted; instead it triggers a full graceful shutdown of every
    feed, rather than silently continuing to drop rows into a queue nobody
    is draining.
    """

    def __init__(
        self,
        backoff_policy: BackoffPolicy,
        rng: RandomLike,
        max_restarts: int = 5,
        restart_window_seconds: float = 300.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._backoff_policy = backoff_policy
        self._rng = rng
        self._max_restarts = max_restarts
        self._restart_window = restart_window_seconds
        self._clock = clock or time.monotonic
        self._feeds: dict[str, _FeedEntry] = {}
        self._sink_factory: Callable[[], Awaitable[None]] | None = None
        self._shutdown_event = asyncio.Event()

    def add_feed(self, name: str, factory: Callable[[], Awaitable[None]]) -> None:
        self._feeds[name] = _FeedEntry(name=name, factory=factory)

    def set_sink(self, factory: Callable[[], Awaitable[None]]) -> None:
        self._sink_factory = factory

    def feed_state(self, name: str) -> FeedState:
        return self._feeds[name].state

    def restart_count(self, name: str) -> int:
        return self._feeds[name].restart_count

    def request_shutdown(self) -> None:
        self._shutdown_event.set()

    async def run(self) -> None:
        if self._sink_factory is None:
            raise RuntimeError("set_sink() must be called before run()")

        sink_task = asyncio.create_task(self._run_sink(self._sink_factory))
        for entry in self._feeds.values():
            entry.task = asyncio.create_task(self._run_feed(entry))

        await self._shutdown_event.wait()

        tasks = [entry.task for entry in self._feeds.values() if entry.task is not None]
        tasks.append(sink_task)
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_feed(self, entry: _FeedEntry) -> None:
        while True:
            try:
                await entry.factory()
                # a clean return means the feed intentionally stopped (e.g.
                # shutdown already under way) -- not a failure, don't restart
                entry.state = FeedState.STOPPED
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("feed crashed", extra={"extra_fields": {"feed": entry.name}})
                now = self._clock()
                entry.restart_times = [
                    t for t in entry.restart_times if now - t < self._restart_window
                ]
                entry.restart_times.append(now)
                if len(entry.restart_times) > self._max_restarts:
                    entry.state = FeedState.PERMANENTLY_FAILED
                    logger.error(
                        "feed exceeded max_restarts, giving up permanently",
                        extra={
                            "extra_fields": {
                                "feed": entry.name,
                                "max_restarts": self._max_restarts,
                                "restart_window_seconds": self._restart_window,
                            }
                        },
                    )
                    return
                entry.restart_count += 1
                entry.state = FeedState.RESTARTING
                delay = full_jitter_delay(self._backoff_policy, entry.restart_count - 1, self._rng)
                await asyncio.sleep(delay)
                entry.state = FeedState.RUNNING

    async def _run_sink(self, factory: Callable[[], Awaitable[None]]) -> None:
        try:
            await factory()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("sink crashed -- process-critical, triggering full shutdown")
        finally:
            self._shutdown_event.set()
