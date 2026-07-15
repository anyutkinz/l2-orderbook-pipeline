from __future__ import annotations

import asyncio
import contextlib
import decimal
import json
import logging
import random
import time
from collections import defaultdict
from collections.abc import Callable
from decimal import Decimal
from typing import Any, Protocol, cast

import httpx
from prometheus_client import Gauge, Histogram

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.book.types import ApplyStatus, BookState, DiffEvent, PriceLevel, SnapshotEvent
from l2_pipeline.feeds.connection import (
    DEFAULT_MAX_CONSECUTIVE_RECONNECT_FAILURES,
    BackoffPolicy,
    ConnectionManager,
    ReconnectBudgetExhausted,
)
from l2_pipeline.feeds.envelope import InstrumentId, TimestampedEvent, build_snapshot_row
from l2_pipeline.feeds.ratelimit import (
    DEFAULT_SNAPSHOT_BUCKET_CAPACITY,
    DEFAULT_SNAPSHOT_BUCKET_REFILL_PER_SEC,
    TokenBucket,
)
from l2_pipeline.feeds.transport import WebSocketConnector, WebSocketLike
from l2_pipeline.sinks.parquet_sink import BoundedRowQueue

logger = logging.getLogger(__name__)

WS_BASE_URL = "wss://stream.binance.com:9443/stream"
REST_BASE_URL = "https://api.binance.com"
DEFAULT_WATCHDOG_TIMEOUT_SECONDS = 10.0
DEFAULT_SNAPSHOT_RETRY_LIMIT = 20
DEFAULT_HTTP_RETRY_LIMIT = 10
DEFAULT_DEPTH_LIMIT = 100  # weight 5 -- see ratelimit.py for the verified weight table
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30.0


class ParseError(Exception):
    """Raised when a raw Binance message can't be parsed into our types.
    Caught by the reader loop, which logs a MALFORMED_MESSAGE incident and
    skips the message -- the resulting gap surfaces naturally through the
    engine's own sequencing check, no special-case handling needed.
    """


class HttpRetryLimitExceeded(RuntimeError):
    pass


def parse_combined_stream_envelope(raw: dict[str, Any]) -> dict[str, Any]:
    """Unwrap the combined-stream {"stream": ..., "data": ...} wrapper."""
    try:
        data = raw["data"]
    except (KeyError, TypeError) as exc:
        raise ParseError(str(exc)) from exc
    if not isinstance(data, dict):
        raise ParseError(f"'data' must be a mapping, got {type(data).__name__}")
    return data


def parse_diff_event(data: dict[str, Any]) -> DiffEvent:
    """Binance spot @depth diff -> DiffEvent. Maps U -> prev_id = U-1 (no
    `pu` field on the spot stream), u -> final_id, per the M1 design.
    """
    try:
        u = int(data["U"])
        final_id = int(data["u"])
        bids = [PriceLevel(Decimal(p), Decimal(q)) for p, q in data["b"]]
        asks = [PriceLevel(Decimal(p), Decimal(q)) for p, q in data["a"]]
    except (KeyError, ValueError, TypeError, decimal.InvalidOperation) as exc:
        raise ParseError(str(exc)) from exc
    return DiffEvent(prev_id=u - 1, final_id=final_id, bids=bids, asks=asks)


def _extract_ts_exchange_ms(data: dict[str, Any]) -> int | None:
    """Binance's own event-time field ("E"), read at the reader-loop call
    site rather than folded into parse_diff_event -- keeps that function's
    signature (and its existing tests) untouched.
    """
    raw = data.get("E")
    return int(raw) if raw is not None else None


def parse_snapshot(data: dict[str, Any]) -> SnapshotEvent:
    """Binance GET /api/v3/depth response -> SnapshotEvent."""
    try:
        last_update_id = int(data["lastUpdateId"])
        bids = [PriceLevel(Decimal(p), Decimal(q)) for p, q in data["bids"]]
        asks = [PriceLevel(Decimal(p), Decimal(q)) for p, q in data["asks"]]
    except (KeyError, ValueError, TypeError, decimal.InvalidOperation) as exc:
        raise ParseError(str(exc)) from exc
    return SnapshotEvent(last_update_id=last_update_id, bids=bids, asks=asks)


class HttpResponseLike(Protocol):
    status_code: int
    headers: Any

    def json(self) -> Any: ...
    def raise_for_status(self) -> None: ...


class HttpClientLike(Protocol):
    async def get(self, url: str, params: dict[str, Any]) -> HttpResponseLike: ...
    async def aclose(self) -> None: ...


class BinanceFeedClient:
    """Production twin of M2's SimulatedFeedDriver: the same control loop
    (GAP_DETECTED / cold start -> fetch snapshot -> load_snapshot, retry
    on SNAPSHOT_STALE) with poll() replaced by a real websocket stream and
    request_snapshot() replaced by a real REST call. See DECISIONS.md for
    the full twin-architecture rationale.
    """

    def __init__(
        self,
        symbol: str,
        engine: BookEngine,
        *,
        update_speed: str = "100ms",
        watchdog_timeout_seconds: float = DEFAULT_WATCHDOG_TIMEOUT_SECONDS,
        snapshot_retry_limit: int = DEFAULT_SNAPSHOT_RETRY_LIMIT,
        http_retry_limit: int = DEFAULT_HTTP_RETRY_LIMIT,
        depth_limit: int = DEFAULT_DEPTH_LIMIT,
        heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        max_consecutive_reconnect_failures: int = DEFAULT_MAX_CONSECUTIVE_RECONNECT_FAILURES,
        backoff_policy: BackoffPolicy | None = None,
        ws_connector: WebSocketConnector | None = None,
        http_client: HttpClientLike | None = None,
        rng: random.Random | None = None,
        clock: Callable[[], float] | None = None,
        row_queue: BoundedRowQueue | None = None,
        processing_latency: Histogram | None = None,
        feed_lag: Gauge | None = None,
    ) -> None:
        self._symbol = symbol
        self._engine = engine
        self._update_speed = update_speed
        self._watchdog_timeout = watchdog_timeout_seconds
        self._snapshot_retry_limit = snapshot_retry_limit
        self._http_retry_limit = http_retry_limit
        self._depth_limit = depth_limit
        self._heartbeat_interval = heartbeat_interval_seconds
        self._max_consecutive_reconnect_failures = max_consecutive_reconnect_failures
        self._row_queue = row_queue
        self._processing_latency = processing_latency
        self._feed_lag = feed_lag

        clock = clock or time.monotonic
        rng = rng or random.Random()
        self._connection = ConnectionManager(backoff_policy or BackoffPolicy(), rng, clock)
        self._token_bucket = TokenBucket(
            DEFAULT_SNAPSHOT_BUCKET_CAPACITY, DEFAULT_SNAPSHOT_BUCKET_REFILL_PER_SEC, clock
        )

        if ws_connector is None:
            import websockets

            ws_connector = websockets.connect
        self._ws_connector: WebSocketConnector = ws_connector

        self._owns_http_client = http_client is None
        self._http_client: HttpClientLike = (
            http_client
            if http_client is not None
            else cast(HttpClientLike, httpx.AsyncClient(base_url=REST_BASE_URL))
        )

        # Not pre-set here: run()'s reconnect loop already calls
        # invalidate() + resync_needed.set() unconditionally on every
        # connection, including the very first one. Pre-setting it here
        # let the resync worker race ahead and complete a trivial
        # empty-buffer resync before the WS handshake even finished,
        # immediately discarded once invalidate() ran anyway -- a real,
        # observed double-resync at cold start (found during the M3
        # manual Ctrl+C acceptance run), not just a theoretical race.
        self._resync_needed = asyncio.Event()
        self._current_ws: WebSocketLike | None = None
        self._buffered_since_resync = 0
        self._stats: dict[str, int] = defaultdict(int)

    def get_stats(self) -> dict[str, Any]:
        return {
            "counters": dict(self._stats),
            "connection_state": self._connection.state.value,
            "connection_attempt": self._connection.attempt,
            "book_state": self._engine.state.value,
            "book_last_applied_id": self._engine.last_applied_id,
        }

    def _raise_if_reconnect_budget_exhausted(self) -> None:
        """Called right after ConnectionManager.disconnected(), whose
        `attempt` counter only resets once a message actually gets through
        on a live connection -- so this only fires for a genuine run of
        consecutive failures, never for isolated disconnects sprinkled
        across an otherwise-healthy run.
        """
        attempt = self._connection.attempt
        if attempt < self._max_consecutive_reconnect_failures:
            return
        self._log(
            logging.ERROR,
            "exceeded consecutive reconnect failures without a single message, "
            "escalating to supervisor for a full restart",
            "RECONNECT_BUDGET_EXHAUSTED",
            consecutive_failures=attempt,
        )
        self._stats["reconnect_budget_exhausted"] += 1
        raise ReconnectBudgetExhausted(f"{attempt} consecutive failed reconnect attempts")

    def _stream_url(self) -> str:
        stream_name = f"{self._symbol.lower()}@depth@{self._update_speed}"
        return f"{WS_BASE_URL}?streams={stream_name}"

    def _log(self, level: int, message: str, incident: str, **fields: Any) -> None:
        logger.log(
            level,
            message,
            extra={"extra_fields": {"incident": incident, "symbol": self._symbol, **fields}},
        )

    async def run(self) -> None:
        resync_task = asyncio.create_task(self._resync_worker())
        heartbeat_task = asyncio.create_task(self._heartbeat_worker())
        try:
            while True:
                self._connection.connecting()
                try:
                    async with self._ws_connector(self._stream_url()) as ws:
                        self._current_ws = ws
                        info = self._connection.connected()
                        self._log(
                            logging.INFO,
                            "websocket connected",
                            "WS_RECONNECTED",
                            attempt=info.attempt,
                            outage_duration_seconds=info.outage_duration_seconds,
                        )
                        self._stats["ws_reconnected"] += 1
                        # Coupling rule: every new connection forces a fresh
                        # sync, regardless of what the engine's own state
                        # currently shows -- missed events during the outage
                        # are near-certain, and we don't rely on the
                        # engine's chain-check to discover that on its own.
                        self._engine.invalidate("reconnect")
                        self._resync_needed.set()
                        await self._reader_loop(ws)
                except asyncio.CancelledError:
                    raise
                except TimeoutError:
                    self._log(
                        logging.WARNING,
                        "no message within watchdog timeout, treating connection as dead",
                        "WATCHDOG_TRIPPED",
                        timeout_seconds=self._watchdog_timeout,
                    )
                    self._stats["watchdog_tripped"] += 1
                    delay = self._connection.disconnected("watchdog_tripped")
                    self._raise_if_reconnect_budget_exhausted()
                    await asyncio.sleep(delay)
                except Exception as exc:
                    reason = f"{type(exc).__name__}: {exc}"
                    self._log(
                        logging.WARNING, "websocket disconnected", "WS_DISCONNECTED", reason=reason
                    )
                    self._stats["ws_disconnected"] += 1
                    delay = self._connection.disconnected(reason)
                    self._raise_if_reconnect_budget_exhausted()
                    await asyncio.sleep(delay)
                finally:
                    self._current_ws = None
        finally:
            resync_task.cancel()
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await resync_task
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            if self._owns_http_client:
                await self._http_client.aclose()
            logger.info("feed client stopped", extra={"extra_fields": {"stats": self.get_stats()}})

    async def _reader_loop(self, ws: WebSocketLike) -> None:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=self._watchdog_timeout)
            ts_local_ns = time.monotonic_ns()
            ts_wall_ns = time.time_ns()
            self._connection.message_received()
            self._stats["messages_received"] += 1

            try:
                envelope = json.loads(raw)
                data = parse_combined_stream_envelope(envelope)
                diff = parse_diff_event(data)
            except (ParseError, json.JSONDecodeError) as exc:
                self._log(
                    logging.WARNING,
                    "skipping malformed message",
                    "MALFORMED_MESSAGE",
                    error=str(exc),
                    raw_truncated=str(raw)[:200],
                )
                self._stats["malformed_message"] += 1
                continue

            timestamped = TimestampedEvent(
                ts_local_ns=ts_local_ns,
                ts_wall_ns=ts_wall_ns,
                instrument=InstrumentId("binance", self._symbol),
                event=diff,
                ts_exchange_ms=_extract_ts_exchange_ms(data),
            )
            logger.debug(
                "event received",
                extra={
                    "extra_fields": {
                        "symbol": self._symbol,
                        "ts_local_ns": timestamped.ts_local_ns,
                        "prev_id": diff.prev_id,
                        "final_id": diff.final_id,
                    }
                },
            )

            result = self._engine.apply_event(timestamped.event)
            if result.status is ApplyStatus.BUFFERED:
                self._buffered_since_resync += 1
            elif result.status is ApplyStatus.GAP_DETECTED:
                self._log(
                    logging.WARNING, "sequence gap detected", "GAP_DETECTED", detail=result.detail
                )
                self._stats["gap_detected"] += 1
                self._resync_needed.set()
            elif result.status is ApplyStatus.APPLIED:
                # Latency/lag are only observed here, for steady-state diff
                # applies -- not at resync completion, which is a network-
                # bound, entirely different latency shape that would pollute
                # this histogram's buckets.
                if self._processing_latency is not None:
                    elapsed_ns = time.monotonic_ns() - timestamped.ts_local_ns
                    self._processing_latency.labels(
                        exchange="binance", symbol=self._symbol
                    ).observe(elapsed_ns / 1e9)
                if self._feed_lag is not None and timestamped.ts_exchange_ms is not None:
                    self._feed_lag.labels(exchange="binance", symbol=self._symbol).set(
                        time.time() - timestamped.ts_exchange_ms / 1000.0
                    )
                if self._row_queue is not None:
                    row = build_snapshot_row(
                        self._engine,
                        timestamped.instrument,
                        timestamped.ts_local_ns,
                        timestamped.ts_wall_ns,
                        timestamped.ts_exchange_ms,
                    )
                    self._row_queue.put(row)

    async def _resync_worker(self) -> None:
        while True:
            await self._resync_needed.wait()
            self._resync_needed.clear()
            if self._engine.state is BookState.LIVE:
                continue
            await self._perform_resync()

    async def _heartbeat_worker(self) -> None:
        """Low-frequency liveness signal for long unattended runs, separate
        from event-driven incident logging. Steady-state message
        processing is deliberately silent (counted, not logged) -- without
        this, there's no way to tell "silently healthy" from "silently
        stalled" during a long soak run without interrupting the process.
        """
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            stats = self.get_stats()
            logger.info(
                "heartbeat",
                extra={
                    "extra_fields": {
                        "heartbeat": True,
                        "symbol": self._symbol,
                        "messages_received": stats["counters"].get("messages_received", 0),
                        "connection_state": stats["connection_state"],
                        "book_state": stats["book_state"],
                    }
                },
            )

    async def _perform_resync(self) -> None:
        # events_buffered undercounts if apply_event() buffered messages
        # before this task got scheduled to run (the reader loop and this
        # worker are separate coroutines -- there's a real gap between
        # "resync needed" being signaled and this method actually starting).
        # It's a debug-observability figure only; nothing in the apply
        # logic depends on its precision.
        start = time.monotonic()
        self._buffered_since_resync = 0

        for attempt in range(self._snapshot_retry_limit):
            try:
                snapshot = await self._fetch_snapshot()
            except HttpRetryLimitExceeded:
                self._log(logging.ERROR, "giving up on snapshot fetch", "SNAPSHOT_FETCH_FAILED")
                self._stats["snapshot_fetch_failed"] += 1
                break

            result = self._engine.load_snapshot(snapshot)
            if result.status is ApplyStatus.APPLIED:
                self._log(
                    logging.INFO,
                    "resync completed",
                    "RESYNC_COMPLETED",
                    duration_seconds=time.monotonic() - start,
                    attempts=attempt + 1,
                    events_buffered=self._buffered_since_resync,
                )
                self._stats["resync_completed"] += 1
                if self._row_queue is not None:
                    row = build_snapshot_row(
                        self._engine,
                        InstrumentId("binance", self._symbol),
                        time.monotonic_ns(),
                        time.time_ns(),
                        None,  # Binance's REST snapshot body carries no exchange timestamp
                    )
                    self._row_queue.put(row)
                return

            if result.status is ApplyStatus.GAP_DETECTED:
                # The snapshot itself was accepted, but a later buffered
                # event failed to chain during replay -- a genuinely
                # different cause than a stale snapshot, even though the
                # recovery action (fetch a fresh snapshot) is the same.
                self._log(
                    logging.WARNING,
                    "gap detected during buffer replay",
                    "GAP_DETECTED",
                    detail=result.detail,
                )
                self._stats["gap_detected"] += 1
            else:
                self._log(
                    logging.INFO,
                    "snapshot stale, retrying",
                    "SNAPSHOT_STALE_RETRY",
                    attempt=attempt + 1,
                )
                self._stats["snapshot_stale_retry"] += 1

        self._log(
            logging.ERROR,
            "exceeded snapshot retry limit without reaching LIVE, forcing reconnect",
            "SNAPSHOT_RETRY_LIMIT_EXCEEDED",
            limit=self._snapshot_retry_limit,
        )
        self._stats["snapshot_retry_limit_exceeded"] += 1
        if self._current_ws is not None:
            with contextlib.suppress(Exception):
                await self._current_ws.close()

    async def _fetch_snapshot(self) -> SnapshotEvent:
        for _attempt in range(self._http_retry_limit):
            wait = self._token_bucket.time_until_available()
            if wait > 0:
                await asyncio.sleep(wait)
            self._token_bucket.try_acquire()

            try:
                response = await self._http_client.get(
                    "/api/v3/depth", params={"symbol": self._symbol, "limit": self._depth_limit}
                )
            except Exception as exc:
                # A network-level failure here (DNS, connection reset,
                # timeout -- the same disconnect class that hits the WS
                # side) is exactly as retryable as an HTTP 429 below.
                # Letting it propagate unhandled would silently kill
                # _resync_worker for the rest of the process's life --
                # the same class of bug OKX's _request_resubscribe
                # explicitly guards against with contextlib.suppress,
                # except here it's the resync path itself, not a
                # best-effort side send, so a bounded retry (not a
                # blanket suppress) is the right shape: give up on this
                # attempt, let the existing http_retry_limit loop and
                # HttpRetryLimitExceeded -> SNAPSHOT_FETCH_FAILED path
                # (which _perform_resync already handles by returning
                # cleanly) absorb it instead of crashing the worker task.
                self._log(
                    logging.WARNING,
                    "network error fetching snapshot, retrying",
                    "SNAPSHOT_FETCH_NETWORK_ERROR",
                    error=f"{type(exc).__name__}: {exc}",
                )
                self._stats["snapshot_fetch_network_error"] += 1
                continue
            used_weight = {
                k: v
                for k, v in response.headers.items()
                if k.upper().startswith("X-MBX-USED-WEIGHT")
            }
            logger.debug(
                "snapshot fetched",
                extra={
                    "extra_fields": {
                        "symbol": self._symbol,
                        "status_code": response.status_code,
                        "used_weight": used_weight,
                    }
                },
            )

            if response.status_code in (429, 418):
                retry_after = float(response.headers.get("Retry-After", "1"))
                self._log(
                    logging.WARNING,
                    "rate limited fetching snapshot",
                    "RATE_LIMITED",
                    status_code=response.status_code,
                    retry_after_seconds=retry_after,
                    used_weight=used_weight,
                )
                self._stats["rate_limited"] += 1
                await asyncio.sleep(retry_after)
                continue

            response.raise_for_status()
            return parse_snapshot(response.json())

        raise HttpRetryLimitExceeded(
            f"exceeded {self._http_retry_limit} HTTP retry attempts fetching snapshot"
        )
