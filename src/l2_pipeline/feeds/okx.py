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
from typing import Any

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.book.types import (
    ApplyResult,
    ApplyStatus,
    BookState,
    DiffEvent,
    PriceLevel,
    SnapshotEvent,
)
from l2_pipeline.feeds.connection import BackoffPolicy, ConnectionManager
from l2_pipeline.feeds.envelope import InstrumentId, TimestampedEvent, build_snapshot_row
from l2_pipeline.feeds.ratelimit import TokenBucket
from l2_pipeline.feeds.transport import WebSocketConnector, WebSocketLike
from l2_pipeline.sinks.parquet_sink import BoundedRowQueue

logger = logging.getLogger(__name__)

WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
SERVICE_NOTICE_CODE = "64008"
CHANNEL = "books"

DEFAULT_PING_INTERVAL_SECONDS = 15.0
DEFAULT_PONG_TIMEOUT_SECONDS = 15.0
DEFAULT_SNAPSHOT_RETRY_LIMIT = 20
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30.0

# Verified against live OKX docs (developers.okx.com, checked 2026-07-12,
# changelog confirms checksum deprecation landed 2026-06-23): subscribe/
# unsubscribe/login ops are capped at 480/hour per connection; connection
# *attempts* are capped at 3/sec per IP. Both budgets are reused via the
# same TokenBucket class from M3 -- no modification to ratelimit.py.
#
# capacity=40, refill=480/3600: this connection's only consumer of the
# hourly op budget beyond the one-time initial subscribe is resubscribe
# (unsubscribe+subscribe = 2 ops per resync attempt), so sizing directly
# against the documented limit needs no further fractioning.
DEFAULT_RESUBSCRIBE_BUCKET_CAPACITY = 40.0
DEFAULT_RESUBSCRIBE_BUCKET_REFILL_PER_SEC = 480.0 / 3600.0
RESUBSCRIBE_OP_COST = 2.0  # unsubscribe + subscribe

DEFAULT_CONNECTION_BUCKET_CAPACITY = 3.0
DEFAULT_CONNECTION_BUCKET_REFILL_PER_SEC = 3.0


class ParseError(Exception):
    """Raised when a raw OKX message can't be parsed into our types. Same
    role as binance.ParseError -- caught by the reader loop, logged as
    MALFORMED_MESSAGE, message skipped.
    """


def unwrap_book_push(raw: dict[str, Any]) -> dict[str, Any]:
    """Extract the single data element from a books-channel push
    ({"arg": ..., "action": ..., "data": [{...}]}). OKX always sends
    exactly one element in `data` for this channel.
    """
    try:
        data = raw["data"][0]
    except (KeyError, IndexError, TypeError) as exc:
        raise ParseError(str(exc)) from exc
    if not isinstance(data, dict):
        raise ParseError(f"data[0] must be a mapping, got {type(data).__name__}")
    return data


def parse_price_level(raw: Any) -> PriceLevel:
    """OKX level shape is [price, qty, deprecated, numOrders] -- 4
    elements, not Binance's 2. Verified live 2026-07-12 against real
    production traffic, not assumed from docs (see DECISIONS.md M4).
    Only the first two elements are ours; the rest is ignored.
    """
    try:
        price, qty = raw[0], raw[1]
        return PriceLevel(Decimal(price), Decimal(qty))
    except (IndexError, TypeError, decimal.InvalidOperation) as exc:
        raise ParseError(str(exc)) from exc


def parse_book_update(data: dict[str, Any]) -> DiffEvent:
    """books-channel action="update" push -> DiffEvent. seqId/prevSeqId
    map directly to final_id/prev_id, per the M1 generalized contract --
    no arithmetic needed, unlike Binance's U-1.
    """
    try:
        seq_id = int(data["seqId"])
        prev_seq_id = int(data["prevSeqId"])
        bids = [parse_price_level(level) for level in data["bids"]]
        asks = [parse_price_level(level) for level in data["asks"]]
    except (KeyError, ValueError, TypeError, decimal.InvalidOperation) as exc:
        raise ParseError(str(exc)) from exc
    return DiffEvent(prev_id=prev_seq_id, final_id=seq_id, bids=bids, asks=asks)


def _extract_ts_exchange_ms(data: dict[str, Any]) -> int | None:
    """OKX's own push-time field ("ts", a string of epoch milliseconds on
    both update and snapshot pushes), read at the call site rather than
    folded into parse_book_update/parse_book_snapshot -- keeps those
    functions' signatures (and their existing tests) untouched.
    """
    raw = data.get("ts")
    return int(raw) if raw is not None else None


def parse_book_snapshot(data: dict[str, Any]) -> SnapshotEvent:
    """books-channel action="snapshot" push -> SnapshotEvent. Sent as the
    first channel message after subscribing -- no separate REST call,
    unlike Binance. `prevSeqId` on this message is a documented sentinel
    (-1, verified live) and is never consumed: SnapshotEvent has no
    prev_id field, so the sentinel simply never needs interpreting.
    """
    try:
        seq_id = int(data["seqId"])
        bids = [parse_price_level(level) for level in data["bids"]]
        asks = [parse_price_level(level) for level in data["asks"]]
    except (KeyError, ValueError, TypeError, decimal.InvalidOperation) as exc:
        raise ParseError(str(exc)) from exc
    return SnapshotEvent(last_update_id=seq_id, bids=bids, asks=asks)


class _ServiceNoticeReconnect(Exception):
    """Signals run() to take the standard disconnect/backoff/reconnect
    path in response to a proactive OKX_SERVICE_NOTICE, without logging a
    redundant WS_DISCONNECTED for the same event -- mirrors how a
    TimeoutError signals a watchdog trip to run()'s except chain.
    """


def _subscribe_message(op: str, symbol: str) -> str:
    return json.dumps({"op": op, "args": [{"channel": CHANNEL, "instId": symbol}]})


class OKXFeedClient:
    """Production twin of BinanceFeedClient, same control-loop shape
    (run() owns ConnectionManager + a persistent resync worker + an
    inline reader loop), reusing ConnectionManager/TokenBucket/
    TimestampedEvent/WebSocketLike unmodified. Two real divergences from
    Binance, both protocol-driven, not stylistic:

    - Keepalive is a private _receive_message() two-stage ping/pong dance
      instead of Binance's one-line watchdog wait_for -- see DECISIONS.md
      M4 (parallel per-exchange methods, no shared Protocol; revisit only
      if a third exchange needs a third keepalive shape).
    - Resync is event-driven, not loop-driven: OKX's snapshot arrives
      asynchronously as a normal channel push (seen by the reader loop),
      not as a response to whatever sent the resubscribe request. See
      DECISIONS.md M4 for the full request/response-vs-push reasoning.
    """

    def __init__(
        self,
        symbol: str,
        engine: BookEngine,
        *,
        ping_interval_seconds: float = DEFAULT_PING_INTERVAL_SECONDS,
        pong_timeout_seconds: float = DEFAULT_PONG_TIMEOUT_SECONDS,
        snapshot_retry_limit: int = DEFAULT_SNAPSHOT_RETRY_LIMIT,
        heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        backoff_policy: BackoffPolicy | None = None,
        ws_connector: WebSocketConnector | None = None,
        rng: random.Random | None = None,
        clock: Callable[[], float] | None = None,
        resubscribe_bucket: TokenBucket | None = None,
        connection_bucket: TokenBucket | None = None,
        row_queue: BoundedRowQueue | None = None,
    ) -> None:
        self._symbol = symbol
        self._engine = engine
        self._ping_interval = ping_interval_seconds
        self._pong_timeout = pong_timeout_seconds
        self._snapshot_retry_limit = snapshot_retry_limit
        self._heartbeat_interval = heartbeat_interval_seconds
        self._row_queue = row_queue

        clock = clock or time.monotonic
        rng = rng or random.Random()
        self._connection = ConnectionManager(backoff_policy or BackoffPolicy(), rng, clock)
        self._resubscribe_bucket = resubscribe_bucket or TokenBucket(
            DEFAULT_RESUBSCRIBE_BUCKET_CAPACITY, DEFAULT_RESUBSCRIBE_BUCKET_REFILL_PER_SEC, clock
        )
        self._connection_bucket = connection_bucket or TokenBucket(
            DEFAULT_CONNECTION_BUCKET_CAPACITY, DEFAULT_CONNECTION_BUCKET_REFILL_PER_SEC, clock
        )

        if ws_connector is None:
            import websockets

            ws_connector = websockets.connect
        self._ws_connector: WebSocketConnector = ws_connector

        self._resync_needed = asyncio.Event()
        self._current_ws: WebSocketLike | None = None
        # Explicit retry-counter semantics (not implicit in a for-loop,
        # since resync here is event-driven): incremented on each failed
        # resync attempt (SNAPSHOT_STALE or GAP_DETECTED-during-replay),
        # reset to 0 on APPLIED and on every new connection -- so it never
        # leaks across episodes, whether within one connection (a later,
        # unrelated gap) or across reconnects (a resync storm at hour 3
        # doesn't inherit counts from hour 1).
        self._snapshot_retry_count = 0
        self._stats: dict[str, int] = defaultdict(int)

    def get_stats(self) -> dict[str, Any]:
        return {
            "counters": dict(self._stats),
            "connection_state": self._connection.state.value,
            "connection_attempt": self._connection.attempt,
            "book_state": self._engine.state.value,
            "book_last_applied_id": self._engine.last_applied_id,
        }

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
                wait = self._connection_bucket.time_until_available()
                if wait > 0:
                    await asyncio.sleep(wait)
                self._connection_bucket.try_acquire()

                self._connection.connecting()
                try:
                    async with self._ws_connector(WS_URL) as ws:
                        self._current_ws = ws
                        await ws.send(_subscribe_message("subscribe", self._symbol))
                        info = self._connection.connected()
                        self._log(
                            logging.INFO,
                            "websocket connected",
                            "WS_RECONNECTED",
                            attempt=info.attempt,
                            outage_duration_seconds=info.outage_duration_seconds,
                        )
                        self._stats["ws_reconnected"] += 1
                        # Coupling rule, same as Binance: every new
                        # connection forces a fresh sync. For OKX this
                        # resync happens for free -- the snapshot arrives
                        # automatically as the first channel push after
                        # subscribing, no explicit resync_needed trigger
                        # required for cold start/reconnect, only for a
                        # later gap-triggered resubscribe.
                        self._engine.invalidate("reconnect")
                        self._snapshot_retry_count = 0
                        await self._reader_loop(ws)
                except asyncio.CancelledError:
                    raise
                except _ServiceNoticeReconnect:
                    delay = self._connection.disconnected("service_notice")
                    await asyncio.sleep(delay)
                except TimeoutError:
                    self._log(
                        logging.WARNING,
                        "no message within keepalive timeout, treating connection as dead",
                        "WATCHDOG_TRIPPED",
                        ping_interval_seconds=self._ping_interval,
                        pong_timeout_seconds=self._pong_timeout,
                    )
                    self._stats["watchdog_tripped"] += 1
                    delay = self._connection.disconnected("watchdog_tripped")
                    await asyncio.sleep(delay)
                except Exception as exc:
                    reason = f"{type(exc).__name__}: {exc}"
                    self._log(
                        logging.WARNING, "websocket disconnected", "WS_DISCONNECTED", reason=reason
                    )
                    self._stats["ws_disconnected"] += 1
                    delay = self._connection.disconnected(reason)
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
            logger.info("feed client stopped", extra={"extra_fields": {"stats": self.get_stats()}})

    async def _recv_text(self, ws: WebSocketLike, timeout: float) -> str:
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        return raw if isinstance(raw, str) else raw.decode()

    async def _receive_message(self, ws: WebSocketLike) -> str:
        """Two-stage OKX keepalive: silence for ping_interval -> send text
        "ping" -> silence for another pong_timeout -> dead (propagates as
        TimeoutError, handled identically to Binance's watchdog trip by
        run()'s except chain). A received "pong" (or any message) resets
        the timer naturally, since it's returned here and the reader loop
        calls connection.message_received() on every receive.
        """
        return await self._recv_text(ws, self._ping_interval)

    async def _reader_loop(self, ws: WebSocketLike) -> None:
        while True:
            try:
                raw = await self._receive_message(ws)
            except TimeoutError:
                await ws.send("ping")
                raw = await self._recv_text(ws, self._pong_timeout)

            ts_local_ns = time.monotonic_ns()
            self._connection.message_received()
            self._stats["messages_received"] += 1

            if raw == "pong":
                # liveness only -- never reaches the JSON parser, so a
                # pong can never become a false MALFORMED_MESSAGE
                continue

            try:
                envelope: dict[str, Any] = json.loads(raw)
            except json.JSONDecodeError as exc:
                self._log(
                    logging.WARNING,
                    "skipping malformed message",
                    "MALFORMED_MESSAGE",
                    error=str(exc),
                    raw_truncated=str(raw)[:200],
                )
                self._stats["malformed_message"] += 1
                continue

            if "event" in envelope:
                await self._handle_control_message(envelope)
                continue

            await self._handle_channel_push(envelope, ts_local_ns)

    async def _handle_control_message(self, envelope: dict[str, Any]) -> None:
        event = envelope.get("event")

        if event in ("subscribe", "unsubscribe"):
            logger.debug(
                f"{event} ack",
                extra={"extra_fields": {"symbol": self._symbol, "envelope": envelope}},
            )
            return

        if event == "notice" and envelope.get("code") == SERVICE_NOTICE_CODE:
            self._log(
                logging.WARNING,
                "service notice received, reconnecting proactively",
                "OKX_SERVICE_NOTICE",
                code=envelope.get("code"),
            )
            self._stats["okx_service_notice"] += 1
            raise _ServiceNoticeReconnect

        if event == "error":
            self._log(
                logging.WARNING,
                "subscribe/unsubscribe error",
                "OKX_SUBSCRIBE_ERROR",
                envelope=str(envelope),
            )
            self._stats["okx_subscribe_error"] += 1
            return

        self._log(
            logging.WARNING,
            "unrecognized control event",
            "MALFORMED_MESSAGE",
            envelope=str(envelope),
        )
        self._stats["malformed_message"] += 1

    async def _handle_channel_push(self, envelope: dict[str, Any], ts_local_ns: int) -> None:
        try:
            data = unwrap_book_push(envelope)
        except ParseError as exc:
            self._log(
                logging.WARNING,
                "skipping malformed message",
                "MALFORMED_MESSAGE",
                error=str(exc),
                raw_truncated=str(envelope)[:200],
            )
            self._stats["malformed_message"] += 1
            return

        action = envelope.get("action")
        if action == "snapshot":
            await self._handle_snapshot_push(data)
        elif action == "update":
            self._handle_update_push(data, ts_local_ns)
        else:
            self._log(
                logging.WARNING, "unrecognized action", "MALFORMED_MESSAGE", action=str(action)
            )
            self._stats["malformed_message"] += 1

    def _handle_update_push(self, data: dict[str, Any], ts_local_ns: int) -> None:
        try:
            diff = parse_book_update(data)
        except ParseError as exc:
            self._log(
                logging.WARNING,
                "skipping malformed message",
                "MALFORMED_MESSAGE",
                error=str(exc),
                raw_truncated=str(data)[:200],
            )
            self._stats["malformed_message"] += 1
            return

        timestamped = TimestampedEvent(
            ts_local_ns=ts_local_ns,
            instrument=InstrumentId("okx", self._symbol),
            event=diff,
            ts_exchange_ms=_extract_ts_exchange_ms(data),
        )
        result = self._engine.apply_event(timestamped.event)
        if result.status is ApplyStatus.GAP_DETECTED:
            self._log(
                logging.WARNING, "sequence gap detected", "GAP_DETECTED", detail=result.detail
            )
            self._stats["gap_detected"] += 1
            self._resync_needed.set()
        elif result.status is ApplyStatus.APPLIED and self._row_queue is not None:
            row = build_snapshot_row(
                self._engine,
                timestamped.instrument,
                timestamped.ts_local_ns,
                timestamped.ts_exchange_ms,
            )
            self._row_queue.put(row)

    async def _handle_snapshot_push(self, data: dict[str, Any]) -> None:
        try:
            snapshot = parse_book_snapshot(data)
        except ParseError as exc:
            self._log(
                logging.WARNING,
                "skipping malformed message",
                "MALFORMED_MESSAGE",
                error=str(exc),
                raw_truncated=str(data)[:200],
            )
            self._stats["malformed_message"] += 1
            return

        ts_exchange_ms = _extract_ts_exchange_ms(data)
        result = self._engine.load_snapshot(snapshot)
        await self._handle_resync_result(result, ts_exchange_ms)

    async def _handle_resync_result(self, result: ApplyResult, ts_exchange_ms: int | None) -> None:
        if result.status is ApplyStatus.APPLIED:
            self._snapshot_retry_count = 0
            self._log(logging.INFO, "resync completed", "RESYNC_COMPLETED")
            self._stats["resync_completed"] += 1
            if self._row_queue is not None:
                row = build_snapshot_row(
                    self._engine,
                    InstrumentId("okx", self._symbol),
                    time.monotonic_ns(),
                    ts_exchange_ms,
                )
                self._row_queue.put(row)
            return

        self._snapshot_retry_count += 1
        if result.status is ApplyStatus.GAP_DETECTED:
            # Snapshot itself was accepted, but a later buffered event
            # failed to chain during replay -- genuinely different cause
            # than staleness, same recovery action either way (see
            # binance.py's identical distinction, DECISIONS.md M3).
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
                "snapshot stale, will resubscribe",
                "SNAPSHOT_STALE_RETRY",
                attempt=self._snapshot_retry_count,
            )
            self._stats["snapshot_stale_retry"] += 1

        if self._snapshot_retry_count >= self._snapshot_retry_limit:
            self._log(
                logging.ERROR,
                "exceeded snapshot retry limit without reaching LIVE, forcing reconnect",
                "SNAPSHOT_RETRY_LIMIT_EXCEEDED",
                limit=self._snapshot_retry_limit,
            )
            self._stats["snapshot_retry_limit_exceeded"] += 1
            ws = self._current_ws
            if ws is not None:
                with contextlib.suppress(Exception):
                    await ws.close()
            return

        self._resync_needed.set()

    async def _resync_worker(self) -> None:
        while True:
            await self._resync_needed.wait()
            self._resync_needed.clear()
            if self._engine.state is BookState.LIVE:
                continue
            await self._request_resubscribe()

    async def _request_resubscribe(self) -> None:
        wait = self._resubscribe_bucket.time_until_available(cost=RESUBSCRIBE_OP_COST)
        if wait > 0:
            self._log(
                logging.INFO, "resubscribe throttled", "RESUBSCRIBE_THROTTLED", wait_seconds=wait
            )
            self._stats["resubscribe_throttled"] += 1
            await asyncio.sleep(wait)
        self._resubscribe_bucket.try_acquire(cost=RESUBSCRIBE_OP_COST)

        # Captured once, not re-read via self._current_ws between the two
        # awaits: a disconnect landing between them (clearing
        # self._current_ws to None in run()'s finally block) would
        # otherwise crash this task on the second send with an
        # AttributeError on None -- silently killing _resync_worker for
        # the rest of the process's life, since nothing awaits its
        # result. A mid-flight failure here is harmless to suppress:
        # run()'s own reconnect handling is already covering the
        # disconnect, and the next connection gets a fresh snapshot for
        # free via the normal subscribe flow regardless.
        ws = self._current_ws
        if ws is None:
            return
        with contextlib.suppress(Exception):
            await ws.send(_subscribe_message("unsubscribe", self._symbol))
            await ws.send(_subscribe_message("subscribe", self._symbol))

    async def _heartbeat_worker(self) -> None:
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
