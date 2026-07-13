from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any


class FakeWebSocket:
    def __init__(self, recv_fn: Callable[[], Awaitable[str]]) -> None:
        self._recv_fn = recv_fn
        self.closed = False

    async def __aenter__(self) -> FakeWebSocket:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def recv(self) -> str:
        return await self._recv_fn()

    async def send(self, message: str) -> None:
        # Binance's client never sends anything post-connect; only exists
        # to satisfy WebSocketLike now that OKX's client needs send().
        pass

    async def close(self) -> None:
        self.closed = True


def stalling_connector(_url: str) -> FakeWebSocket:
    """A connector whose recv() never returns -- for exercising the
    watchdog timeout (T4)."""

    async def _stall() -> str:
        await asyncio.sleep(3600)
        raise AssertionError("should have been cancelled by the watchdog timeout first")

    return FakeWebSocket(_stall)


def scripted_connector(messages: list[str]) -> Callable[[str], FakeWebSocket]:
    """A connector that yields messages one at a time, then hangs forever
    once the script is exhausted (so the reader loop just waits quietly
    instead of erroring, letting a test settle and assert)."""
    queue: list[str] = list(messages)

    async def _recv() -> str:
        if queue:
            return queue.pop(0)
        await asyncio.sleep(3600)
        raise AssertionError("script exhausted, nothing left to deliver")

    def _connector(_url: str) -> FakeWebSocket:
        return FakeWebSocket(_recv)

    return _connector


class ConnectionClosed(Exception):
    """Stands in for websockets.exceptions.ConnectionClosed -- raised by
    FakeOKXWebSocket.recv() once the connection has been closed, so
    close()-then-recv() exercises the same "next recv fails naturally"
    path the real client relies on (see okx.py's retry-limit-exceeded
    handling)."""


class FakeOKXWebSocket:
    """Scriptable fake supporting the send-then-respond pattern OKX's
    resync needs (unsubscribe/subscribe are requests with an
    asynchronously-pushed response, not a direct reply). `on_send` is
    called with each sent message and returns 0+ messages to enqueue in
    response.
    """

    def __init__(self, on_send: Callable[[str], list[str]] | None = None) -> None:
        self.sent: list[str] = []
        self._on_send = on_send
        self._queue: list[str] = []
        self._new_message = asyncio.Event()
        self.closed = False

    async def __aenter__(self) -> FakeOKXWebSocket:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    def enqueue(self, *messages: str) -> None:
        self._queue.extend(messages)
        self._new_message.set()

    async def send(self, message: str) -> None:
        self.sent.append(message)
        if self._on_send is not None:
            responses = self._on_send(message)
            if responses:
                self._queue.extend(responses)
                self._new_message.set()

    async def recv(self) -> str:
        # Waits on an Event rather than a fixed sleep, so a live
        # enqueue()/send()-triggered response arriving *after* recv() is
        # already suspended actually wakes it up -- a fixed sleep(N)
        # can't be woken early, so queuing a message mid-test would
        # silently never be seen until the sleep expired.
        while not self._queue:
            if self.closed:
                raise ConnectionClosed("closed")
            self._new_message.clear()
            await self._new_message.wait()
        if self.closed:
            raise ConnectionClosed("closed")
        return self._queue.pop(0)

    async def close(self) -> None:
        self.closed = True
        self._new_message.set()


def okx_connector(ws: FakeOKXWebSocket) -> Callable[[str], FakeOKXWebSocket]:
    def _connector(_url: str) -> FakeOKXWebSocket:
        return ws

    return _connector


class FakeHttpResponse:
    def __init__(
        self, status_code: int, json_data: Any, headers: dict[str, str] | None = None
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.headers = headers or {}

    def json(self) -> Any:
        return self._json_data

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHttpClient:
    def __init__(
        self,
        responses: list[FakeHttpResponse] | None = None,
        default: FakeHttpResponse | None = None,
        delay_seconds: float = 0.0,
    ) -> None:
        self._responses = list(responses or [])
        self._default = default
        self._delay = delay_seconds
        self.calls: list[dict[str, Any]] = []

    async def get(self, url: str, params: dict[str, Any]) -> FakeHttpResponse:
        if self._delay:
            await asyncio.sleep(self._delay)
        self.calls.append({"url": url, "params": dict(params)})
        if self._responses:
            return self._responses.pop(0)
        if self._default is not None:
            return self._default
        raise AssertionError("no more scripted HTTP responses")

    async def aclose(self) -> None:
        pass
