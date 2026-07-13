from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Protocol


class WebSocketLike(Protocol):
    async def recv(self) -> str | bytes: ...
    async def send(self, message: str) -> None: ...
    async def close(self) -> None: ...


WebSocketConnector = Callable[[str], AbstractAsyncContextManager[WebSocketLike]]
