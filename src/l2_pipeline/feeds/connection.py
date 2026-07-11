from __future__ import annotations

import enum
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol


class RandomLike(Protocol):
    def uniform(self, a: float, b: float) -> float: ...


class ConnectionState(enum.Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    BACKOFF = "backoff"


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    base_seconds: float = 0.5
    cap_seconds: float = 30.0


@dataclass(frozen=True, slots=True)
class ConnectedInfo:
    attempt: int
    outage_duration_seconds: float | None


class ConnectionManager:
    """Pure connection-lifecycle state machine. Never sleeps or performs
    I/O itself -- disconnected() returns a delay for the caller to await,
    so this is testable with a fake clock and RNG (no event loop needed).
    """

    def __init__(self, policy: BackoffPolicy, rng: RandomLike, clock: Callable[[], float]) -> None:
        self._policy = policy
        self._rng = rng
        self._clock = clock
        self._state = ConnectionState.DISCONNECTED
        self._attempt = 0
        self._message_seen_this_connection = False
        self._disconnected_at: float | None = None

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def attempt(self) -> int:
        return self._attempt

    def connecting(self) -> None:
        self._state = ConnectionState.CONNECTING
        self._message_seen_this_connection = False

    def connected(self) -> ConnectedInfo:
        self._state = ConnectionState.CONNECTED
        outage_duration = None
        if self._disconnected_at is not None:
            outage_duration = self._clock() - self._disconnected_at
        info = ConnectedInfo(attempt=self._attempt, outage_duration_seconds=outage_duration)
        self._disconnected_at = None
        return info

    def message_received(self) -> None:
        if self._state is ConnectionState.CONNECTED and not self._message_seen_this_connection:
            self._message_seen_this_connection = True
            self._attempt = 0

    def disconnected(self, reason: str) -> float:
        """Transition to BACKOFF and return the delay (seconds) the caller
        should await before calling connecting() again. `reason` is not
        interpreted here -- it's for the caller's own structured logging
        (WS_DISCONNECTED vs WATCHDOG_TRIPPED etc), since a plain close, an
        error, and a watchdog trip are all the same state transition.
        """
        del reason
        self._state = ConnectionState.BACKOFF
        self._disconnected_at = self._clock()
        delay = self._rng.uniform(
            0, min(self._policy.cap_seconds, self._policy.base_seconds * (2**self._attempt))
        )
        self._attempt += 1
        return delay
