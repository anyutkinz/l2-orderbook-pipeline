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


# Shared by both feed clients (not a BackoffPolicy field: this bounds how
# many consecutive failed *reconnect attempts* a feed's own retry loop is
# allowed to absorb silently, an orthogonal concern to the delay formula
# between attempts). Observed live (M9): a feed's own ConnectionManager
# backoff loop retries forever by design, and nothing outside it ever
# notices a feed that keeps failing to reconnect -- feed_states stayed
# "running" and restart_counts stayed 0 for 93 minutes straight. 10
# consecutive failures is a few minutes of real wall-clock time at this
# module's default BackoffPolicy (cap=30s), a reasonable point to stop
# trusting the feed's own retry loop and let FeedSupervisor force a full
# restart (fresh client instance, fresh sockets, fresh DNS) instead.
DEFAULT_MAX_CONSECUTIVE_RECONNECT_FAILURES = 10


class ReconnectBudgetExhausted(RuntimeError):
    """Raised by a feed client's run() loop once its ConnectionManager has
    failed to reconnect DEFAULT_MAX_CONSECUTIVE_RECONNECT_FAILURES times in
    a row without a single message getting through. Deliberately lets this
    propagate out of run() entirely (rather than swallowing it, the way
    every other disconnect reason is handled) so FeedSupervisor's existing
    `except Exception` restart path -- previously unreachable for ordinary
    reconnect failures, since run() never raised for those -- finally gets
    a chance to force a full restart instead of the feed retrying forever
    silently.
    """


@dataclass(frozen=True, slots=True)
class ConnectedInfo:
    attempt: int
    outage_duration_seconds: float | None


def full_jitter_delay(policy: BackoffPolicy, attempt: int, rng: RandomLike) -> float:
    """AWS "full jitter" backoff: uniform(0, min(cap, base * 2**attempt)).
    Standalone so restart-backoff callers (e.g. FeedSupervisor) can reuse
    the exact same formula without depending on ConnectionManager's own
    attempt-counter state machine.
    """
    return rng.uniform(0, min(policy.cap_seconds, policy.base_seconds * (2**attempt)))


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

        `_disconnected_at` is only set the *first* time this fires after a
        connection -- a real outage is rarely one clean disconnect/connect
        pair, it's a disconnect followed by however many failed reconnect
        attempts (each one calling this method again) before a connect()
        finally succeeds. Overwriting it on every retry would measure only
        the last retry's gap instead of the whole episode (observed live:
        a ~50-minute outage logged as a 21.2s `outage_duration_seconds`
        because the second-to-last retry landed 21.2s before the eventual
        reconnect).
        """
        del reason
        self._state = ConnectionState.BACKOFF
        if self._disconnected_at is None:
            self._disconnected_at = self._clock()
        delay = full_jitter_delay(self._policy, self._attempt, self._rng)
        self._attempt += 1
        return delay
