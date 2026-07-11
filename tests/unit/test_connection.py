from __future__ import annotations

import random

from l2_pipeline.feeds.connection import BackoffPolicy, ConnectionManager, ConnectionState


class _FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class _RecordingRandom:
    """Stands in for random.Random: returns the upper bound of every
    uniform() call and records the (a, b) range it was asked for, so
    tests can assert the exact formula's inputs instead of fighting
    real randomness."""

    def __init__(self) -> None:
        self.calls: list[tuple[float, float]] = []

    def uniform(self, a: float, b: float) -> float:
        self.calls.append((a, b))
        return b


def test_full_jitter_formula_ranges() -> None:
    rng = _RecordingRandom()
    manager = ConnectionManager(
        BackoffPolicy(base_seconds=0.5, cap_seconds=30.0), rng, _FakeClock()
    )

    delays = []
    for _ in range(6):
        manager.connecting()
        delays.append(manager.disconnected("test"))

    # delay = uniform(0, min(cap, base * 2**attempt)) for attempt = 0, 1, 2, ...
    expected_uppers = [0.5, 1.0, 2.0, 4.0, 8.0, 16.0]
    assert [b for _a, b in rng.calls] == expected_uppers
    assert all(a == 0.0 for a, _b in rng.calls)
    assert delays == expected_uppers  # our fake returns the upper bound


def test_cap_is_respected() -> None:
    rng = _RecordingRandom()
    manager = ConnectionManager(BackoffPolicy(base_seconds=0.5, cap_seconds=2.0), rng, _FakeClock())

    for _ in range(10):
        manager.connecting()
        manager.disconnected("test")

    assert all(b <= 2.0 for _a, b in rng.calls)
    assert rng.calls[-1] == (0.0, 2.0)


def test_real_rng_delays_are_bounded_correctly() -> None:
    rng = random.Random(12345)
    manager = ConnectionManager(
        BackoffPolicy(base_seconds=0.5, cap_seconds=30.0), rng, _FakeClock()
    )

    for attempt in range(8):
        manager.connecting()
        delay = manager.disconnected("test")
        expected_upper = min(30.0, 0.5 * 2**attempt)
        assert 0.0 <= delay <= expected_upper


def test_attempt_counter_resets_after_message_delivered() -> None:
    rng = _RecordingRandom()
    manager = ConnectionManager(BackoffPolicy(), rng, _FakeClock())

    manager.connecting()
    manager.disconnected("test")  # attempt 0 -> 1
    manager.connecting()
    manager.disconnected("test")  # attempt 1 -> 2
    assert manager.attempt == 2

    manager.connecting()
    manager.connected()
    manager.message_received()
    assert manager.attempt == 0

    manager.disconnected("test")  # should use the reset attempt (0), not 2
    assert rng.calls[-1] == (0.0, 0.5)


def test_attempt_counter_not_reset_without_a_message() -> None:
    rng = _RecordingRandom()
    manager = ConnectionManager(BackoffPolicy(), rng, _FakeClock())

    manager.connecting()
    manager.connected()
    # connection dies with zero messages delivered -- no reset
    manager.disconnected("test")
    manager.connecting()
    manager.disconnected("test")

    assert manager.attempt == 2


def test_message_received_only_resets_once_per_connection() -> None:
    rng = _RecordingRandom()
    manager = ConnectionManager(BackoffPolicy(), rng, _FakeClock())

    manager.connecting()
    manager.disconnected("test")  # attempt -> 1
    manager.connecting()
    manager.connected()
    manager.message_received()
    assert manager.attempt == 0
    manager.message_received()
    manager.message_received()
    assert manager.attempt == 0  # still 0, not decremented or otherwise touched


def test_connected_info_outage_duration() -> None:
    clock = _FakeClock(start=100.0)
    manager = ConnectionManager(BackoffPolicy(), random.Random(1), clock)

    manager.connecting()
    info = manager.connected()
    assert info.outage_duration_seconds is None  # first-ever connect, nothing to measure

    clock.now = 150.0
    manager.disconnected("test")
    clock.now = 155.5
    manager.connecting()
    info = manager.connected()
    assert info.outage_duration_seconds == 5.5
    assert info.attempt == 1  # the attempt number that was in flight when this connect succeeded


def test_state_transitions() -> None:
    manager = ConnectionManager(BackoffPolicy(), random.Random(1), _FakeClock())

    state = manager.state
    assert state is ConnectionState.DISCONNECTED
    manager.connecting()
    state = manager.state
    assert state is ConnectionState.CONNECTING
    manager.connected()
    state = manager.state
    assert state is ConnectionState.CONNECTED
    manager.disconnected("test")
    state = manager.state
    assert state is ConnectionState.BACKOFF
    manager.connecting()
    state = manager.state
    assert state is ConnectionState.CONNECTING
