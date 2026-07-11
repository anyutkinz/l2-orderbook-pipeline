from __future__ import annotations

from l2_pipeline.feeds.ratelimit import TokenBucket


class _FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def test_burst_capacity_allows_up_to_capacity_immediately() -> None:
    clock = _FakeClock()
    bucket = TokenBucket(capacity=5.0, refill_rate_per_sec=1.0, clock=clock)

    for _ in range(5):
        assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False


def test_exhausted_bucket_reports_wait_time_not_failure_forever() -> None:
    clock = _FakeClock()
    bucket = TokenBucket(capacity=1.0, refill_rate_per_sec=0.5, clock=clock)

    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False

    wait = bucket.time_until_available()
    assert wait == 2.0  # 1 token deficit / 0.5 tokens-per-sec refill

    clock.now += wait
    assert bucket.try_acquire() is True  # delays, doesn't drop -- available once time passes


def test_refill_rate_accrues_over_time() -> None:
    clock = _FakeClock()
    bucket = TokenBucket(capacity=10.0, refill_rate_per_sec=2.0, clock=clock)

    for _ in range(10):
        bucket.try_acquire()
    assert bucket.try_acquire() is False

    clock.now += 3.0  # 3s * 2/s = 6 tokens refilled
    assert bucket.time_until_available() == 0.0
    for _ in range(6):
        assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False


def test_refill_never_exceeds_capacity() -> None:
    clock = _FakeClock()
    bucket = TokenBucket(capacity=3.0, refill_rate_per_sec=1.0, clock=clock)

    clock.now += 1000.0  # long idle period
    for _ in range(3):
        assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False


def test_time_until_available_is_zero_when_tokens_present() -> None:
    clock = _FakeClock()
    bucket = TokenBucket(capacity=5.0, refill_rate_per_sec=1.0, clock=clock)

    assert bucket.time_until_available() == 0.0


def test_cost_greater_than_one() -> None:
    clock = _FakeClock()
    bucket = TokenBucket(capacity=10.0, refill_rate_per_sec=1.0, clock=clock)

    assert bucket.try_acquire(cost=7.0) is True
    assert bucket.try_acquire(cost=4.0) is False
    assert bucket.time_until_available(cost=4.0) == 1.0  # 1 token short, 1/sec refill
