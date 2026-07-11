from __future__ import annotations

from collections.abc import Callable

# Verified against developers.binance.com (Market Data Endpoints, GET
# /api/v3/depth) at implementation time, not from memory: weight is 5 for
# limit<=100, 25 for limit<=500, 50 for limit<=1000, 250 for limit<=5000.
# We fetch at limit=100 (weight 5) -- 5x book.depth_levels=20, comfortable
# margin for the internal ladder at the cheapest tier. Overall budget is
# 6000 REQUEST_WEIGHT/minute per IP (confirmed), reported via
# X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter) response headers.
#
# capacity=10: burst room for a stale-snapshot retry storm (each retry
# costs one token) without artificially throttling recovery.
# refill_rate=0.5/sec: sustained ceiling of 30 snapshot fetches/min = 150
# weight/min, ~2.5% of the 6000/min budget -- large headroom for the
# always-on WS stream (free) and any future REST calls sharing the IP.
DEFAULT_SNAPSHOT_BUCKET_CAPACITY = 10.0
DEFAULT_SNAPSHOT_BUCKET_REFILL_PER_SEC = 0.5


class TokenBucket:
    """Pure token bucket: no asyncio.sleep inside, so it's testable with a
    fake clock and no event loop. The caller is responsible for awaiting
    time_until_available() -- this class only ever delays by telling the
    caller how long, never by blocking itself.
    """

    def __init__(
        self, capacity: float, refill_rate_per_sec: float, clock: Callable[[], float]
    ) -> None:
        self._capacity = capacity
        self._refill_rate = refill_rate_per_sec
        self._clock = clock
        self._tokens = capacity
        self._last_refill = clock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_rate)
        self._last_refill = now

    def try_acquire(self, cost: float = 1.0) -> bool:
        self._refill()
        if self._tokens >= cost:
            self._tokens -= cost
            return True
        return False

    def time_until_available(self, cost: float = 1.0) -> float:
        self._refill()
        if self._tokens >= cost:
            return 0.0
        deficit = cost - self._tokens
        return deficit / self._refill_rate
