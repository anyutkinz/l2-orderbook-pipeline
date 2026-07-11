from __future__ import annotations

from decimal import Decimal

from l2_pipeline.book.types import (
    ApplyResult,
    ApplyStatus,
    BookState,
    DiffEvent,
    PriceLevel,
    SnapshotEvent,
)


def apply_levels(side: dict[Decimal, Decimal], levels: list[PriceLevel]) -> None:
    """Apply absolute price-level updates to one side of the book, in place.

    Each level is an absolute (price, qty) pair, not a delta: qty == 0
    removes the level, any other qty overwrites it. Applying the same
    levels twice in a row is therefore a no-op the second time -- this
    idempotency is what makes it safe to apply an event whose effect a
    fresh REST snapshot may already partially reflect (see the boundary
    handling in BookEngine.load_snapshot).
    """
    for price, qty in levels:
        if qty == 0:
            side.pop(price, None)
        else:
            side[price] = qty


class BookEngine:
    def __init__(self, depth_levels: int) -> None:
        self._depth_levels = depth_levels
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._state = BookState.BUFFERING
        self._last_applied_id: int | None = None
        self._buffer: list[DiffEvent] = []

    @property
    def state(self) -> BookState:
        return self._state

    @property
    def last_applied_id(self) -> int | None:
        return self._last_applied_id

    def apply_event(self, event: DiffEvent) -> ApplyResult:
        if self._state is not BookState.LIVE:
            self._buffer.append(event)
            return ApplyResult(ApplyStatus.BUFFERED)
        return self._apply_live(event)

    def load_snapshot(self, snapshot: SnapshotEvent) -> ApplyResult:
        if self._state is BookState.LIVE:
            raise ValueError(
                "load_snapshot() called while state is LIVE -- the feed "
                "client should only fetch a new snapshot while BUFFERING "
                "or RESYNCING"
            )

        survivors = [e for e in self._buffer if e.final_id > snapshot.last_update_id]

        if survivors:
            first = survivors[0]
            if not (first.prev_id <= snapshot.last_update_id < first.final_id):
                return ApplyResult(
                    ApplyStatus.SNAPSHOT_STALE,
                    detail=(
                        f"first buffered event prev_id={first.prev_id} "
                        f"final_id={first.final_id} does not straddle "
                        f"snapshot last_update_id={snapshot.last_update_id}"
                    ),
                )

        self._bids = {level.price: level.qty for level in snapshot.bids}
        self._asks = {level.price: level.qty for level in snapshot.asks}
        self._buffer = []
        self._state = BookState.LIVE

        if not survivors:
            self._last_applied_id = snapshot.last_update_id
            return ApplyResult(ApplyStatus.APPLIED)

        # The first survivor is validated above via the straddle condition,
        # not prev_id == last_applied_id -- its prev_id can legitimately
        # fall strictly before the snapshot checkpoint. Every event after it
        # goes through the same strict chaining check as steady-state.
        first, rest = survivors[0], survivors[1:]
        apply_levels(self._bids, first.bids)
        apply_levels(self._asks, first.asks)
        self._last_applied_id = first.final_id

        for event in rest:
            result = self._apply_live(event)
            if result.status is ApplyStatus.GAP_DETECTED:
                return result

        return ApplyResult(ApplyStatus.APPLIED)

    def top_levels(self, n: int | None = None) -> tuple[list[PriceLevel], list[PriceLevel]]:
        limit = n if n is not None else self._depth_levels
        bids = [
            PriceLevel(price, qty)
            for price, qty in sorted(self._bids.items(), reverse=True)[:limit]
        ]
        asks = [PriceLevel(price, qty) for price, qty in sorted(self._asks.items())[:limit]]
        return bids, asks

    def full_book(self) -> tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]]:
        return dict(self._bids), dict(self._asks)

    def _apply_live(self, event: DiffEvent) -> ApplyResult:
        if event.prev_id != self._last_applied_id:
            detail = (
                f"expected prev_id={self._last_applied_id}, got "
                f"prev_id={event.prev_id} (final_id={event.final_id})"
            )
            self._state = BookState.RESYNCING
            self._bids = {}
            self._asks = {}
            self._last_applied_id = None
            self._buffer = [event]
            return ApplyResult(ApplyStatus.GAP_DETECTED, detail=detail)

        apply_levels(self._bids, event.bids)
        apply_levels(self._asks, event.asks)
        self._last_applied_id = event.final_id
        return ApplyResult(ApplyStatus.APPLIED)
