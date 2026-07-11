from __future__ import annotations

import random
from decimal import Decimal

from l2_pipeline.book.engine import apply_levels
from l2_pipeline.book.types import DiffEvent, PriceLevel, SnapshotEvent

_TICK = Decimal("0.01")
_MIN_LEVELS_PER_SIDE = 5
_INITIAL_LEVELS_PER_SIDE = 10
_INITIAL_MID = Decimal("100.00")


class MarketSimulator:
    """Owns a ground-truth order book and generates realistic diff events.

    Not econometrically realistic -- just enough dynamics (inserts,
    overwrites, deletes, deep and shallow levels, a wandering best
    bid/ask) to exercise the ladder mechanics, while never generating a
    crossing book.
    """

    def __init__(self, seed: int, spread_min: Decimal = Decimal("0.01")) -> None:
        self._rng = random.Random(seed)
        self._spread_min = spread_min
        self._update_id = 0
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._seed_book()

    def _seed_book(self) -> None:
        for k in range(1, _INITIAL_LEVELS_PER_SIDE + 1):
            self._bids[_INITIAL_MID - k * _TICK] = Decimal(self._rng.randint(1, 50))
            self._asks[_INITIAL_MID + k * _TICK] = Decimal(self._rng.randint(1, 50))
        assert self._best_bid() < self._best_ask()

    @property
    def update_id(self) -> int:
        return self._update_id

    def oracle_book(self) -> tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]]:
        return dict(self._bids), dict(self._asks)

    def snapshot(self) -> SnapshotEvent:
        return SnapshotEvent(
            last_update_id=self._update_id,
            bids=[PriceLevel(p, q) for p, q in self._bids.items()],
            asks=[PriceLevel(p, q) for p, q in self._asks.items()],
        )

    def step(self) -> DiffEvent:
        prev_id = self._update_id
        bid_changes: list[PriceLevel] = []
        ask_changes: list[PriceLevel] = []

        # Each side's changes are applied immediately after being generated,
        # not batched and applied together at the end -- an insert on one
        # side is clamped against the *current* opposite best price, so if
        # both sides insert in the same tick, the second one sees the
        # first's result. Batching both against a stale shared best_bid/
        # best_ask snapshot lets them independently reach toward each other
        # and cross (found by the Hypothesis property test, H1).
        if self._rng.random() < 0.55:
            bid_changes = self._changes_for_side(is_bid=True)
            apply_levels(self._bids, bid_changes)
        if self._rng.random() < 0.55:
            ask_changes = self._changes_for_side(is_bid=False)
            apply_levels(self._asks, ask_changes)
        if not bid_changes and not ask_changes:
            # guarantee forward progress: every tick must change something
            if self._rng.random() < 0.5:
                bid_changes = self._changes_for_side(is_bid=True)
                apply_levels(self._bids, bid_changes)
            else:
                ask_changes = self._changes_for_side(is_bid=False)
                apply_levels(self._asks, ask_changes)

        assert self._best_bid() < self._best_ask()
        self._update_id += 1

        return DiffEvent(
            prev_id=prev_id,
            final_id=self._update_id,
            bids=bid_changes,
            asks=ask_changes,
        )

    def _best_bid(self) -> Decimal:
        return max(self._bids) if self._bids else _INITIAL_MID - _TICK

    def _best_ask(self) -> Decimal:
        return min(self._asks) if self._asks else _INITIAL_MID + _TICK

    def _changes_for_side(self, *, is_bid: bool) -> list[PriceLevel]:
        side = self._bids if is_bid else self._asks
        action = self._rng.choices(["insert", "update", "delete"], weights=[0.3, 0.4, 0.3])[0]
        if action == "delete" and len(side) <= _MIN_LEVELS_PER_SIDE:
            action = "update"

        if action == "delete":
            price = self._rng.choice(list(side.keys()))
            return [PriceLevel(price, Decimal(0))]

        if action == "update":
            price = self._rng.choice(list(side.keys()))
            return [PriceLevel(price, Decimal(self._rng.randint(1, 50)))]

        return [self._insert_level(is_bid=is_bid)]

    def _insert_level(self, *, is_bid: bool) -> PriceLevel:
        best_bid, best_ask = self._best_bid(), self._best_ask()
        qty = Decimal(self._rng.randint(1, 50))
        if is_bid:
            offset = Decimal(self._rng.randint(-5, 1)) * _TICK
            price = min(best_bid + offset, best_ask - self._spread_min)
            price = max(price, _TICK)
        else:
            offset = Decimal(self._rng.randint(-1, 5)) * _TICK
            price = max(best_ask + offset, best_bid + self._spread_min)
        return PriceLevel(price, qty)
