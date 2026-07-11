from __future__ import annotations

import enum
from dataclasses import dataclass
from decimal import Decimal
from typing import NamedTuple


class PriceLevel(NamedTuple):
    price: Decimal
    qty: Decimal


@dataclass(frozen=True, slots=True)
class DiffEvent:
    prev_id: int
    final_id: int
    bids: list[PriceLevel]
    asks: list[PriceLevel]


@dataclass(frozen=True, slots=True)
class SnapshotEvent:
    last_update_id: int
    bids: list[PriceLevel]
    asks: list[PriceLevel]


class BookState(enum.Enum):
    BUFFERING = "buffering"
    RESYNCING = "resyncing"
    LIVE = "live"


class ApplyStatus(enum.Enum):
    APPLIED = "applied"
    BUFFERED = "buffered"
    GAP_DETECTED = "gap_detected"
    SNAPSHOT_STALE = "snapshot_stale"


@dataclass(frozen=True, slots=True)
class ApplyResult:
    status: ApplyStatus
    detail: str | None = None
