from __future__ import annotations

import decimal
from decimal import Decimal
from typing import Any

from l2_pipeline.book.types import DiffEvent, PriceLevel, SnapshotEvent

WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
SERVICE_NOTICE_CODE = "64008"


class ParseError(Exception):
    """Raised when a raw OKX message can't be parsed into our types. Same
    role as binance.ParseError -- caught by the reader loop, logged as
    MALFORMED_MESSAGE, message skipped.
    """


def unwrap_book_push(raw: dict[str, Any]) -> dict[str, Any]:
    """Extract the single data element from a books-channel push
    ({"arg": ..., "action": ..., "data": [{...}]}). OKX always sends
    exactly one element in `data` for this channel.
    """
    try:
        data = raw["data"][0]
    except (KeyError, IndexError, TypeError) as exc:
        raise ParseError(str(exc)) from exc
    if not isinstance(data, dict):
        raise ParseError(f"data[0] must be a mapping, got {type(data).__name__}")
    return data


def parse_price_level(raw: Any) -> PriceLevel:
    """OKX level shape is [price, qty, deprecated, numOrders] -- 4
    elements, not Binance's 2. Verified live 2026-07-12 against real
    production traffic, not assumed from docs (see DECISIONS.md M4).
    Only the first two elements are ours; the rest is ignored.
    """
    try:
        price, qty = raw[0], raw[1]
        return PriceLevel(Decimal(price), Decimal(qty))
    except (IndexError, TypeError, decimal.InvalidOperation) as exc:
        raise ParseError(str(exc)) from exc


def parse_book_update(data: dict[str, Any]) -> DiffEvent:
    """books-channel action="update" push -> DiffEvent. seqId/prevSeqId
    map directly to final_id/prev_id, per the M1 generalized contract --
    no arithmetic needed, unlike Binance's U-1.
    """
    try:
        seq_id = int(data["seqId"])
        prev_seq_id = int(data["prevSeqId"])
        bids = [parse_price_level(level) for level in data["bids"]]
        asks = [parse_price_level(level) for level in data["asks"]]
    except (KeyError, ValueError, TypeError, decimal.InvalidOperation) as exc:
        raise ParseError(str(exc)) from exc
    return DiffEvent(prev_id=prev_seq_id, final_id=seq_id, bids=bids, asks=asks)


def parse_book_snapshot(data: dict[str, Any]) -> SnapshotEvent:
    """books-channel action="snapshot" push -> SnapshotEvent. Sent as the
    first channel message after subscribing -- no separate REST call,
    unlike Binance. `prevSeqId` on this message is a documented sentinel
    (-1, verified live) and is never consumed: SnapshotEvent has no
    prev_id field, so the sentinel simply never needs interpreting.
    """
    try:
        seq_id = int(data["seqId"])
        bids = [parse_price_level(level) for level in data["bids"]]
        asks = [parse_price_level(level) for level in data["asks"]]
    except (KeyError, ValueError, TypeError, decimal.InvalidOperation) as exc:
        raise ParseError(str(exc)) from exc
    return SnapshotEvent(last_update_id=seq_id, bids=bids, asks=asks)
