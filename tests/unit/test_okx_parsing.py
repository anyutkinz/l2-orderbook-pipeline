from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from l2_pipeline.feeds.okx import (
    ParseError,
    parse_book_snapshot,
    parse_book_update,
    parse_price_level,
    unwrap_book_push,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "okx"


def _load(name: str) -> dict[str, Any]:
    result: dict[str, Any] = json.loads((FIXTURES_DIR / name).read_text())
    return result


def test_unwrap_book_push_update() -> None:
    raw = _load("books_update_normal.json")

    data = unwrap_book_push(raw)

    assert data["seqId"] == 78677952687
    assert data["prevSeqId"] == 78677952608


def test_parse_book_update_normal_maps_seqid_prevseqid_directly() -> None:
    data = unwrap_book_push(_load("books_update_normal.json"))

    event = parse_book_update(data)

    # OKX maps directly, no arithmetic (unlike Binance's U-1)
    assert event.prev_id == 78677952608
    assert event.final_id == 78677952687
    assert len(event.asks) == 19
    assert len(event.bids) == 4
    # includes real deletions (qty "0") and inserts/updates, parsed as-is
    assert Decimal("0") in {level.qty for level in event.asks}


def test_parse_book_update_asks_only_leaves_bids_empty() -> None:
    data = unwrap_book_push(_load("books_update_asks_only.json"))

    event = parse_book_update(data)

    assert event.bids == []
    assert len(event.asks) == 8


def test_parse_book_snapshot() -> None:
    data = unwrap_book_push(_load("books_snapshot.json"))

    snapshot = parse_book_snapshot(data)

    assert snapshot.last_update_id == 78677952608
    assert len(snapshot.bids) == 400
    assert len(snapshot.asks) == 400


def test_snapshot_prevseqid_sentinel_is_never_consumed() -> None:
    # verified live: prevSeqId == -1 on the snapshot message. SnapshotEvent
    # has no prev_id field, so this sentinel simply never needs
    # interpreting -- this test documents that fact rather than exercising
    # any special-case handling (there is none).
    data = unwrap_book_push(_load("books_snapshot.json"))
    assert data["prevSeqId"] == -1

    snapshot = parse_book_snapshot(data)
    assert not hasattr(snapshot, "prev_id")


# U7: level-shape regression test, pinned against a real captured message.
# OKX levels are [price, qty, deprecated, numOrders] -- 4 elements, not
# Binance's 2-element [price, qty]. Verified live 2026-07-12, not assumed
# from docs -- exactly the kind of divergence that silently breaks a
# parser written from memory, so it gets its own named test rather than
# incidental coverage inside the normal-update test above.
def test_u7_four_element_level_shape_regression() -> None:
    data = unwrap_book_push(_load("books_update_normal.json"))
    raw_level = data["asks"][0]

    assert len(raw_level) == 4  # [price, qty, deprecated ("0"), numOrders]
    assert raw_level[2] == "0"  # deprecated field, always "0"

    level = parse_price_level(raw_level)

    assert level.price == Decimal(raw_level[0])
    assert level.qty == Decimal(raw_level[1])


def test_malformed_missing_seqid_raises_parse_error() -> None:
    data = unwrap_book_push(_load("books_malformed_missing_seqid.json"))

    with pytest.raises(ParseError):
        parse_book_update(data)


def test_malformed_bad_level_shape_raises_parse_error() -> None:
    data = unwrap_book_push(_load("books_malformed_bad_level_shape.json"))

    with pytest.raises(ParseError):
        parse_book_update(data)


def test_malformed_non_numeric_price_raises_parse_error() -> None:
    data = unwrap_book_push(_load("books_update_normal.json"))
    corrupted = dict(data)
    corrupted["asks"] = [["not-a-number", "1.0", "0", "1"]]

    with pytest.raises(ParseError):
        parse_book_update(corrupted)


def test_unwrap_missing_data_raises_parse_error() -> None:
    with pytest.raises(ParseError):
        unwrap_book_push({"arg": {"channel": "books", "instId": "BTC-USDT"}})
