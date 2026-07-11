from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from l2_pipeline.feeds.binance import (
    ParseError,
    parse_combined_stream_envelope,
    parse_diff_event,
    parse_snapshot,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "binance"


def _load(name: str) -> dict[str, Any]:
    result: dict[str, Any] = json.loads((FIXTURES_DIR / name).read_text())
    return result


def test_parse_combined_stream_envelope() -> None:
    raw = _load("depth_diff_normal.json")

    data = parse_combined_stream_envelope(raw)

    assert data["s"] == "BTCUSDT"
    assert data["U"] == 97274742110


def test_parse_diff_event_normal_maps_u_to_prev_id_minus_one() -> None:
    data = parse_combined_stream_envelope(_load("depth_diff_normal.json"))

    event = parse_diff_event(data)

    # raw U=97274742110, u=97274742146 -- prev_id must be U-1, not U itself
    assert event.prev_id == 97274742109
    assert event.final_id == 97274742146
    assert len(event.bids) == 15
    assert len(event.asks) == 1
    # includes real deletions (qty "0.00000000") and inserts, parsed as-is
    assert Decimal("0.00000000") in {level.qty for level in event.bids}
    assert event.bids[0].price == Decimal("64175.83000000")
    assert event.bids[0].qty == Decimal("0.11117000")


def test_parse_diff_event_bids_only_leaves_asks_empty() -> None:
    data = parse_combined_stream_envelope(_load("depth_diff_bids_only.json"))

    event = parse_diff_event(data)

    assert event.asks == []
    assert len(event.bids) == 2
    assert event.prev_id == 97274742097  # U=97274742098 -> prev_id = U-1


def test_parse_snapshot() -> None:
    data = _load("depth_snapshot.json")

    snapshot = parse_snapshot(data)

    assert snapshot.last_update_id == 97274744811
    assert len(snapshot.bids) == 10
    assert len(snapshot.asks) == 10
    assert snapshot.bids[0].price == Decimal("64175.83000000")
    assert snapshot.asks[0].price == Decimal("64175.84000000")


def test_malformed_missing_field_raises_parse_error() -> None:
    data = parse_combined_stream_envelope(_load("depth_diff_malformed_missing_field.json"))

    with pytest.raises(ParseError):
        parse_diff_event(data)


def test_malformed_bad_level_shape_raises_parse_error() -> None:
    data = parse_combined_stream_envelope(_load("depth_diff_malformed_bad_level_shape.json"))

    with pytest.raises(ParseError):
        parse_diff_event(data)


def test_malformed_non_numeric_price_raises_parse_error() -> None:
    data = parse_combined_stream_envelope(_load("depth_diff_normal.json"))
    corrupted = dict(data)
    corrupted["b"] = [["not-a-number", "1.0"]]

    with pytest.raises(ParseError):
        parse_diff_event(corrupted)


def test_envelope_missing_data_key_raises_parse_error() -> None:
    with pytest.raises(ParseError):
        parse_combined_stream_envelope({"stream": "btcusdt@depth@100ms"})
