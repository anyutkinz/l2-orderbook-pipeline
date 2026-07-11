from __future__ import annotations

import inspect
from decimal import Decimal

import pytest

from l2_pipeline.book import (
    ApplyStatus,
    BookEngine,
    BookState,
    DiffEvent,
    PriceLevel,
    SnapshotEvent,
    apply_levels,
)
from l2_pipeline.book import engine as engine_module


def _snapshot(
    last_update_id: int, bids: list[tuple[str, str]], asks: list[tuple[str, str]]
) -> SnapshotEvent:
    return SnapshotEvent(
        last_update_id=last_update_id,
        bids=[PriceLevel(Decimal(p), Decimal(q)) for p, q in bids],
        asks=[PriceLevel(Decimal(p), Decimal(q)) for p, q in asks],
    )


def _event(
    prev_id: int,
    final_id: int,
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
) -> DiffEvent:
    return DiffEvent(
        prev_id=prev_id,
        final_id=final_id,
        bids=[PriceLevel(Decimal(p), Decimal(q)) for p, q in (bids or [])],
        asks=[PriceLevel(Decimal(p), Decimal(q)) for p, q in (asks or [])],
    )


def _live_engine(
    last_update_id: int,
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
) -> BookEngine:
    engine = BookEngine(depth_levels=20)
    result = engine.load_snapshot(_snapshot(last_update_id, bids or [], asks or []))
    assert result.status is ApplyStatus.APPLIED
    assert engine.state is BookState.LIVE
    return engine


# 1. Normal sequential application
def test_normal_sequential_application() -> None:
    engine = _live_engine(1000, bids=[("100", "1")], asks=[("101", "1")])

    r1 = engine.apply_event(_event(1000, 1001, bids=[("100", "2")]))
    r2 = engine.apply_event(_event(1001, 1002, asks=[("101", "3")]))
    r3 = engine.apply_event(_event(1002, 1003, bids=[("99", "5")]))

    assert [r1.status, r2.status, r3.status] == [ApplyStatus.APPLIED] * 3
    assert engine.last_applied_id == 1003

    bids, asks = engine.top_levels()
    assert bids == [
        PriceLevel(Decimal("100"), Decimal("2")),
        PriceLevel(Decimal("99"), Decimal("5")),
    ]
    assert asks == [PriceLevel(Decimal("101"), Decimal("3"))]


# 2. Level deletion (qty == 0 removes, doesn't leave a zero-size level)
def test_level_deletion_removes_level() -> None:
    engine = _live_engine(1000, bids=[("100", "1"), ("99", "2")])

    result = engine.apply_event(_event(1000, 1001, bids=[("100", "0")]))

    assert result.status is ApplyStatus.APPLIED
    bids, _ = engine.top_levels()
    assert bids == [PriceLevel(Decimal("99"), Decimal("2"))]


# 3. Insert of a brand-new price level not previously in the book
def test_insert_new_price_level() -> None:
    engine = _live_engine(1000, bids=[("100", "1")])

    result = engine.apply_event(_event(1000, 1001, bids=[("98", "4")]))

    assert result.status is ApplyStatus.APPLIED
    bids, _ = engine.top_levels()
    assert bids == [
        PriceLevel(Decimal("100"), Decimal("1")),
        PriceLevel(Decimal("98"), Decimal("4")),
    ]


# 4. Gap detection in Phase B (live steady state)
def test_gap_detection_in_live_state() -> None:
    engine = _live_engine(1010)

    result = engine.apply_event(_event(1015, 1020))

    assert result.status is ApplyStatus.GAP_DETECTED
    assert result.detail is not None and "expected prev_id=1010" in result.detail
    assert engine.state is BookState.RESYNCING
    assert engine.last_applied_id is None
    assert engine.top_levels() == ([], [])


# 5. Snapshot boundary -- event fully before the snapshot: discarded, not applied
def test_event_fully_before_snapshot_is_discarded() -> None:
    engine = BookEngine(depth_levels=20)
    engine.apply_event(_event(1190, 1198, bids=[("50", "9")]))

    result = engine.load_snapshot(_snapshot(1200, bids=[("100", "1")], asks=[]))

    assert result.status is ApplyStatus.APPLIED
    assert engine.last_applied_id == 1200
    bids, _ = engine.top_levels()
    assert bids == [
        PriceLevel(Decimal("100"), Decimal("1"))
    ]  # the discarded event's "50" never applied


# 6. Snapshot boundary -- event straddling the snapshot: kept and applied (classic off-by-one)
def test_event_straddling_snapshot_is_kept_and_applied() -> None:
    engine = BookEngine(depth_levels=20)
    engine.apply_event(_event(1195, 1205, bids=[("100", "7")]))

    result = engine.load_snapshot(_snapshot(1200, bids=[("100", "1")], asks=[]))

    assert result.status is ApplyStatus.APPLIED
    assert engine.state is BookState.LIVE
    assert engine.last_applied_id == 1205
    bids, _ = engine.top_levels()
    assert bids == [
        PriceLevel(Decimal("100"), Decimal("7"))
    ]  # straddling event's value wins over snapshot's


# 7. Snapshot stale relative to buffer: first kept event's prev_id > lastUpdateId + 1 range
def test_stale_snapshot_is_rejected_and_recovers_on_retry() -> None:
    engine = BookEngine(depth_levels=20)
    engine.apply_event(_event(1205, 1210, bids=[("100", "3")]))

    stale_result = engine.load_snapshot(_snapshot(1200, bids=[], asks=[]))
    assert stale_result.status is ApplyStatus.SNAPSHOT_STALE
    state_after_stale = engine.state
    assert state_after_stale is not BookState.LIVE
    assert engine.last_applied_id is None

    retry_result = engine.load_snapshot(_snapshot(1206, bids=[("100", "1")], asks=[]))
    assert retry_result.status is ApplyStatus.APPLIED
    state_after_retry = engine.state
    assert state_after_retry is BookState.LIVE
    assert engine.last_applied_id == 1210
    bids, _ = engine.top_levels()
    assert bids == [PriceLevel(Decimal("100"), Decimal("3"))]


# 8. Idempotent re-application: apply_levels applied twice == applied once
def test_apply_levels_is_idempotent() -> None:
    levels = [PriceLevel(Decimal("100"), Decimal("2")), PriceLevel(Decimal("101"), Decimal("0"))]

    applied_once: dict[Decimal, Decimal] = {
        Decimal("100"): Decimal("1"),
        Decimal("101"): Decimal("5"),
    }
    apply_levels(applied_once, levels)

    applied_twice: dict[Decimal, Decimal] = {
        Decimal("100"): Decimal("1"),
        Decimal("101"): Decimal("5"),
    }
    apply_levels(applied_twice, levels)
    apply_levels(applied_twice, levels)

    assert applied_once == applied_twice
    assert applied_once == {Decimal("100"): Decimal("2")}  # 101 deleted, 100 overwritten


# 9. Out-of-order arrival: event with a lower final_id than the current checkpoint
def test_out_of_order_stale_event_is_treated_as_gap() -> None:
    engine = _live_engine(1010)

    result = engine.apply_event(_event(1003, 1008))

    assert result.status is ApplyStatus.GAP_DETECTED
    assert engine.state is BookState.RESYNCING


# Bonus: contract violation, not a protocol condition -- raises, doesn't return a result
def test_load_snapshot_while_live_raises() -> None:
    engine = _live_engine(1000)

    with pytest.raises(ValueError, match="LIVE"):
        engine.load_snapshot(_snapshot(2000, [], []))


# M3 addition: invalidate() forces a fresh sync from any state
def test_invalidate_from_live_forces_resyncing_and_discards_book() -> None:
    engine = _live_engine(1000, bids=[("100", "1")], asks=[("101", "1")])
    engine.apply_event(_event(1000, 1001, bids=[("100", "2")]))

    engine.invalidate("reconnect")

    assert engine.state is BookState.RESYNCING
    assert engine.last_applied_id is None
    assert engine.full_book() == ({}, {})


def test_invalidate_from_buffering_stays_buffering() -> None:
    engine = BookEngine(depth_levels=20)
    engine.apply_event(_event(1190, 1198, bids=[("50", "9")]))

    engine.invalidate("reconnect before ever reaching LIVE")

    assert engine.state is BookState.BUFFERING
    assert engine.last_applied_id is None


def test_invalidate_discards_buffered_events() -> None:
    engine = BookEngine(depth_levels=20)
    engine.apply_event(_event(1195, 1205, bids=[("100", "7")]))  # would have straddled a snapshot

    engine.invalidate("reconnect")

    # the pre-invalidate buffered event must not be replayed against a
    # snapshot that arrives after invalidate() -- it's discarded, not kept
    result = engine.load_snapshot(_snapshot(1200, bids=[("100", "1")], asks=[]))
    assert result.status is ApplyStatus.APPLIED
    assert engine.last_applied_id == 1200
    bids, _ = engine.top_levels()
    assert bids == [
        PriceLevel(Decimal("100"), Decimal("1"))
    ]  # snapshot's value, not the discarded "7"


def test_invalidate_from_resyncing_stays_resyncing() -> None:
    engine = _live_engine(1010)
    engine.apply_event(_event(1015, 1020))  # gap -> RESYNCING
    assert engine.state is BookState.RESYNCING

    engine.invalidate("reconnect")

    assert engine.state is BookState.RESYNCING
    assert engine.last_applied_id is None


# M3 addition: guards the "no lock needed" concurrency argument documented
# in DECISIONS.md -- BinanceFeedClient's reader loop and resync worker
# both call BookEngine methods without a lock, safe only because no method
# here contains an await (each call runs to completion with no yield point
# for the event loop to interleave on). This test fails loudly if a future
# change (e.g. M4's OKX logic landing in this package) violates that.
def test_book_engine_has_no_async_methods() -> None:
    for name, member in inspect.getmembers(engine_module.BookEngine):
        assert not inspect.iscoroutinefunction(member), (
            f"{name} is async -- this breaks the no-lock concurrency "
            f"safety argument documented in DECISIONS.md M3 entry"
        )
