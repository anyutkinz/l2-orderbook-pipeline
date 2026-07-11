from __future__ import annotations

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.book.types import BookState
from l2_pipeline.simulation.market import MarketSimulator


def assert_converged(engine: BookEngine, market: MarketSimulator) -> None:
    assert engine.state is BookState.LIVE, f"expected LIVE, got {engine.state}"
    assert engine.last_applied_id == market.update_id, (
        f"checkpoint mismatch: engine={engine.last_applied_id} market={market.update_id}"
    )
    assert engine.full_book() == market.oracle_book(), "book contents diverged from oracle"
