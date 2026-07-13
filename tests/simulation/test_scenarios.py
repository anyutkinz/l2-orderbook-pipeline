from __future__ import annotations

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.simulation import (
    STORM_FAULT_CONFIG,
    FaultConfig,
    FaultType,
    SimulatedFeedDriver,
    assert_converged,
    build_simulation,
    summarize_log,
)
from l2_pipeline.simulation.market import MarketSimulator


def _caught_up(engine: BookEngine, market: MarketSimulator) -> bool:
    """True once the engine's checkpoint matches the market's -- the real
    signal for "safe to compare full book contents", not just "no fault is
    pending in the injector". A single dropped event, for instance, leaves
    the injector with nothing pending (is_quiescent is True) even though
    the engine hasn't yet noticed the resulting gap -- that only happens
    once the *next* event's prev_id fails to chain.
    """
    return engine.last_applied_id == market.update_id


def _drain_until_caught_up(
    driver: SimulatedFeedDriver,
    engine: BookEngine,
    market: MarketSimulator,
    max_extra_steps: int = 200,
) -> None:
    """Run extra steps until the engine has caught up to the market, so the
    final convergence check isn't racing an in-progress fault. Bounded so a
    real bug still fails loudly instead of hanging."""
    for _ in range(max_extra_steps):
        if _caught_up(engine, market):
            return
        driver.run_step()
    raise AssertionError(f"engine did not catch up within {max_extra_steps} extra steps")


_STORM_CONFIG = STORM_FAULT_CONFIG


def test_s1_clean_run_stays_converged() -> None:
    market, injector, engine, driver = build_simulation(seed=1001, config=FaultConfig())

    for _ in range(500):
        driver.run_step()
        assert_converged(engine, market)
        bids, asks = market.oracle_book()
        assert max(bids) < min(asks)

    assert injector.log == []


def test_s2_single_drop_recovers() -> None:
    market, injector, engine, driver = build_simulation(
        seed=1002, config=FaultConfig(drop_one_prob=0.05)
    )

    driver.run(300)
    _drain_until_caught_up(driver, engine, market)

    assert_converged(engine, market)
    fired = [r for r in injector.log if r.fault_type is FaultType.DROP_ONE and not r.shadowed]
    assert fired, "expected at least one DROP_ONE to fire in this run"
    assert driver.recoveries, "expected at least one recovery to be recorded"


def test_s3_burst_drop_recovers() -> None:
    market, injector, engine, driver = build_simulation(
        seed=1003, config=FaultConfig(drop_burst_prob=0.03, drop_burst_max_len=5)
    )

    driver.run(300)
    _drain_until_caught_up(driver, engine, market)

    assert_converged(engine, market)
    fired = [r for r in injector.log if r.fault_type is FaultType.DROP_BURST and not r.shadowed]
    assert fired, "expected at least one DROP_BURST to fire in this run"
    assert driver.recoveries


def test_s4_duplicate_delivery_is_not_silently_absorbed() -> None:
    market, injector, engine, driver = build_simulation(
        seed=1004, config=FaultConfig(duplicate_prob=0.05)
    )

    driver.run(300)
    _drain_until_caught_up(driver, engine, market)

    assert_converged(engine, market)
    fired = [r for r in injector.log if r.fault_type is FaultType.DUPLICATE and not r.shadowed]
    assert fired, "expected at least one DUPLICATE to fire in this run"
    assert driver.recoveries, (
        "a duplicate must break the prev_id chain and force a real resync -- "
        "if this is empty, duplicates are being silently absorbed"
    )


def test_s5_reorder_recovers() -> None:
    market, injector, engine, driver = build_simulation(
        seed=1005, config=FaultConfig(reorder_prob=0.05)
    )

    driver.run(300)
    _drain_until_caught_up(driver, engine, market)

    assert_converged(engine, market)
    fired = [r for r in injector.log if r.fault_type is FaultType.REORDER and not r.shadowed]
    assert fired, "expected at least one REORDER to fire in this run"
    assert driver.recoveries


def test_s6_disconnect_window_recovers() -> None:
    market, injector, engine, driver = build_simulation(
        seed=1006, config=FaultConfig(disconnect_prob=0.02, disconnect_max_len=15)
    )

    driver.run(300)
    _drain_until_caught_up(driver, engine, market)

    assert_converged(engine, market)
    fired = [r for r in injector.log if r.fault_type is FaultType.DISCONNECT and not r.shadowed]
    assert fired, "expected at least one DISCONNECT to fire in this run"
    assert driver.recoveries


def test_s7_delayed_snapshot_recovers_via_retry() -> None:
    market, injector, engine, driver = build_simulation(
        seed=1007,
        config=FaultConfig(
            drop_one_prob=0.1, delayed_snapshot_prob=0.6, delayed_snapshot_max_steps=5
        ),
    )

    driver.run(400)
    _drain_until_caught_up(driver, engine, market)

    assert_converged(engine, market)
    assert driver.stale_snapshot_retries > 0, "expected at least one SNAPSHOT_STALE observed"
    delayed_fired = [r for r in injector.log if r.fault_type is FaultType.DELAYED_SNAPSHOT]
    assert delayed_fired


def test_s8_fault_storm_converges() -> None:
    steps = 5000
    market, injector, engine, driver = build_simulation(seed=1008, config=_STORM_CONFIG)

    quiescent_checks = 0
    for _ in range(steps):
        driver.run_step()
        if _caught_up(engine, market):
            assert_converged(engine, market)
            quiescent_checks += 1
    _drain_until_caught_up(driver, engine, market)
    assert_converged(engine, market)

    assert quiescent_checks > steps // 2, (
        f"only {quiescent_checks}/{steps} ticks were caught up -- suspiciously low"
    )

    counts = summarize_log(injector.log)
    for fault_type in FaultType:
        fired, _shadowed = counts[fault_type]
        assert fired > 0, f"{fault_type} never fired in {steps} steps -- weak test"

    total_fired = sum(fired for fired, _shadowed in counts.values())
    total_shadowed = sum(shadowed for _fired, shadowed in counts.values())
    print(
        f"\nS8 fault storm: {steps} steps, {quiescent_checks} quiescent convergence "
        f"checks passed, {total_fired} faults fired, {total_shadowed} shadowed, "
        f"{len(driver.recoveries)} recoveries, {driver.stale_snapshot_retries} "
        f"stale-snapshot retries"
    )
    for fault_type in FaultType:
        fired, shadowed = counts[fault_type]
        print(f"  {fault_type.value:<18} fired={fired:<6} shadowed={shadowed}")


def test_d1_same_seed_is_fully_reproducible() -> None:
    seed = 4242
    steps = 2000

    market1, injector1, engine1, driver1 = build_simulation(seed=seed, config=_STORM_CONFIG)
    driver1.run(steps)

    market2, injector2, engine2, driver2 = build_simulation(seed=seed, config=_STORM_CONFIG)
    driver2.run(steps)

    assert injector1.log == injector2.log
    assert engine1.full_book() == engine2.full_book()
    assert engine1.last_applied_id == engine2.last_applied_id
    assert market1.oracle_book() == market2.oracle_book()
    assert driver1.recoveries == driver2.recoveries
