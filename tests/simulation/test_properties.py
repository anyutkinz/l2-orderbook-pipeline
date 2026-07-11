from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from l2_pipeline.simulation import FaultConfig, FaultInjector, assert_converged, build_simulation
from l2_pipeline.simulation.driver import SimulatedFeedDriver
from l2_pipeline.simulation.seeding import derive_seed

_TAIL_STEPS = 40


@st.composite
def fault_configs(draw: st.DrawFn) -> FaultConfig:
    return FaultConfig(
        drop_one_prob=draw(st.floats(0.0, 0.2)),
        drop_burst_prob=draw(st.floats(0.0, 0.1)),
        drop_burst_max_len=draw(st.integers(1, 6)),
        duplicate_prob=draw(st.floats(0.0, 0.2)),
        reorder_prob=draw(st.floats(0.0, 0.2)),
        disconnect_prob=draw(st.floats(0.0, 0.05)),
        disconnect_max_len=draw(st.integers(1, 20)),
        delayed_snapshot_prob=draw(st.floats(0.0, 0.8)),
        delayed_snapshot_max_steps=draw(st.integers(1, 8)),
    )


@settings(max_examples=200, deadline=None)
@given(
    seed=st.integers(min_value=0, max_value=2**32 - 1),
    config=fault_configs(),
    steps=st.integers(30, 200),
)
def test_h1_converges_under_arbitrary_fault_config(
    seed: int, config: FaultConfig, steps: int
) -> None:
    market, _injector, engine, driver = build_simulation(seed, config)
    driver.run(steps)

    # Fault-free tail: standard DST practice to let any in-flight recovery
    # settle before the final assert. In this architecture every run_step()
    # already guarantees LIVE-and-converged (or raises) before returning --
    # there's no genuinely async in-flight state -- so this tail is a hedge
    # for a future async feed client (M3), not covering a real gap today.
    tail_injector = FaultInjector(
        market=market, config=FaultConfig(), seed=derive_seed(seed, "tail")
    )
    tail_driver = SimulatedFeedDriver(injector=tail_injector, engine=engine, seed=seed)
    tail_driver.run(_TAIL_STEPS)

    assert_converged(engine, market)
