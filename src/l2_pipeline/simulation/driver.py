from __future__ import annotations

from dataclasses import dataclass

from l2_pipeline.book.engine import BookEngine
from l2_pipeline.book.types import ApplyStatus, BookState
from l2_pipeline.simulation.faults import FaultConfig, FaultInjector
from l2_pipeline.simulation.market import MarketSimulator
from l2_pipeline.simulation.seeding import derive_seed

DEFAULT_SNAPSHOT_RETRY_LIMIT = 20


class SnapshotRetryLimitExceeded(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RecoveryRecord:
    gap_step: int
    recovered_step: int


class SimulatedFeedDriver:
    """Plays the role of the M3 feed client: pulls events from the
    injector, feeds them to the engine, and drives resync on gaps. Never
    touches engine internals directly -- buffer ownership stays with
    BookEngine, per M1.
    """

    def __init__(
        self,
        injector: FaultInjector,
        engine: BookEngine,
        seed: int | None = None,
        snapshot_retry_limit: int = DEFAULT_SNAPSHOT_RETRY_LIMIT,
    ) -> None:
        self._injector = injector
        self._engine = engine
        self._seed = seed
        self._snapshot_retry_limit = snapshot_retry_limit
        self._recoveries: list[RecoveryRecord] = []
        self._stale_snapshot_retries = 0
        self._step = 0
        if self._engine.state is not BookState.LIVE:
            self._sync()

    @property
    def recoveries(self) -> list[RecoveryRecord]:
        return list(self._recoveries)

    @property
    def stale_snapshot_retries(self) -> int:
        return self._stale_snapshot_retries

    def run(self, steps: int) -> None:
        for _ in range(steps):
            self.run_step()

    def run_step(self) -> None:
        step = self._step
        self._step += 1
        gap_this_step = False
        for event in self._injector.poll():
            result = self._engine.apply_event(event)
            if result.status is ApplyStatus.GAP_DETECTED:
                gap_this_step = True
        if gap_this_step:
            self._sync(gap_step=step)

    def _sync(self, gap_step: int | None = None) -> None:
        for _ in range(self._snapshot_retry_limit):
            snapshot = self._injector.request_snapshot()
            result = self._engine.load_snapshot(snapshot)
            if result.status is ApplyStatus.APPLIED:
                if gap_step is not None:
                    self._recoveries.append(
                        RecoveryRecord(gap_step=gap_step, recovered_step=self._step)
                    )
                return
            self._stale_snapshot_retries += 1
        raise SnapshotRetryLimitExceeded(
            f"exceeded {self._snapshot_retry_limit} snapshot retry attempts without "
            f"reaching LIVE (step={self._step}, seed={self._seed})"
        )


def build_simulation(
    seed: int,
    config: FaultConfig,
    depth_levels: int = 20,
    snapshot_retry_limit: int = DEFAULT_SNAPSHOT_RETRY_LIMIT,
) -> tuple[MarketSimulator, FaultInjector, BookEngine, SimulatedFeedDriver]:
    market = MarketSimulator(seed=derive_seed(seed, "market"))
    injector = FaultInjector(market=market, config=config, seed=derive_seed(seed, "fault"))
    engine = BookEngine(depth_levels=depth_levels)
    driver = SimulatedFeedDriver(
        injector=injector, engine=engine, seed=seed, snapshot_retry_limit=snapshot_retry_limit
    )
    return market, injector, engine, driver
