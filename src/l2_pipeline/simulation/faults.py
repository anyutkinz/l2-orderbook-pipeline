from __future__ import annotations

import enum
import random
from collections import deque
from dataclasses import dataclass

from l2_pipeline.book.types import DiffEvent, SnapshotEvent
from l2_pipeline.simulation.market import MarketSimulator


class FaultType(enum.Enum):
    DROP_ONE = "drop_one"
    DROP_BURST = "drop_burst"
    DUPLICATE = "duplicate"
    REORDER = "reorder"
    DISCONNECT = "disconnect"
    DELAYED_SNAPSHOT = "delayed_snapshot"


@dataclass(frozen=True, slots=True)
class FaultConfig:
    drop_one_prob: float = 0.0
    drop_burst_prob: float = 0.0
    drop_burst_max_len: int = 3
    duplicate_prob: float = 0.0
    reorder_prob: float = 0.0
    disconnect_prob: float = 0.0
    disconnect_max_len: int = 20
    delayed_snapshot_prob: float = 0.0
    delayed_snapshot_max_steps: int = 5


@dataclass(frozen=True, slots=True)
class FaultRecord:
    step: int
    fault_type: FaultType
    detail: str
    shadowed: bool = False


def summarize_log(log: list[FaultRecord]) -> dict[FaultType, tuple[int, int]]:
    """Per fault type: (fired_count, shadowed_count)."""
    counts: dict[FaultType, list[int]] = {ft: [0, 0] for ft in FaultType}
    for record in log:
        counts[record.fault_type][1 if record.shadowed else 0] += 1
    return {ft: (c[0], c[1]) for ft, c in counts.items()}


class FaultInjector:
    """Sits between the market and the engine, deciding what actually gets
    delivered. Owns its own RNG stream -- construct with an already-derived
    seed, never a shared random.Random instance (see seeding.derive_seed).
    """

    def __init__(self, market: MarketSimulator, config: FaultConfig, seed: int) -> None:
        self._market = market
        self._config = config
        self._rng = random.Random(seed)
        self._step = 0
        self._window_remaining = 0
        self._held: DiffEvent | None = None
        self._log: list[FaultRecord] = []
        history_len = config.delayed_snapshot_max_steps + 1
        self._snapshot_history: deque[tuple[int, SnapshotEvent]] = deque(
            [(0, market.snapshot())], maxlen=history_len
        )

    @property
    def log(self) -> list[FaultRecord]:
        return list(self._log)

    @property
    def is_quiescent(self) -> bool:
        """True iff the injector itself has no pending internal state: no
        held reorder swap, no active drop/disconnect window. Note this is
        narrower than "engine has caught up to the market" -- a one-shot
        DROP_ONE leaves nothing pending here, but the engine won't notice
        the resulting gap until the next event's prev_id fails to chain.
        Useful for deciding whether it's safe to discard this injector
        (e.g. swapping in a fresh one) without losing in-flight state; not
        sufficient on its own for driving convergence-check timing.
        """
        return self._held is None and self._window_remaining == 0

    def poll(self) -> list[DiffEvent]:
        step = self._step
        self._step += 1
        event = self._market.step()
        self._snapshot_history.append((step + 1, self._market.snapshot()))

        # Resolve a previously-held reorder swap first: the real event for
        # this tick is delivered before the held one, completing the swap.
        # No other fault can also apply on a resolving tick -- keeps
        # reorder strictly "swap two adjacent events," nothing more.
        if self._held is not None:
            held = self._held
            self._held = None
            return [event, held]

        # An active drop window (from DISCONNECT or DROP_BURST) takes
        # unconditional precedence -- no other fault is rolled this tick.
        if self._window_remaining > 0:
            self._window_remaining -= 1
            return []

        rolls = [
            (FaultType.DISCONNECT, self._rng.random() < self._config.disconnect_prob),
            (FaultType.DROP_BURST, self._rng.random() < self._config.drop_burst_prob),
            (FaultType.DROP_ONE, self._rng.random() < self._config.drop_one_prob),
            (FaultType.DUPLICATE, self._rng.random() < self._config.duplicate_prob),
            (FaultType.REORDER, self._rng.random() < self._config.reorder_prob),
        ]
        winner = next((fault_type for fault_type, rolled in rolls if rolled), None)
        for fault_type, rolled in rolls:
            if rolled and fault_type is not winner:
                self._log.append(
                    FaultRecord(step, fault_type, "shadowed by precedence", shadowed=True)
                )

        if winner is None:
            return [event]

        if winner is FaultType.DISCONNECT:
            length = self._rng.randint(1, self._config.disconnect_max_len)
            self._window_remaining = length - 1
            self._log.append(FaultRecord(step, FaultType.DISCONNECT, f"window_len={length}"))
            return []

        if winner is FaultType.DROP_BURST:
            length = self._rng.randint(1, self._config.drop_burst_max_len)
            self._window_remaining = length - 1
            self._log.append(FaultRecord(step, FaultType.DROP_BURST, f"window_len={length}"))
            return []

        if winner is FaultType.DROP_ONE:
            self._log.append(FaultRecord(step, FaultType.DROP_ONE, f"final_id={event.final_id}"))
            return []

        if winner is FaultType.DUPLICATE:
            self._log.append(FaultRecord(step, FaultType.DUPLICATE, f"final_id={event.final_id}"))
            return [event, event]

        self._held = event
        self._log.append(FaultRecord(step, FaultType.REORDER, f"held final_id={event.final_id}"))
        return []

    def request_snapshot(self) -> SnapshotEvent:
        if (
            self._config.delayed_snapshot_prob > 0
            and len(self._snapshot_history) > 1
            and self._rng.random() < self._config.delayed_snapshot_prob
        ):
            max_lookback = min(
                self._config.delayed_snapshot_max_steps, len(self._snapshot_history) - 1
            )
            lookback = self._rng.randint(1, max_lookback)
            aged_step, aged_snapshot = self._snapshot_history[-1 - lookback]
            self._log.append(
                FaultRecord(
                    self._step,
                    FaultType.DELAYED_SNAPSHOT,
                    f"served snapshot captured at step={aged_step} (lookback={lookback})",
                )
            )
            return aged_snapshot
        return self._snapshot_history[-1][1]
