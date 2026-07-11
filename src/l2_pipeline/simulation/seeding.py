from __future__ import annotations

import random


def derive_seed(seed: int, label: str) -> int:
    """Derive an independent child seed from a top-level seed + label.

    Two components each given their own derived seed get independent RNG
    streams: adding or removing a random draw in one component never
    shifts what the other draws, unlike sharing a single random.Random
    instance would.
    """
    return random.Random(f"{seed}:{label}").getrandbits(64)
