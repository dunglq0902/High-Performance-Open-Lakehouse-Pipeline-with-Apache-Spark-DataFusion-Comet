"""Deterministic paired AB/BA scheduling."""

from __future__ import annotations

import random


def paired_randomized_schedule(
    measurement_runs: int,
    seed: int,
    engines: tuple[str, str] = ("spark_baseline", "comet_accelerated"),
) -> list[dict[str, object]]:
    """Return balanced, deterministic AB/BA pairs.

    ``measurement_runs`` is the number of successful measurements required *per engine*.
    Every pair contains both engines, which makes paired speedup estimable by pair index.
    """

    if measurement_runs < 1:
        raise ValueError("measurement_runs must be positive")
    if len(set(engines)) != 2:
        raise ValueError("exactly two distinct engines are required")

    forward = engines
    reverse = (engines[1], engines[0])
    orders = [forward] * (measurement_runs // 2)
    orders += [reverse] * (measurement_runs // 2)
    rng = random.Random(seed)
    if measurement_runs % 2:
        orders.append(forward if rng.getrandbits(1) == 0 else reverse)
    rng.shuffle(orders)
    return [{"pair_index": index + 1, "order": list(order)} for index, order in enumerate(orders)]
