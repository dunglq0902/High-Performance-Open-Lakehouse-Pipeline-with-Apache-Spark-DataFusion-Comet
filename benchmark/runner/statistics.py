"""Deterministic descriptive statistics for paired benchmark measurements."""

from __future__ import annotations

import math
import random
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

DEFAULT_BOOTSTRAP_RESAMPLES = 10_000
DEFAULT_BOOTSTRAP_SEED = 20260824
MIN_P95_SAMPLE_SIZE = 20

type PairId = int | str
type EngineName = Literal["spark_baseline", "comet_accelerated"]


@dataclass(frozen=True, slots=True)
class PercentileConfidenceInterval:
    """A two-sided percentile bootstrap confidence interval."""

    lower: float
    upper: float
    confidence_level: float
    resamples: int
    seed: int
    method: Literal["percentile"] = "percentile"
    percentile_method: Literal["linear-r7"] = "linear-r7"


@dataclass(frozen=True, slots=True)
class PairFailure:
    """Why a pair was excluded from the paired estimator."""

    pair_id: PairId
    missing_engines: tuple[EngineName, ...]
    failed_engines: tuple[EngineName, ...]


@dataclass(frozen=True, slots=True)
class PairedSamples:
    """Valid paired speedups plus every excluded pair and its reason."""

    pair_ids: tuple[PairId, ...]
    speedups: tuple[float, ...]
    failures: tuple[PairFailure, ...]

    @property
    def n_pairs_total(self) -> int:
        return len(self.pair_ids) + len(self.failures)

    @property
    def n_pairs_succeeded(self) -> int:
        return len(self.pair_ids)

    @property
    def n_pairs_failed(self) -> int:
        return len(self.failures)


@dataclass(frozen=True, slots=True)
class PairedBootstrapResult:
    """Median paired speedup and its reproducible percentile interval."""

    n_pairs_total: int
    n_pairs_succeeded: int
    n_pairs_failed: int
    median_speedup: float | None
    confidence_interval: PercentileConfidenceInterval | None
    failures: tuple[PairFailure, ...]
    resamples: int
    seed: int


def percentile(values: Sequence[float], probability: float) -> float:
    """Return the R-7 linearly interpolated percentile."""

    if not values:
        raise ValueError("cannot compute a percentile of an empty sequence")
    if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
        raise ValueError("probability must be finite and between zero and one")
    _require_finite_values(values)

    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    weight = position - lower
    return float(ordered[lower] * (1 - weight) + ordered[upper] * weight)


def describe(values: Sequence[float]) -> dict[str, float | int | None]:
    """Return schema-v1 descriptive statistics for successful observations."""

    _require_finite_values(values)
    if not values:
        return {
            "n": 0,
            "median": None,
            "mean": None,
            "stddev": None,
            "q1": None,
            "q3": None,
            "iqr": None,
            "min": None,
            "max": None,
            "p95": None,
        }
    q1, q3 = percentile(values, 0.25), percentile(values, 0.75)
    return {
        "n": len(values),
        "median": statistics.median(values),
        "mean": statistics.fmean(values),
        "stddev": statistics.stdev(values) if len(values) > 1 else 0.0,
        "q1": q1,
        "q3": q3,
        "iqr": q3 - q1,
        "min": min(values),
        "max": max(values),
        "p95": percentile(values, 0.95) if len(values) >= MIN_P95_SAMPLE_SIZE else None,
    }


def bootstrap_percentile_interval(
    values: Sequence[float],
    *,
    confidence_level: float = 0.95,
    resamples: int = DEFAULT_BOOTSTRAP_RESAMPLES,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> PercentileConfidenceInterval:
    """Bootstrap a confidence interval for the sample median.

    Sampling is performed with a private ``random.Random`` instance, so calls do not
    mutate global random state and the same inputs and seed always produce the same
    interval.
    """

    if not values:
        raise ValueError("cannot bootstrap an empty sequence")
    _require_finite_values(values)
    if not math.isfinite(confidence_level) or not 0.0 < confidence_level < 1.0:
        raise ValueError("confidence_level must be finite and strictly between zero and one")
    if isinstance(resamples, bool) or not isinstance(resamples, int) or resamples < 1:
        raise ValueError("resamples must be a positive integer")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")

    sample = tuple(float(value) for value in values)
    sample_size = len(sample)
    rng = random.Random(seed)
    estimates = [
        float(statistics.median(sample[rng.randrange(sample_size)] for _ in range(sample_size)))
        for _ in range(resamples)
    ]
    tail_probability = (1.0 - confidence_level) / 2.0
    return PercentileConfidenceInterval(
        lower=percentile(estimates, tail_probability),
        upper=percentile(estimates, 1.0 - tail_probability),
        confidence_level=confidence_level,
        resamples=resamples,
        seed=seed,
    )


def pair_speedups_by_id(
    spark_by_pair: Mapping[PairId, float | None],
    comet_by_pair: Mapping[PairId, float | None],
) -> PairedSamples:
    """Match measurements by pair ID and calculate ``Spark / Comet`` speedups.

    ``None`` denotes a recorded failed measurement. A key absent from one mapping
    denotes a missing engine record. Both cases remain explicit in ``failures``.
    """

    _validate_pair_ids(spark_by_pair)
    _validate_pair_ids(comet_by_pair)
    all_pair_ids = sorted(
        spark_by_pair.keys() | comet_by_pair.keys(),
        key=lambda pair_id: (type(pair_id).__name__, str(pair_id)),
    )
    complete_pair_ids: list[PairId] = []
    speedups: list[float] = []
    failures: list[PairFailure] = []

    for pair_id in all_pair_ids:
        missing_engines: list[EngineName] = []
        failed_engines: list[EngineName] = []
        if pair_id not in spark_by_pair:
            missing_engines.append("spark_baseline")
        elif spark_by_pair[pair_id] is None:
            failed_engines.append("spark_baseline")
        if pair_id not in comet_by_pair:
            missing_engines.append("comet_accelerated")
        elif comet_by_pair[pair_id] is None:
            failed_engines.append("comet_accelerated")

        if missing_engines or failed_engines:
            failures.append(
                PairFailure(
                    pair_id=pair_id,
                    missing_engines=tuple(missing_engines),
                    failed_engines=tuple(failed_engines),
                )
            )
            continue

        spark_latency = _require_positive_latency(spark_by_pair[pair_id], "spark_baseline")
        comet_latency = _require_positive_latency(comet_by_pair[pair_id], "comet_accelerated")
        complete_pair_ids.append(pair_id)
        speedups.append(spark_latency / comet_latency)

    return PairedSamples(
        pair_ids=tuple(complete_pair_ids),
        speedups=tuple(speedups),
        failures=tuple(failures),
    )


def paired_bootstrap_median_speedup(
    spark_by_pair: Mapping[PairId, float | None],
    comet_by_pair: Mapping[PairId, float | None],
    *,
    confidence_level: float = 0.95,
    resamples: int = DEFAULT_BOOTSTRAP_RESAMPLES,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> PairedBootstrapResult:
    """Estimate median paired speedup and its deterministic percentile CI."""

    samples = pair_speedups_by_id(spark_by_pair, comet_by_pair)
    median_speedup: float | None = None
    confidence_interval: PercentileConfidenceInterval | None = None
    if samples.speedups:
        median_speedup = float(statistics.median(samples.speedups))
        confidence_interval = bootstrap_percentile_interval(
            samples.speedups,
            confidence_level=confidence_level,
            resamples=resamples,
            seed=seed,
        )
    else:
        _validate_bootstrap_parameters(confidence_level, resamples, seed)

    return PairedBootstrapResult(
        n_pairs_total=samples.n_pairs_total,
        n_pairs_succeeded=samples.n_pairs_succeeded,
        n_pairs_failed=samples.n_pairs_failed,
        median_speedup=median_speedup,
        confidence_interval=confidence_interval,
        failures=samples.failures,
        resamples=resamples,
        seed=seed,
    )


def _validate_bootstrap_parameters(confidence_level: float, resamples: int, seed: int) -> None:
    if not math.isfinite(confidence_level) or not 0.0 < confidence_level < 1.0:
        raise ValueError("confidence_level must be finite and strictly between zero and one")
    if isinstance(resamples, bool) or not isinstance(resamples, int) or resamples < 1:
        raise ValueError("resamples must be a positive integer")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")


def _require_finite_values(values: Sequence[float]) -> None:
    if any(isinstance(value, bool) or not math.isfinite(value) for value in values):
        raise ValueError("statistical samples must contain only finite numeric values")


def _require_positive_latency(value: float | None, engine: EngineName) -> float:
    if value is None:
        raise AssertionError("failed measurements must be handled before latency validation")
    if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{engine} latency must be finite and greater than zero")
    return float(value)


def _validate_pair_ids(values: Mapping[PairId, float | None]) -> None:
    if any(isinstance(pair_id, bool) or not isinstance(pair_id, int | str) for pair_id in values):
        raise ValueError("pair IDs must be integers or strings, not booleans")
