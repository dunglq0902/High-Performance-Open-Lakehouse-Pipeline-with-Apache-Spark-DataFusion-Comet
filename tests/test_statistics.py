import random

import pytest

from benchmark.runner.statistics import (
    PairFailure,
    bootstrap_percentile_interval,
    describe,
    pair_speedups_by_id,
    paired_bootstrap_median_speedup,
    percentile,
)


def test_percentile_uses_linear_r7_and_validates_inputs() -> None:
    assert percentile([1.0, 2.0, 3.0, 4.0], 0.25) == 1.75
    assert percentile([1.0, 2.0, 3.0, 4.0], 0.95) == pytest.approx(3.85)

    with pytest.raises(ValueError, match="empty"):
        percentile([], 0.5)
    with pytest.raises(ValueError, match="between zero and one"):
        percentile([1.0], 1.01)
    with pytest.raises(ValueError, match="finite numeric"):
        percentile([1.0, float("nan")], 0.5)


def test_describe_suppresses_p95_until_twenty_successes() -> None:
    assert describe([float(value) for value in range(19)])["p95"] is None
    assert describe([float(value) for value in range(20)])["p95"] == pytest.approx(18.05)


def test_bootstrap_interval_is_seeded_and_does_not_change_global_random_state() -> None:
    values = [1.0, 1.5, 2.0, 3.0, 5.0]
    random.seed(99)
    state = random.getstate()

    first = bootstrap_percentile_interval(values, resamples=2_000, seed=42)
    second = bootstrap_percentile_interval(values, resamples=2_000, seed=42)

    assert first == second
    assert first.lower <= 2.0 <= first.upper
    assert first.confidence_level == 0.95
    assert first.method == "percentile"
    assert first.percentile_method == "linear-r7"
    assert first.resamples == 2_000
    assert first.seed == 42
    assert random.getstate() == state


def test_pair_speedups_match_by_id_and_report_every_excluded_pair() -> None:
    samples = pair_speedups_by_id(
        {2: 200.0, 1: 100.0, 3: None, 4: 80.0},
        {1: 50.0, 2: 100.0, 3: 75.0, 5: 40.0},
    )

    assert samples.pair_ids == (1, 2)
    assert samples.speedups == (2.0, 2.0)
    assert samples.n_pairs_total == 5
    assert samples.n_pairs_succeeded == 2
    assert samples.n_pairs_failed == 3
    assert samples.failures == (
        PairFailure(pair_id=3, missing_engines=(), failed_engines=("spark_baseline",)),
        PairFailure(pair_id=4, missing_engines=("comet_accelerated",), failed_engines=()),
        PairFailure(pair_id=5, missing_engines=("spark_baseline",), failed_engines=()),
    )


def test_paired_bootstrap_reports_median_ci_seed_and_failures() -> None:
    spark = {1: 10.0, 2: 20.0, 3: 30.0, 4: None, 5: 50.0}
    comet = {5: 10.0, 3: 10.0, 2: 10.0, 1: 10.0, 4: 10.0}

    first = paired_bootstrap_median_speedup(spark, comet, resamples=2_000, seed=7)
    second = paired_bootstrap_median_speedup(spark, comet, resamples=2_000, seed=7)

    assert first == second
    assert first.n_pairs_total == 5
    assert first.n_pairs_succeeded == 4
    assert first.n_pairs_failed == 1
    assert first.median_speedup == 2.5
    assert first.confidence_interval is not None
    assert first.confidence_interval.lower <= first.median_speedup
    assert first.confidence_interval.upper >= first.median_speedup
    assert first.failures[0].pair_id == 4
    assert first.failures[0].failed_engines == ("spark_baseline",)
    assert first.resamples == 2_000
    assert first.seed == 7


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"confidence_level": 1.0}, "confidence_level"),
        ({"resamples": 0}, "resamples"),
        ({"seed": True}, "seed"),
    ],
)
def test_bootstrap_rejects_invalid_parameters(kwargs: dict[str, float | int], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        bootstrap_percentile_interval([1.0, 2.0], **kwargs)


def test_paired_speedup_rejects_invalid_latency_and_pair_id() -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        pair_speedups_by_id({1: 10.0}, {1: 0.0})
    with pytest.raises(ValueError, match="pair IDs"):
        pair_speedups_by_id({True: 10.0}, {True: 5.0})
