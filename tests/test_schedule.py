from benchmark.runner.schedule import paired_randomized_schedule


def test_schedule_is_deterministic_balanced_and_paired() -> None:
    first = paired_randomized_schedule(21, 20260824)
    second = paired_randomized_schedule(21, 20260824)

    assert first == second
    assert [pair["pair_index"] for pair in first] == list(range(1, 22))
    assert all(set(pair["order"]) == {"spark_baseline", "comet_accelerated"} for pair in first)
    forward = sum(pair["order"][0] == "spark_baseline" for pair in first)
    assert abs(forward - (len(first) - forward)) <= 1


def test_schedule_rejects_invalid_inputs() -> None:
    import pytest

    with pytest.raises(ValueError, match="positive"):
        paired_randomized_schedule(0, 1)
    with pytest.raises(ValueError, match="distinct"):
        paired_randomized_schedule(1, 1, ("same", "same"))
