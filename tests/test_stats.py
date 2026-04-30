from utils.stats import calculate_statistics, calculate_confidence_interval

def test_calculate_statistics_basic():
    result = calculate_statistics([1.8, 2.0, 2.2])

    assert result["mean"] == 2.0
    assert result["min"] == 1.8
    assert result["max"] == 2.2
    assert result["count"] == 3

def test_calculate_statistics_empty():
    result = calculate_statistics([])

    assert result["mean"] == 0.0
    assert result["std"] == 0.0
    assert result["min"] == 0.0
    assert result["max"] == 0.0
    assert result["count"] == 0

def test_confidence_interval_too_few_values():
    low, high = calculate_confidence_interval([2.0])

    assert low == 0.0
    assert high == 0.0
