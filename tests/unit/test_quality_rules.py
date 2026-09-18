from pix_fraud.domain.quality_rules import (
    fraud_rate_is_valid,
    critical_rate_is_valid,
    quality_score_is_valid,
)


def test_fraud_rate():
    assert fraud_rate_is_valid(0.01, 0.001, 0.03)
    assert not fraud_rate_is_valid(0.10, 0.001, 0.03)


def test_critical_rate():
    assert critical_rate_is_valid(0.04, 0.05)
    assert not critical_rate_is_valid(0.06, 0.05)


def test_quality_score():
    assert quality_score_is_valid(0.97, 0.95)
    assert not quality_score_is_valid(0.90, 0.95)
