def fraud_rate_is_valid(rate, minimum, maximum):
    return minimum <= rate <= maximum


def critical_rate_is_valid(rate, maximum):
    return rate <= maximum


def quality_score_is_valid(score, minimum):
    return score >= minimum
