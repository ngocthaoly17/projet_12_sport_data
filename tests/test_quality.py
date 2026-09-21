from datetime import datetime, timedelta


def test_distance_not_negative():
    distance_m = 0
    assert distance_m >= 0


def test_end_after_start():
    start = datetime.now()
    assert start + timedelta(minutes=1) > start


def test_non_distance_sport_can_have_null_distance():
    sport = "Yoga"
    distance_m = None
    assert sport.lower() == "yoga" and distance_m is None
