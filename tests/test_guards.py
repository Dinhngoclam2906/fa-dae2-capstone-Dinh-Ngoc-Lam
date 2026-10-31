"""Published-app guards: password gate and the app-wide daily question limit."""

from core.guards import DailyLimit, password_ok


def test_no_password_set_lets_everyone_in():
    assert password_ok("", "")
    assert password_ok("anything", "")


def test_password_must_match_exactly():
    assert password_ok("s3cret", "s3cret")
    assert not password_ok("", "s3cret")
    assert not password_ok("S3cret", "s3cret")
    assert not password_ok("s3cret ", "s3cret")


def test_daily_limit_stops_at_the_limit():
    lim = DailyLimit(2, today=lambda: "2026-10-31")
    assert lim.take() and lim.take()
    assert not lim.take()
    assert lim.used == 2


def test_daily_limit_resets_on_a_new_utc_day():
    day = ["2026-10-31"]
    lim = DailyLimit(1, today=lambda: day[0])
    assert lim.take()
    assert not lim.take()
    day[0] = "2026-11-01"
    assert lim.used == 0
    assert lim.take()


def test_zero_means_no_limit():
    lim = DailyLimit(0, today=lambda: "2026-10-31")
    assert all(lim.take() for _ in range(1000))
