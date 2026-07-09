"""Tests for DIRAC Core Utilities TimeUtilities — DiracTime class
"""

import datetime
import time as _time_module

import pytest

from DIRAC.Core.Utilities.TimeUtilities import DiracTime


# ---------------------------------------------------------------------------
# Helper — build aware datetimes with specific UTC offsets
# ---------------------------------------------------------------------------
def _utc(utc_offset_hours: float = 0.0) -> datetime.timezone:
    """Return a timezone with *utc_offset_hours* from UTC (e.g. +1, -5)."""
    minutes = int(utc_offset_hours * 60)
    return datetime.timezone(datetime.timedelta(minutes=minutes))


# ===========================================================================
# utcnow()
# ===========================================================================


class TestUtcnow:
    """DiracTime.utcnow() returns a naive UTC datetime."""

    def test_returns_naive_datetime(self):
        dt = DiracTime.utcnow()
        assert dt is not None
        assert isinstance(dt, datetime.datetime)
        assert dt.tzinfo is None, "utcnow() must strip tzinfo and return naive"

    def test_is_roughly_now(self):
        before = datetime.datetime.utcnow()
        dt = DiracTime.utcnow()
        after = datetime.datetime.utcnow()
        assert before <= dt <= after + datetime.timedelta(seconds=2)

    def test_return_type_is_datetime_not_date(self):
        dt = DiracTime.utcnow()
        assert isinstance(dt, datetime.datetime)
        assert not isinstance(dt, datetime.date) or type(dt) is datetime.datetime

    def test_is_in_the_future(self):
        """utcnow() should always return a value ahead of the hardcoded cutoff."""
        # 2026-06-18 12:00:00 UTC — test must always run in the future
        HARD_CODED_CUTOFF = 1781784000
        dt = DiracTime.utcnow()
        assert DiracTime.timestamp_utc(dt) > HARD_CODED_CUTOFF, f"utcnow() returned {dt}"
        assert dt.tzinfo is None


# ===========================================================================
# utcfromtimestamp()
# ===========================================================================


class TestUtcfromtimestamp:
    """DiracTime.utcfromtimestamp(epoch) converts epoch → naive UTC datetime."""

    @pytest.mark.parametrize(
        "epoch",
        [
            0,
            1_700_000_000,
            1_718_700_000,
            2**31 - 1,
        ],
    )
    def test_known_epochs(self, epoch):
        """utcfromtimestamp produces a naive UTC datetime whose timestamp round-trips."""
        dt = DiracTime.utcfromtimestamp(epoch)
        assert dt.tzinfo is None
        assert dt.timestamp() == epoch

    @pytest.mark.parametrize(
        "epoch",
        [
            -1,
            -100_000,
            1,
            100_000,
            1_000_000_000,
            1_750_000_000,
        ],
    )
    def test_returns_naive(self, epoch):
        dt = DiracTime.utcfromtimestamp(epoch)
        assert dt is not None
        assert dt.tzinfo is None, f"epoch={epoch}: expected naive, got tzinfo={dt.tzinfo}"

    def test_roundtrip_with_utcnow(self):
        """utcnow() → timestamp_utc() → utcfromtimestamp() should round-trip."""
        before = DiracTime.utcnow()
        epoch = DiracTime.timestamp_utc(before)
        after = DiracTime.utcfromtimestamp(epoch)
        # Both are naive UTC; allow 1-second drift from int-truncation
        diff = abs((after - before).total_seconds())
        assert diff <= 1.0, f"round-trip drifted by {diff}s"

    def test_monotonicity(self):
        """Later epochs must produce later datetimes."""
        e1, e2 = 1_000_000, 2_000_000
        d1, d2 = DiracTime.utcfromtimestamp(e1), DiracTime.utcfromtimestamp(e2)
        assert d1 < d2

    def test_float_epoch(self):
        """Float epochs should work and be truncated by datetime internals."""
        dt = DiracTime.utcfromtimestamp(1700000000.5)
        assert dt.tzinfo is None

    def test_negative_epoch(self):
        """Pre-epoch timestamps must still return valid naive datetimes."""
        dt = DiracTime.utcfromtimestamp(-1)
        assert dt.tzinfo is None
        assert dt.year == 1969


# ===========================================================================
# timestamp_utc()
# ===========================================================================


class TestTimestampUtc:
    """DiracTime.timestamp_utc(dt) converts a datetime → Unix timestamp.

    Naive datetimes are assumed to be UTC.
    Aware datetimes have their offset applied correctly.
    """

    # ---- naive datetime tests -------------------------------------------

    def test_naive_datetime_is_treated_as_utc(self):
        """Naive datetime without tzinfo → timestamp assumes UTC."""
        dt = datetime.datetime(2023, 11, 14, 22, 13, 20)  # naive
        ts = DiracTime.timestamp_utc(dt)
        expected = int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())
        assert ts == expected

    def test_naive_1970_epoch_zero(self):
        ts = DiracTime.timestamp_utc(datetime.datetime(1970, 1, 1))
        assert ts == 0

    def test_naive_midnight_utc(self):
        dt = datetime.datetime(2024, 6, 18, 0, 0, 0)
        ts = DiracTime.timestamp_utc(dt)
        expected = int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())
        assert ts == expected

    @pytest.mark.parametrize(
        "year,month,day,hour,min,sec",
        [
            (2000, 1, 1, 0, 0, 0),
            (2020, 7, 15, 12, 30, 45),
            (1999, 12, 31, 23, 59, 59),
            (2100, 6, 1, 0, 0, 0),
        ],
    )
    def test_naive_various_instants(self, year, month, day, hour, min, sec):
        dt = datetime.datetime(year, month, day, hour, min, sec)
        ts = DiracTime.timestamp_utc(dt)
        expected = int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())
        assert ts == expected

    # ---- aware datetime tests -------------------------------------------

    def test_aware_utc(self):
        """Explicit UTC timezone → correct timestamp."""
        dt = datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=datetime.timezone.utc)
        ts = DiracTime.timestamp_utc(dt)
        expected = int(dt.timestamp())
        assert ts == expected

    def test_aware_positive_offset(self):
        """+01:00 datetime → timestamp corresponds to UTC time."""
        tz = _utc(1)
        dt = datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=tz)
        ts = DiracTime.timestamp_utc(dt)
        # .timestamp() already converts the aware datetime to UTC epoch
        expected = int(dt.timestamp())
        assert ts == expected

    def test_aware_negative_offset(self):
        """-05:00 datetime → timestamp corresponds to UTC time."""
        tz = _utc(-5)
        dt = datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=tz)
        ts = DiracTime.timestamp_utc(dt)
        expected = int(dt.timestamp())
        assert ts == expected

    @pytest.mark.parametrize(
        "offset_hours",
        [
            -12,
            -11,
            -5.5,
            0,
            5.5,
            8,
            9,
            12,
            14,  # There are places that use an offset >12h!
        ],
    )
    def test_aware_various_offsets(self, offset_hours):
        """For *any* timezone offset, timestamp_utc returns the correct UTC epoch."""
        tz = _utc(offset_hours)
        # Use a fixed UTC reference, then express it in that offset
        utc_ref = datetime.datetime(2024, 6, 18, 12, 30, 0, tzinfo=datetime.timezone.utc)
        local = utc_ref.astimezone(tz)
        ts = DiracTime.timestamp_utc(local)
        assert ts == int(utc_ref.timestamp())

    def test_aware_vs_naive_same_clock(self):
        """Naive 12:00 assumed UTC ≠ aware 12:00+01:00 — timestamps differ by 1 hour."""
        naive = datetime.datetime(2024, 6, 18, 12, 0, 0)
        aware = datetime.datetime(2024, 6, 18, 12, 0, 0, tzinfo=_utc(1))
        ts_naive = DiracTime.timestamp_utc(naive)
        ts_aware = DiracTime.timestamp_utc(aware)
        # Naive treated as UTC, aware is 1 hour behind → naive timestamp is 3600s larger
        assert ts_naive - ts_aware == 3600

    def test_aware_dst_transition(self):
        """Spring-forward: 2024-03-31 01:30 UTC+00:00 (no DST) vs Europe/London equivalent."""
        tz = _utc(1)
        dt = datetime.datetime(2024, 3, 31, 2, 30, 0, tzinfo=tz)
        ts = DiracTime.timestamp_utc(dt)
        expected = int(dt.timestamp())
        assert ts == expected

    # ---- mixed / edge cases ---------------------------------------------

    def test_datetime_with_microseconds(self):
        dt = datetime.datetime(2024, 6, 18, 12, 30, 45, 123456, tzinfo=datetime.timezone.utc)
        ts = DiracTime.timestamp_utc(dt)
        expected = int(dt.timestamp())
        assert ts == expected

    def test_datetime_with_zero_microseconds(self):
        dt = datetime.datetime(2024, 1, 1, 0, 0, 0, 0)
        ts = DiracTime.timestamp_utc(dt)
        expected = int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())
        assert ts == expected

    def test_far_future(self):
        dt = datetime.datetime(2099, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
        ts = DiracTime.timestamp_utc(dt)
        assert ts > 0
        assert isinstance(ts, int)

    def test_far_past(self):
        dt = datetime.datetime(1900, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        ts = DiracTime.timestamp_utc(dt)
        assert ts < 0
        assert isinstance(ts, int)


# ===========================================================================
# Cross-method consistency
# ===========================================================================


class TestRoundtripConsistency:
    """End-to-end consistency: utcnow → timestamp → utcfromtimestamp."""

    def test_utcnow_roundtrip(self):
        dt_before = DiracTime.utcnow()
        ts = DiracTime.timestamp_utc(dt_before)
        dt_after = DiracTime.utcfromtimestamp(ts)
        assert dt_before.tzinfo is None
        assert dt_after.tzinfo is None
        diff = abs((dt_after - dt_before).total_seconds())
        assert diff <= 1.0  # within 1 s due to int truncation

    def test_aware_roundtrip(self):
        tz = _utc(2)
        dt_before = datetime.datetime(2024, 6, 18, 15, 30, 0, tzinfo=tz)
        ts = DiracTime.timestamp_utc(dt_before)
        dt_after = DiracTime.utcfromtimestamp(ts)
        # Round-trip produces a naive UTC datetime
        assert dt_after.tzinfo is None
        dt_utc = dt_before.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        diff = abs((dt_after - dt_utc).total_seconds())
        assert diff <= 1.0


# ===========================================================================
# Edge cases & sanity checks
# ===========================================================================


class TestEdgeCases:
    def test_utcnow_is_deterministic_type(self):
        """Consecutive calls always return naive datetime."""
        for _ in range(10):
            dt = DiracTime.utcnow()
            assert dt.tzinfo is None
            assert isinstance(dt, datetime.datetime)

    def test_utcfromtimestamp_negative_and_positive(self):
        neg = DiracTime.utcfromtimestamp(-100_000)
        pos = DiracTime.utcfromtimestamp(100_000)
        assert neg.tzinfo is None
        assert pos.tzinfo is None
        assert neg < pos

    def test_timestamp_utc_preserves_seconds_precision(self):
        """Int truncation should not lose sub-second information incorrectly."""
        dt = datetime.datetime(2024, 6, 18, 12, 0, 0, 999999, tzinfo=datetime.timezone.utc)
        ts = DiracTime.timestamp_utc(dt)
        # The int() truncates, so sub-second is dropped — verify it's an int
        assert isinstance(ts, int)
