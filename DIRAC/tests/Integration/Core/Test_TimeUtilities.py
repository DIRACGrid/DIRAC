"""Test for the TimeUtilities module
"""
import pytest

import datetime, pytz

from DIRAC.Core.Utilities import TimeUtilities

americanTimezone = pytz.timezone("America/Los_Angeles")
japaneseTimezone = pytz.timezone("Asia/Tokyo")
euroTimezone = pytz.timezone("Europe/Rome")


@pytest.mark.parametrize(
    "dt, expected, expectedMillis, timezone",
    [
        # Try with a datetime type object
        (datetime.datetime(1997, 6, 12, 2, 30, 0, 0), 866082600, None, None),
        # Now with a date type object, it should return the timestamp of the date with the time set to midnight
        (datetime.date(1997, 6, 12), 866073600, None, None),
        # Try the same with Milliseconds
        (datetime.datetime(1997, 6, 12, 2, 30, 0, 0), None, 866082600000, None),
        (datetime.date(1997, 6, 12), None, 866073600000, None),
        # Try with timezone aware datetimes
        (datetime.datetime(1997, 6, 12, 2, 30, 0, 0), None, 866082600000, americanTimezone),
        (datetime.datetime(1997, 6, 12, 2, 30, 0, 0), None, 866082600000, japaneseTimezone),
        (datetime.datetime(1997, 6, 12, 2, 30, 0, 0), None, 866082600000, euroTimezone),
    ],
)
def test_toEpoch(dt, expected, expectedMillis, timezone):
    if timezone:
        dt = timezone.localize(dt)
    if expected:
        res = TimeUtilities.toEpoch(dt)
        assert res == expected
    # Try toEpochMilliseconds
    if expectedMillis:
        res = TimeUtilities.toEpochMilliSeconds(dt)
        assert res == expectedMillis


@pytest.mark.parametrize(
    "dt, expected",
    [
        (1656063560, datetime.datetime(2022, 6, 24, 9, 39, 20, 0)),
        (1656063560000, datetime.datetime(2022, 6, 24, 9, 39, 20, 0)),
        (1656063560000000000, datetime.datetime(2022, 6, 24, 9, 39, 20, 0)),
    ],
)
def test_fromEpoch(dt, expected):
    res = TimeUtilities.fromEpoch(dt)
    assert res == expected
