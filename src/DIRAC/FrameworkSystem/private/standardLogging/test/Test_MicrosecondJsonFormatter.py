"""
Test MicrosecondJsonFormatter
"""

import logging

from DIRAC.FrameworkSystem.private.standardLogging.Formatter.MicrosecondJsonFormatter import MicrosecondJsonFormatter


DIRAC_CUSTOM_ATTRS = [
    "spacer",
    "headerIsShown",
    "timeStampIsShown",
    "contextIsShown",
    "threadIDIsShown",
    "color",
]


def test_init_with_tuple_reserved_attrs():
    """Test that MicrosecondJsonFormatter works when RESERVED_ATTRS is a tuple (python-json-logger v2.x)"""
    from pythonjsonlogger import jsonlogger

    original = jsonlogger.RESERVED_ATTRS
    try:
        jsonlogger.RESERVED_ATTRS = ("args", "asctime", "created")
        formatter = MicrosecondJsonFormatter()
        assert isinstance(formatter.reserved_attrs, set)
        for attr in DIRAC_CUSTOM_ATTRS:
            assert attr in formatter.reserved_attrs
    finally:
        jsonlogger.RESERVED_ATTRS = original


def test_init_with_list_reserved_attrs():
    """Test that MicrosecondJsonFormatter works when RESERVED_ATTRS is a list (python-json-logger v3.x+)"""
    from pythonjsonlogger import jsonlogger

    original = jsonlogger.RESERVED_ATTRS
    try:
        jsonlogger.RESERVED_ATTRS = ["args", "asctime", "created"]
        formatter = MicrosecondJsonFormatter()
        assert isinstance(formatter.reserved_attrs, set)
        for attr in DIRAC_CUSTOM_ATTRS:
            assert attr in formatter.reserved_attrs
    finally:
        jsonlogger.RESERVED_ATTRS = original


def test_formatTime_microsecond_precision():
    """Test that formatTime produces microsecond precision timestamps"""
    formatter = MicrosecondJsonFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="test message",
        args=(),
        exc_info=None,
    )
    record.created = 1700000000.123456

    result = formatter.formatTime(record)
    assert result.endswith(",123456")


def test_formatTime_custom_datefmt():
    """Test that formatTime respects custom datefmt"""
    formatter = MicrosecondJsonFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="test message",
        args=(),
        exc_info=None,
    )
    record.created = 1700000000.0

    result = formatter.formatTime(record, datefmt="%Y/%m/%d")
    assert "/" in result
