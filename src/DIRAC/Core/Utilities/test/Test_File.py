""" Test case for DIRAC.Core.Utilities.File module
"""
##
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/01/17 14:01:18
# @brief Definition of FileTestCase class.

# imports
import os
import time
from os.path import abspath
import re
import sys
from pathlib import Path

from hypothesis import given
from hypothesis.strategies import floats

from pytest import mark
from unittest.mock import MagicMock

# sut
from DIRAC.Core.Utilities.File import (
    checkGuid,
    cleanDirectory,
    makeGuid,
    getSize,
    getMD5ForFiles,
    convertSizeUnits,
    SIZE_UNIT_CONVERSION,
)

parametrize = mark.parametrize


def testCheckGuid():
    """checkGuid tests"""
    # empty string
    guid = ""
    assert checkGuid(guid) is False, "empty guid"

    # wrong length in a 1st field
    guid = "012345678-0123-0123-0123-0123456789AB"
    assert checkGuid(guid) is False, "wrong length in 1st field"
    guid = "0123456-0123-0123-0123-0123456789AB"
    assert checkGuid(guid) is False, "wrong length in 1st field"

    # wrong length in a 2nd field
    guid = "01234567-01234-0123-0123-0123456789AB"
    assert checkGuid(guid) is False, "wrong length in 2nd field"
    guid = "01234567-012-0123-0123-0123456789AB"
    assert checkGuid(guid) is False, "wrong length in 2nd field"

    # wrong length in a 3rd field
    guid = "01234567-0123-01234-0123-0123456789AB"
    assert checkGuid(guid) is False, "wrong length in 3rd field"
    guid = "01234567-0123-012-0123-0123456789AB"
    assert checkGuid(guid) is False, "wrong length in 3rd field"

    # wrong length in a 4th field
    guid = "01234567-0123-0123-01234-0123456789AB"
    assert checkGuid(guid) is False, "wrong length in 4th field"
    guid = "01234567-0123-0123-012-0123456789AB"
    assert checkGuid(guid) is False, "wrong length in 4th field"

    # wrong length in a 5th field
    guid = "01234567-0123-0123-0123-0123-0123456789ABC"
    assert checkGuid(guid) is False, "wrong length in 5th field"
    guid = "01234567-0123-0123-0123-0123-0123456789A"
    assert checkGuid(guid) is False, "wrong length in 5th field"

    # small caps
    guid = "01234567-9ABC-0DEF-0123-456789ABCDEF".lower()
    assert checkGuid(guid) is True, "small caps in guid, zut!"

    # wrong characters not in [0-9A-F]
    guid = "NEEDMORE-SPAM-SPAM-SPAM-SPAMWITHEGGS"
    assert checkGuid(guid) is True, "wrong set of characters, zut!"

    # normal operation
    guid = "01234567-9ABC-0DEF-0123-456789ABCDEF"
    assert checkGuid(guid) is True, "proper GUID"


def testMakeGuid():
    """makeGuid tests"""
    # no filename - fake guid produced
    assert checkGuid(makeGuid()) is True, "fake guid for inexisting file"
    # using this python file
    assert checkGuid(makeGuid(__file__)) is True, "guid for FileTestCase.py file"


def testGetSize():
    """getSize tests"""
    # non existing file
    assert getSize("/spam/eggs/eggs") == -1, "inexisting file"
    # file unreadable
    assert getSize("/root/.login") == -1, "unreadable file"


def testGetMD5ForFiles():
    """getMD5ForFiles tests"""

    filesList = [abspath(".") + os.sep + x for x in os.listdir(".")]
    md5sum = getMD5ForFiles(filesList)
    reMD5 = re.compile("^[0-9a-fA-F]+$")
    assert reMD5.match(md5sum) is not None


@given(nb=floats(allow_nan=False, allow_infinity=False, min_value=1))
def test_convert_to_bigger_unit_floats(nb):
    """Make sure that converting to bigger unit gets the number smaller .
    Also tests that two steps are equal to two consecutive steps
    """
    toKB = convertSizeUnits(nb, "B", "kB")
    toMB = convertSizeUnits(nb, "B", "MB")
    fromkBtoMB = convertSizeUnits(toKB, "kB", "MB")

    assert toKB < nb
    assert toMB < toKB
    assert toMB == fromkBtoMB


def test_convert_error_to_maxint():
    """Make sure that on error we receive -sys.maxint"""
    assert convertSizeUnits("size", "B", "kB") == -sys.maxsize
    assert convertSizeUnits(0, "srcUnit", "kB") == -sys.maxsize
    assert convertSizeUnits(0, "B", "dstUnit") == -sys.maxsize


@given(nb=floats(allow_nan=False, allow_infinity=False, min_value=1))
@parametrize("srcUnit", SIZE_UNIT_CONVERSION)
@parametrize("dstUnit", SIZE_UNIT_CONVERSION)
def test_convert_loop(nb, srcUnit, dstUnit):
    """Make sure that converting a size back and forth preserves the number"""

    converted = convertSizeUnits(convertSizeUnits(nb, srcUnit, dstUnit), dstUnit, srcUnit)
    # We exclude the infinity case
    if converted != float("Inf"):
        assert converted == nb


def _set_old_mtime(path: str, age_seconds: int = 3600) -> None:
    """Set a file's mtime to `age_seconds` ago."""
    old_time = time.time() - age_seconds
    os.utime(path, (old_time, old_time))


def _build_tree(tmp_path, files, subdirs=None):
    """Create a file tree and return a mapping of name -> pathlib.Path."""
    mapping = {}
    if subdirs:
        for sd in subdirs:
            (tmp_path / sd).mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        mapping[name] = tmp_path / name
        mapping[name].write_text(content)
    return mapping


def test_clean_directory_basic(tmp_path):
    """Check old file removed, newer file kept and old-non-matching kept."""
    files = _build_tree(
        tmp_path,
        {"DIRAC_old_job": "abc", "DIRAC_new_job": "def", "other.log": "ghi"},
    )
    _set_old_mtime(str(files["DIRAC_old_job"]))
    _set_old_mtime(str(files["other.log"]))

    err = cleanDirectory(str(tmp_path), maxSecs=60, filePatterns=["DIRAC_*"])
    assert err == []
    assert not files["DIRAC_old_job"].exists()
    assert files["DIRAC_new_job"].exists()
    assert files["other.log"].exists()


def test_clean_directory_empty_dir(tmp_path):
    assert cleanDirectory(str(tmp_path)) == []


def test_clean_directory_maxDepth_restricts_recursion(tmp_path):
    files = _build_tree(
        tmp_path,
        {"root": "a", "DIRAC_root_job": "b"},
        subdirs=["subdir"],
    )
    _set_old_mtime(str(files["root"]))
    _set_old_mtime(str(files["DIRAC_root_job"]))
    deep = tmp_path / "subdir" / "DIRAC_deep"
    deep.write_text("c")
    _set_old_mtime(str(deep))

    err = cleanDirectory(str(tmp_path), maxSecs=60, filePatterns=["DIRAC_*"], maxDepth=1)
    assert err == []
    assert not files["DIRAC_root_job"].exists()
    assert (tmp_path / "subdir" / "DIRAC_deep").exists()


def test_clean_directory_recursive_without_maxDepth(tmp_path):
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    deep = nested / "DIRAC_nested.log"
    deep.write_text("data")
    _set_old_mtime(str(deep))

    err = cleanDirectory(str(tmp_path), maxSecs=60, filePatterns=["*.log"])
    assert err == []
    assert not deep.exists()


def test_clean_directory_symlinks_not_deleted(tmp_path):
    target = tmp_path / "real"
    target.write_text("data")
    symlink = tmp_path / "DIRAC_link"
    symlink.symlink_to(target)
    _set_old_mtime(str(symlink))

    err = cleanDirectory(str(tmp_path), maxSecs=60, filePatterns=["DIRAC_*"])
    assert err == []
    assert symlink.exists()
    assert target.exists()


def test_clean_directory_multiple_patterns(tmp_path):
    files = _build_tree(
        tmp_path,
        {"a.out": "x", "b.err": "y", "c.txt": "z"},
    )
    for f in files.values():
        _set_old_mtime(str(f))

    err = cleanDirectory(str(tmp_path), maxSecs=60, filePatterns=["*.out", "*.err"])
    assert err == []
    assert not files["a.out"].exists()
    assert not files["b.err"].exists()
    assert files["c.txt"].exists()


def test_clean_directory_returns_errors_on_unable_delete(mocker, tmp_path):
    """When unlink raises OSError, errors are collected."""
    old_file = tmp_path / "DIRAC_blocked"
    old_file.write_text("data")
    _set_old_mtime(str(old_file))

    mocker.patch("pathlib.Path.unlink", side_effect=PermissionError("Denied"))

    err = cleanDirectory(str(tmp_path), maxSecs=60, filePatterns=["DIRAC_*"])
    assert len(err) == 1
    assert "DIRAC_blocked" in err[0]


def test_clean_directory_empty_dirs_removed(tmp_path):
    """With delEmptyDirs=True, empty directories are removed after file cleanup."""
    subdir = tmp_path / "subdir" / "nested"
    subdir.mkdir(parents=True)
    log_file = subdir / "old.log"
    log_file.write_text("data")
    _set_old_mtime(str(log_file))

    err = cleanDirectory(str(tmp_path), maxSecs=60, filePatterns=["*.log"], delEmptyDirs=True)
    assert err == []
    assert not subdir.exists()


def test_clean_directory_empty_dirs_kept_by_default(tmp_path):
    """Without delEmptyDirs, empty directories remain after file cleanup."""
    subdir = tmp_path / "subdir"
    subdir.mkdir(parents=True)
    log_file = subdir / "old.log"
    log_file.write_text("data")
    _set_old_mtime(str(log_file))

    err = cleanDirectory(str(tmp_path), maxSecs=60, filePatterns=["*.log"])
    assert err == []
    assert subdir.exists()


def test_clean_directory_callback_fn(mocker, tmp_path):
    """When callbackFn is provided, it replaces unlink."""
    callback = mocker.Mock(side_effect=lambda p: (p.unlink(), True)[1])

    old_file = tmp_path / "DIRAC_test"
    old_file.write_text("data")
    _set_old_mtime(str(old_file))

    err = cleanDirectory(str(tmp_path), maxSecs=60, filePatterns=["DIRAC_*"], callbackFn=callback)
    assert err == []
    assert not old_file.exists()
    callback.assert_called_once()


def test_clean_directory_callback_false_recorded_as_error(tmp_path):
    """Callback returning False records the file as an error; file is left untouched."""

    def skip_all(_path):
        return False

    old_file = tmp_path / "DIRAC_test"
    old_file.write_text("data")
    _set_old_mtime(str(old_file))

    err = cleanDirectory(str(tmp_path), maxSecs=60, filePatterns=["DIRAC_*"], callbackFn=skip_all)
    assert len(err) == 1
    assert "DIRAC_test" in err[0]
    assert old_file.exists()


def test_clean_directory_callback_oserror_recorded(mocker, tmp_path):
    """Callback raising OSError is treated as a deletion error."""
    mocker.patch("pathlib.Path.unlink", side_effect=PermissionError("Denied"))

    old_file = tmp_path / "DIRAC_test"
    old_file.write_text("data")
    _set_old_mtime(str(old_file))

    err = cleanDirectory(
        str(tmp_path),
        maxSecs=60,
        filePatterns=["DIRAC_*"],
        callbackFn=lambda p: (p.unlink(), True)[1],
    )
    assert len(err) == 1
