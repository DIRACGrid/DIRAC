from itertools import zip_longest

import pytest

from DIRACCommon.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform, _platformSortKey


MOCK_OS_COMPATIBILITY_DICT = {
    "plat1": {"plat1", "OS1", "OS2", "OS3"},
    "plat2": {"plat2", "OS4", "OS5"},
    "plat3": {"plat3", "OS1", "OS4"},
}


@pytest.mark.parametrize(
    "osCompatibilityDict, osList, expectedRes, expectedValue",
    [
        ({}, ["plat"], False, None),
        (MOCK_OS_COMPATIBILITY_DICT, ["plat"], False, None),
        (MOCK_OS_COMPATIBILITY_DICT, ["OS1"], True, ["plat1", "plat3"]),
        (MOCK_OS_COMPATIBILITY_DICT, ["OS2"], True, ["plat1"]),
        (MOCK_OS_COMPATIBILITY_DICT, ["OS3"], True, ["plat1"]),
        (MOCK_OS_COMPATIBILITY_DICT, ["OS4"], True, ["plat2", "plat3"]),
        (MOCK_OS_COMPATIBILITY_DICT, ["OS5"], True, ["plat2"]),
        (MOCK_OS_COMPATIBILITY_DICT, ["plat1"], True, ["plat1"]),
    ],
)
def test_getDIRACPlatform(osCompatibilityDict, osList, expectedRes, expectedValue):
    res = getDIRACPlatform(osList, osCompatibilityDict)
    assert res["OK"] is expectedRes, res
    if expectedRes:
        assert set(res["Value"]) == set(expectedValue), res["Value"]


@pytest.mark.parametrize(
    "string,expected",
    [
        ("Darwin_arm64_12.4", ["darwin", "_", "arm", "64", "_", "12", "4"]),
        ("Linux_x86_64_glibc-2.17", ["linux", "_", "x", "86", "_", "64", "_", "glibc", "-", "2", "17"]),
        ("Linux_aarch64_glibc-2.28", ["linux", "_", "aarch", "64", "_", "glibc", "-", "2", "28"]),
    ],
)
def test_platformSortKey(string, expected):
    actual = _platformSortKey(string)
    for a, e in zip_longest(actual, expected):
        # Numbers are padded with zeros so string comparison works
        assert a.lstrip("0") == e
