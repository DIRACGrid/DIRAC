from itertools import zip_longest

import pytest
from unittest.mock import MagicMock

from DIRAC.ConfigurationSystem.Client.Helpers.Resources import (
    getDIRACPlatform,
    getCompatiblePlatforms,
    _platformSortKey,
)


mockGCReply = MagicMock()


@pytest.mark.parametrize(
    "mockGCReplyInput, requested, expectedRes, expectedValue",
    [
        ({"OK": False, "Message": "error"}, "plat", False, None),
        ({"OK": True, "Value": ""}, "plat", False, None),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "plat",
            False,
            None,
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "OS1",
            True,
            ["plat1", "plat3"],
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "OS2",
            True,
            ["plat1"],
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "OS3",
            True,
            ["plat1"],
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "OS4",
            True,
            ["plat2", "plat3"],
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "OS5",
            True,
            ["plat2"],
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "plat1",
            True,
            ["plat1"],
        ),
    ],
)
def test_getDIRACPlatform(mocker, mockGCReplyInput, requested, expectedRes, expectedValue):

    mockGCReply.return_value = mockGCReplyInput

    mocker.patch("DIRAC.Interfaces.API.Dirac.gConfig.getOptionsDict", side_effect=mockGCReply)

    res = getDIRACPlatform(requested)
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


@pytest.mark.parametrize(
    "mockGCReplyInput, requested, expectedRes, expectedValue",
    [
        ({"OK": False, "Message": "error"}, "plat", False, None),
        ({"OK": True, "Value": ""}, "plat", False, None),
        (
            {
                "OK": True,
                "Value": {"plat1": "xOS1, xOS2,  xOS3", "plat2": "sys2, xOS4, xOS5", "plat3": "sys1, xOS1, xOS4"},
            },
            "plat",
            True,
            ["plat"],
        ),
        (
            {
                "OK": True,
                "Value": {"plat1": "xOS1, xOS2,  xOS3", "plat2": "sys2, xOS4, xOS5", "plat3": "sys1, xOS1, xOS4"},
            },
            "plat1",
            True,
            ["plat1", "xOS1", "xOS2", "xOS3"],
        ),
    ],
)
def test_getCompatiblePlatforms(mocker, mockGCReplyInput, requested, expectedRes, expectedValue):
    mockGCReply.return_value = mockGCReplyInput

    mocker.patch("DIRAC.Interfaces.API.Dirac.gConfig.getOptionsDict", side_effect=mockGCReply)

    res = getCompatiblePlatforms(requested)
    assert res["OK"] is expectedRes, res
    if expectedRes:
        assert set(res["Value"]) == set(expectedValue), res["Value"]
