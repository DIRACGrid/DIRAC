import pytest

from DIRAC import S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.Executor.CheckingToWaitingTransitioner import resolveJobSiteName, resolveSites

# Arrange
@pytest.mark.parametrize(
    "userSites, userBannedSites, onlineSites, expected",
    [
        ([], [], [], S_OK([])),
        ([], ["foo"], [], S_OK([])),
        (["foo"], [], [], S_OK(["foo"])),
        ([], [], ["foo"], S_OK(["foo"])),
        (["foo", "bar"], [], ["foo"], S_OK(["foo"])),
        (["foo", "bar"], [], ["foo", "bar"], S_OK(["foo", "bar"])),
        (["foo", "bar"], [], [], S_OK(["foo", "bar"])),
        (["foo", "bar"], ["bar"], ["foo", "bar"], S_OK(["foo"])),
        (["foo"], ["foo"], [], S_ERROR()),
        ([], ["foo"], ["foo"], S_ERROR()),
        (["foo"], ["foo"], ["foo"], S_ERROR()),
        (["foo"], [], ["bar"], S_ERROR()),
    ],
)
def test_resoleSites(userSites: list[str], userBannedSites: list[str], onlineSites: list[str], expected):

    # Act
    result = resolveSites(set(userSites), set(userBannedSites), set(onlineSites))

    # Assert
    assert result["OK"] is expected["OK"]
    if result["OK"]:
        assert set(result["Value"]) == set(expected["Value"])


# Arrange
@pytest.mark.parametrize(
    "sites, onlineSites, expected",
    [
        ([], [], "ANY"),
        (["foo"], [], "foo"),
        (["foo"], ["foo"], "foo"),
        (["foo", "bar"], [], "Multiple"),
        (["foo", "bar"], ["foo", "bar"], "MultipleInput"),
    ],
)
def test_resolveJobSiteName(sites: list[str], onlineSites: list[str], expected: str):
    # Act
    siteName = resolveJobSiteName(sites, onlineSites)

    # Assert
    assert siteName is expected
