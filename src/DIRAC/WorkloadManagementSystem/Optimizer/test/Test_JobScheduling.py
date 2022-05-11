""" pytest(s) for Optimiz
"""
# pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import pytest
from mock import MagicMock

from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
from DIRAC.WorkloadManagementSystem.Optimizer.JobScheduling import JobScheduling

mockNone = MagicMock()
mockNone.return_value = None


@pytest.mark.parametrize(
    # Arrange
    "sites, banned, expected",
    [
        (["MY.Site1.org", "MY.Site2.org"], None, ["MY.Site1.org", "MY.Site2.org"]),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site1.org", "MY.Site2.org"], []),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site2.org"], ["MY.Site1.org"]),
        (["MY.Site1.org", "MY.Site2.org"], [], ["MY.Site1.org", "MY.Site2.org"]),
        ([], ["MY.Site1.org"], []),
        ([], [], []),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site1.org"], ["MY.Site2.org"]),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site1.org", "MY.Site3.org"], ["MY.Site2.org"]),
        ([], ["MY.Site1.org", "MY.Site3.org"], []),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site4.org"], ["MY.Site1.org", "MY.Site2.org"]),
        (
            ["MY.Site1.org", "MY.Site2.org", "MY.Site3.org"],
            ["MY.Site4.org"],
            ["MY.Site1.org", "MY.Site2.org", "MY.Site3.org"],
        ),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site4.org"], ["MY.Site1.org", "MY.Site2.org"]),
    ],
)
def test__applySiteFilter(sites, banned, expected):
    # Act
    filtered = JobScheduling(MagicMock())._applySiteFilter(sites, banned)

    # Assert
    assert set(filtered) == set(expected)


@pytest.mark.parametrize(
    # Arrange
    "manifestOptions, expected",
    [
        ({}, []),
        ({"Tag": "bof"}, ["bof"]),
        ({"Tags": "bof, bif"}, ["bof", "bif"]),
        ({"MaxRAM": 2}, ["2GB"]),
        ({"Tags": "bof, bif", "MaxRAM": 2}, ["bof", "bif", "2GB"]),
        ({"WholeNode": "yes", "MaxRAM": 2}, ["WholeNode", "MultiProcessor", "2GB"]),
        ({"NumberOfProcessors": 1}, []),
        ({"NumberOfProcessors": 4}, ["MultiProcessor", "4Processors"]),
        ({"NumberOfProcessors": 4, "MinNumberOfProcessors": 2}, ["MultiProcessor", "4Processors"]),
        ({"NumberOfProcessors": 4, "MaxNumberOfProcessors": 12}, ["MultiProcessor", "4Processors"]),
        ({"NumberOfProcessors": 4, "MaxNumberOfProcessors": 12}, ["MultiProcessor", "4Processors"]),
        ({"MinNumberOfProcessors": 4, "MaxNumberOfProcessors": 12}, ["MultiProcessor", "4Processors"]),
        ({"MinNumberOfProcessors": 4, "MaxNumberOfProcessors": 4}, ["MultiProcessor", "4Processors"]),
        ({"MinNumberOfProcessors": 4}, ["MultiProcessor", "4Processors"]),
    ],
)
def test__getTagsFromManifest(manifestOptions, expected):
    manifest = JobManifest()
    for varName, varValue in manifestOptions.items():
        manifest.setOption(varName, varValue)

    # Act
    tagList = JobScheduling(MagicMock())._getTagsFromManifest(manifest)

    # Assert
    assert set(tagList) == set(expected)
