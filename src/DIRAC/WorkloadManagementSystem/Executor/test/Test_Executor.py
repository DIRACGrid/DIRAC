""" pytest(s) for Executors
"""
# pylint: disable=protected-access, missing-docstring

import pytest
from unittest.mock import MagicMock

from DIRAC.Core.Utilities.ClassAd import ClassAd

# sut
from DIRAC.WorkloadManagementSystem.Executor.JobScheduling import JobScheduling
from DIRAC.WorkloadManagementSystem.Executor.InputData import InputData

mockNone = MagicMock()
mockNone.return_value = None


@pytest.mark.parametrize(
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
    js = JobScheduling()
    filtered = js._applySiteFilter(sites, banned)
    assert set(filtered) == set(expected)


@pytest.mark.parametrize(
    "inputSandbox, expected",
    [
        ([], []),
        (["SB:ProductionSandboxSE|/SandBox/l/lhcb_mc/4c7.bof"], []),
        (["LFN:/l/lhcb_mc/4c7.bof"], ["/l/lhcb_mc/4c7.bof"]),
        (
            ["SB:ProductionSandboxSE|/SandBox/l/lhcb_mc/4c7.bof", "LFN:/l/lhcb_mc/4c7.bof"],
            ["/l/lhcb_mc/4c7.bof"],
        ),
        (["SB:ProductionSandboxSE|/SandBox/l/lhcb_mc/4c7.bof", "bif:/l/lhcb_mc/4c7.bof"], []),
        (
            ["LFN:/l/lhcb_mc/4c8.bof", "LFN:/l/lhcb_mc/4c7.bof"],
            ["/l/lhcb_mc/4c8.bof", "/l/lhcb_mc/4c7.bof"],
        ),
    ],
)
def test__getInputSandbox(inputSandbox, expected):

    # Arrange
    jobDescription = ClassAd()
    jobDescription.insertAttributeVectorString("InputSandbox", inputSandbox)

    # Act
    res = InputData()._getInputSandbox(jobDescription)

    # Assert
    assert res["OK"] is True
    assert res["Value"] == expected
