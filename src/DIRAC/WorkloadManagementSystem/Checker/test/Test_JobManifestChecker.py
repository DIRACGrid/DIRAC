# pylint: disable=missing-docstring, invalid-name

import pytest
from DIRAC.WorkloadManagementSystem.Checker.JobManifestChecker import getTagsFromManifest
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest


@pytest.mark.parametrize(
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

    tagList = getTagsFromManifest(manifest)
    assert tagList == set(expected)
