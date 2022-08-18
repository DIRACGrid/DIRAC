#!/bin/env python
"""
tests for HTCondorCEComputingElement module
"""
import pytest

from DIRAC.Resources.Computing import HTCondorCEComputingElement as HTCE
from DIRAC.Resources.Computing.BatchSystems import Condor
from DIRAC import S_OK

MODNAME = "DIRAC.Resources.Computing.HTCondorCEComputingElement"

STATUS_LINES = """
123.2 5
123.1 3
""".strip().split(
    "\n"
)

HISTORY_LINES = """
123 0 4
""".strip().split(
    "\n"
)


@pytest.fixture
def setUp():
    return {"Queue": "espresso", "GridEnv": "/dev/null"}


def test_parseCondorStatus():
    statusLines = """
  104097.9 2
  104098.0 1
  104098.1 4

  foo bar
  104098.2 3
  104098.3 5
  104098.4 7
  """.strip().split(
        "\n"
    )
    # force there to be an empty line

    expectedResults = {
        "104097.9": "Running",
        "104098.0": "Waiting",
        "104098.1": "Done",
        "104098.2": "Aborted",
        "104098.3": "HELD",
        "104098.4": "Unknown",
    }

    for jobID, expected in expectedResults.items():
        assert HTCE.parseCondorStatus(statusLines, jobID) == expected


def test_getJobStatus(mocker):
    """Test HTCondorCE getJobStatus"""
    mocker.patch(MODNAME + ".commands.getstatusoutput", side_effect=([(0, "\n".join(STATUS_LINES)), (0, 0)]))
    patchPopen = mocker.patch("DIRAC.Resources.Computing.BatchSystems.Condor.subprocess.Popen")
    patchPopen.return_value.communicate.side_effect = [("\n".join(HISTORY_LINES), "")]
    patchPopen.return_value.returncode = 0
    mocker.patch(MODNAME + ".HTCondorCEComputingElement._HTCondorCEComputingElement__cleanup")

    htce = HTCE.HTCondorCEComputingElement(12345)
    ret = htce.getJobStatus(
        [
            "htcondorce://condorce.foo.arg/123.0:::abc321",
            "htcondorce://condorce.foo.arg/123.1:::c3b2a1",
            "htcondorce://condorce.foo.arg/123.2:::c3b2a2",
            "htcondorce://condorce.foo.arg/333.3:::c3b2a3",
        ]
    )

    expectedResults = {
        "htcondorce://condorce.foo.arg/123.0": "Done",
        "htcondorce://condorce.foo.arg/123.1": "Aborted",
        "htcondorce://condorce.foo.arg/123.2": "Aborted",
        "htcondorce://condorce.foo.arg/333.3": "Unknown",
    }

    assert ret["OK"] is True
    assert expectedResults == ret["Value"]


def test_getJobStatusBatchSystem(mocker):
    """Test Condor Batch System plugin getJobStatus"""
    patchPopen = mocker.patch("DIRAC.Resources.Computing.BatchSystems.Condor.subprocess.Popen")
    patchPopen.return_value.communicate.side_effect = [("\n".join(STATUS_LINES), ""), ("\n".join(HISTORY_LINES), "")]
    patchPopen.return_value.returncode = 0

    ret = Condor.Condor().getJobStatus(JobIDList=["123.0", "123.1", "123.2", "333.3"])

    expectedResults = {
        "123.0": "Done",
        "123.1": "Aborted",
        "123.2": "Unknown",  # HELD is treated as Unknown
        "333.3": "Unknown",
    }

    assert ret["Status"] == 0
    assert expectedResults == ret["Jobs"]


@pytest.mark.parametrize(
    "localSchedd, optionsNotExpected, optionsExpected",
    [
        (False, ["ShouldTransferFiles = YES", "WhenToTransferOutput = ON_EXIT_OR_EVICT"], ["universe = vanilla"]),
        (True, [], ["ShouldTransferFiles = YES", "WhenToTransferOutput = ON_EXIT_OR_EVICT", "universe = grid"]),
    ],
)
def test__writeSub(mocker, localSchedd, optionsNotExpected, optionsExpected):
    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.useLocalSchedd = localSchedd
    subFileMock = mocker.Mock()

    mocker.patch(MODNAME + ".os.fdopen", return_value=subFileMock)
    mocker.patch(MODNAME + ".tempfile.mkstemp", return_value=("os", "pilotName"))
    mocker.patch(MODNAME + ".mkDir")

    htce._HTCondorCEComputingElement__writeSub("dirac-install", 42, "", 1)  # pylint: disable=E1101
    for option in optionsNotExpected:
        # the three [0] are: call_args_list[firstCall][ArgsArgumentsTuple][FirstArgsArgument]
        assert option not in subFileMock.write.call_args_list[0][0][0]
    for option in optionsExpected:
        assert option in subFileMock.write.call_args_list[0][0][0]


@pytest.mark.parametrize(
    "localSchedd, expected", [(False, "-pool condorce.cern.ch:9619 -name condorce.cern.ch"), (True, "")]
)
def test_reset(setUp, localSchedd, expected):
    ceParameters = setUp

    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.ceParameters = ceParameters
    htce.useLocalSchedd = True
    ceName = "condorce.cern.ch"
    htce.ceName = ceName
    htce._reset()
    assert htce.remoteScheddOptions == ""


@pytest.mark.parametrize(
    "localSchedd, expected",
    [
        (False, "condor_submit -terse -pool condorce.cern.ch:9619 -remote condorce.cern.ch dirac_pilot"),
        (True, "condor_submit -terse dirac_pilot"),
    ],
)
def test_submitJob(setUp, mocker, localSchedd, expected):
    ceParameters = setUp
    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.ceParameters = ceParameters
    htce.useLocalSchedd = localSchedd
    ceName = "condorce.cern.ch"
    htce.ceName = ceName

    execMock = mocker.patch(MODNAME + ".executeGridCommand", return_value=S_OK((0, "123.0 - 123.0")))
    mocker.patch(
        MODNAME + ".HTCondorCEComputingElement._HTCondorCEComputingElement__writeSub", return_value="dirac_pilot"
    )
    mocker.patch(MODNAME + ".os")

    result = htce.submitJob("pilot", "proxy", 1)

    assert result["OK"] is True
    assert " ".join(execMock.call_args_list[0][0][1]) == expected


@pytest.mark.parametrize(
    "jobIDList, jobID, ret, success, local",
    [
        ([], "", 0, True, True),
        ("", "", 0, True, True),
        (["htcondorce://condorce.foo.arg/123.0:::abc321"], "123.0", 0, True, True),
        ("htcondorce://condorce.foo.arg/123.0:::abc321", "123.0", 0, True, True),
        ("htcondorce://condorce.foo.arg/123.0:::abc321", "123.0", 1, False, True),
        (["htcondorce://condorce.foo.arg/333.3"], "333.3", 0, True, True),
        ("htcondorce://condorce.foo.arg/333.3", "333.3", 0, True, False),
    ],
)
def test_killJob(setUp, mocker, jobIDList, jobID, ret, success, local):
    ceParameters = setUp
    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.ceName = "condorce.foo.arg"
    htce.useLocalSchedd = local
    htce.ceParameters = ceParameters
    htce._reset()

    execMock = mocker.patch(MODNAME + ".executeGridCommand", return_value=S_OK((ret, "", "")))

    ret = htce.killJob(jobIDList=jobIDList)
    assert ret["OK"] == success
    if jobID:
        expected = f"condor_rm {htce.remoteScheddOptions.strip()} {jobID}"
        assert " ".join(execMock.call_args_list[0][0][1]) == expected
