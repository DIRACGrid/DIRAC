#!/bin/env python
"""
tests for HTCondorCEComputingElement module
"""
import uuid
import pytest

from DIRAC.Resources.Computing import HTCondorCEComputingElement as HTCE
from DIRAC.Resources.Computing.BatchSystems import Condor
from DIRAC import S_OK

MODNAME = "DIRAC.Resources.Computing.HTCondorCEComputingElement"

STATUS_LINES = """
123.2 5 4 0 undefined
123.1 3 undefined undefined undefined
""".strip().split(
    "\n"
)

HISTORY_LINES = """
123.0 4 undefined undefined undefined
""".strip().split(
    "\n"
)


@pytest.fixture
def setUp():
    return {"Queue": "espresso", "GridEnv": "/dev/null"}


def test_parseCondorStatus():
    statusLines = f"""
  104098.1 1 undefined undefined undefined
  104098.2 2 undefined undefined undefined
  104098.3 3 undefined undefined undefined
  104098.4 4 undefined undefined undefined
  104098.5 5 16 57 Input data are being spooled
  104098.6 5 3 {Condor.HOLD_REASON_SUBCODE} Policy
  104098.7 5 1 0 undefined

  foo bar
  104096.1 3 16 test test
  104096.2 3 test
  104096.3 5 undefined undefined undefined
  104096.4 7
  """.strip().split(
        "\n"
    )
    # force there to be an empty line

    expectedResults = {
        "104098.1": "Waiting",
        "104098.2": "Running",
        "104098.3": "Aborted",
        "104098.4": "Done",
        "104098.5": "Waiting",
        "104098.6": "Failed",
        "104098.7": "Aborted",
        "foo": "Unknown",
        "104096.1": "Aborted",
        "104096.2": "Aborted",
        "104096.3": "Aborted",
        "104096.4": "Unknown",
    }

    for jobID, expected in expectedResults.items():
        assert HTCE.parseCondorStatus(statusLines, jobID)[0] == expected


def test_getJobStatus(mocker):
    """Test HTCondorCE getJobStatus"""
    mocker.patch(
        MODNAME + ".executeGridCommand",
        side_effect=[
            S_OK((0, "\n".join(STATUS_LINES), "")),
            S_OK((0, "\n".join(HISTORY_LINES), "")),
            S_OK((0, "", "")),
            S_OK((0, "", "")),
        ],
    )
    mocker.patch(MODNAME + ".HTCondorCEComputingElement._HTCondorCEComputingElement__cleanup")
    mocker.patch(MODNAME + ".HTCondorCEComputingElement._prepareProxy", return_value=S_OK())

    htce = HTCE.HTCondorCEComputingElement(12345)
    # Need to initialize proxy because it is required by executeCondorCommand()
    htce.proxy = "dumb_proxy"

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
        "123.2": "Aborted",
        "333.3": "Unknown",
    }

    assert ret["Status"] == 0
    assert expectedResults == ret["Jobs"]


@pytest.mark.parametrize(
    "localSchedd, optionsNotExpected, optionsExpected",
    [
        (False, ["grid_resources = "], ["universe = vanilla"]),
        (True, [], ["universe = grid"]),
    ],
)
def test__writeSub(mocker, localSchedd, optionsNotExpected, optionsExpected):
    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.useLocalSchedd = localSchedd
    subFileMock = mocker.Mock()

    mocker.patch(MODNAME + ".os.fdopen", return_value=subFileMock)
    mocker.patch(MODNAME + ".tempfile.mkstemp", return_value=("os", "pilotName"))
    mocker.patch(MODNAME + ".mkDir")

    jobStamps = []
    commonJobStampPart = uuid.uuid4().hex[:3]
    for _i in range(42):
        jobStamp = commonJobStampPart + uuid.uuid4().hex[:29]
        jobStamps.append(jobStamp)

    htce._HTCondorCEComputingElement__writeSub("dirac-install", "", 1, jobStamps)  # pylint: disable=E1101
    for option in optionsNotExpected:
        # the three [0] are: call_args_list[firstCall][ArgsArgumentsTuple][FirstArgsArgument]
        assert option not in subFileMock.write.call_args_list[0][0][0]
    for option in optionsExpected:
        assert option in subFileMock.write.call_args_list[0][0][0]


@pytest.mark.parametrize(
    "localSchedd, expected", [(False, "-pool condorce.cern.ch:9619 -name condorce.cern.ch "), (True, "")]
)
def test_reset(setUp, localSchedd, expected):
    ceParameters = setUp

    htce = HTCE.HTCondorCEComputingElement(12345)
    htce.ceParameters = ceParameters
    htce.useLocalSchedd = localSchedd
    ceName = "condorce.cern.ch"
    htce.ceName = ceName
    htce._reset()
    assert htce.remoteScheddOptions == expected


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
    htce.proxy = "dumb_proxy"
    ceName = "condorce.cern.ch"
    htce.ceName = ceName

    execMock = mocker.patch(MODNAME + ".executeGridCommand", return_value=S_OK((0, "123.0 - 123.0", "")))
    mocker.patch(MODNAME + ".HTCondorCEComputingElement._prepareProxy", return_value=S_OK())
    mocker.patch(
        MODNAME + ".HTCondorCEComputingElement._HTCondorCEComputingElement__writeSub", return_value="dirac_pilot"
    )
    mocker.patch(MODNAME + ".os")

    result = htce.submitJob("pilot", "proxy", 1)

    assert result["OK"] is True
    assert " ".join(execMock.call_args_list[0][0][0]) == expected


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
    htce.proxy = "dumb_proxy"
    htce.useLocalSchedd = local
    htce.ceParameters = ceParameters
    htce._reset()

    execMock = mocker.patch(MODNAME + ".executeGridCommand", return_value=S_OK((ret, "", "")))
    mocker.patch(MODNAME + ".HTCondorCEComputingElement._prepareProxy", return_value=S_OK())

    ret = htce.killJob(jobIDList=jobIDList)
    assert ret["OK"] == success
    if jobID:
        expected = f"condor_rm {htce.remoteScheddOptions.strip()} {jobID}"
        assert " ".join(execMock.call_args_list[0][0][0]) == expected
