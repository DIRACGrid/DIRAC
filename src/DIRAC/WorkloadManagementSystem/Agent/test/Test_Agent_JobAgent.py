""" Test class for Job Agent
"""

import pytest

from DIRAC.WorkloadManagementSystem.Agent.JobAgent import JobAgent
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import TimeLeft
from DIRAC import gLogger, S_ERROR

gLogger.setLevel("DEBUG")


@pytest.mark.parametrize(
    "ceType, expectedType, expectedNumberElement",
    [
        ("InProcess", list, 1),
        ("Pool", list, 2),
    ],
)
def test__getCEDict(mocker, ceType, expectedType, expectedNumberElement):
    """Test JobAgent()._getCEDict()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = ComputingElementFactory().getCE(ceType)
    assert result["OK"]

    ce = result["Value"]
    ce.ceParameters["MultiProcessorStrategy"] = True
    ce.ceParameters["NumberOfProcessors"] = 4
    result = jobAgent._getCEDict(ce)
    assert result["OK"]
    ceDict = result["Value"]
    assert isinstance(ceDict, expectedType)
    assert len(ceDict) == expectedNumberElement


@pytest.mark.parametrize(
    "ceDict, mockGCReply, mockGCReply2, expected",
    [
        (
            {},
            "Test",
            {"OK": False},
            {
                "GridCE": "Test",
                "PilotBenchmark": 0.0,
                "PilotInfoReportedFlag": False,
                "PilotReference": "Unknown",
            },
        ),
        (
            {},
            None,
            {"OK": False},
            {
                "PilotBenchmark": 0.0,
                "PilotInfoReportedFlag": False,
                "PilotReference": "Unknown",
            },
        ),
        (
            {"PilotReference": "ref"},
            "Test",
            {"OK": False},
            {
                "GridCE": "Test",
                "PilotBenchmark": 0.0,
                "PilotInfoReportedFlag": False,
                "PilotReference": "ref",
            },
        ),
        (
            {"PilotReference": "ref"},
            None,
            {"OK": True, "Value": {"JobReq": "test"}},
            {
                "PilotBenchmark": 0.0,
                "PilotInfoReportedFlag": False,
                "PilotReference": "ref",
                "JobReq": "test",
            },
        ),
    ],
)
def test__setCEDict(mocker, ceDict, mockGCReply, mockGCReply2, expected):
    """Test JobAgent()._setCEDict()"""
    if mockGCReply:
        mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.gConfig.getValue", return_value=mockGCReply)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.gConfig.getOptionsDict", return_value=mockGCReply2)
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent._setCEDict(ceDict)
    assert ceDict == expected


@pytest.mark.parametrize(
    "ceType, mockCEReply, expectedResult",
    [
        ("InProcess", {"OK": False, "Message": "CE Not Available"}, {"OK": False, "Message": "CE Not Available"}),
        ("InProcess", {"OK": True, "Value": None, "CEInfoDict": {}}, {"OK": False, "Message": "CE Not Available"}),
        (
            "InProcess",
            {"OK": True, "Value": None, "CEInfoDict": {"RunningJobs": 1}},
            {"OK": True, "Value": "Job Agent cycle complete with 1 running jobs"},
        ),
        ("InProcess", {"OK": True, "Value": 1, "CEInfoDict": {}}, {"OK": True}),
    ],
)
def test__checkCEAvailability(mocker, ceType, mockCEReply, expectedResult):
    """Test JobAgent()._checkAvailability()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.Resources.Computing.ComputingElement.ComputingElement.available", return_value=mockCEReply)

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = ComputingElementFactory().getCE(ceType)
    assert result["OK"]

    ce = result["Value"]
    result = jobAgent._checkCEAvailability(ce)
    assert result["OK"] == expectedResult["OK"]
    if "Value" in expectedResult:
        assert result["Value"] == expectedResult["Value"]
    if "Message" in expectedResult:
        assert result["Message"] == expectedResult["Message"]


#############################################################################


@pytest.mark.parametrize(
    "initTimeLeft, timeLeft, cpuFactor, mockTimeLeftReply, expectedTimeLeft",
    [
        (100000, 75000, None, {"OK": False, "Message": "Error"}, 75000),
        (100000, 75000, 10, {"OK": False, "Message": "Error"}, 100000),
        (100000, 75000, 10, {"OK": True, "Value": 25000}, 25000),
    ],
)
def test__computeCPUWorkLeft(mocker, initTimeLeft, timeLeft, cpuFactor, mockTimeLeftReply, expectedTimeLeft):
    """Test JobAgent()._computeCPUWorkLeft()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft.TimeLeft.getTimeLeft", return_value=mockTimeLeftReply
    )

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")
    jobAgent.timeLeftUtil = TimeLeft()

    jobAgent.initTimeLeft = initTimeLeft
    jobAgent.timeLeft = timeLeft
    jobAgent.cpuFactor = cpuFactor
    result = jobAgent._computeCPUWorkLeft()

    assert abs(result - expectedTimeLeft) < 10


@pytest.mark.parametrize(
    "cpuWorkLeft, fillingMode, expectedResult",
    [
        (10000, False, {"OK": False, "Message": "Filling Mode is Disabled"}),
        (10000, True, {"OK": True, "Value": None}),
        (1000, False, {"OK": False, "Message": "Filling Mode is Disabled"}),
        (1000, True, {"OK": False, "Message": "No more time left"}),
    ],
)
def test__checkCPUWorkLeft(mocker, cpuWorkLeft, fillingMode, expectedResult):
    """Test JobAgent()._checkCPUWorkLeft()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.am_stopExecution")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent.fillingMode = fillingMode

    result = jobAgent._checkCPUWorkLeft(cpuWorkLeft)
    assert result["OK"] == expectedResult["OK"]
    if "Value" in result:
        assert result["Value"] == expectedResult["Value"]
    if "Message" in result:
        assert result["Message"] == expectedResult["Message"]


#############################################################################


@pytest.mark.parametrize(
    "issueMessage, stopAfterFailedMatches, matchFailedCount, expectedResult",
    [("Pilot version does not match", 5, 0, False), ("No match found", 5, 0, True), ("No match found", 5, 5, False)],
)
def test__checkMatchingIssues(mocker, issueMessage, stopAfterFailedMatches, matchFailedCount, expectedResult):
    """Test JobAgent()._checkMatchingIssues()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.am_stopExecution")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jobAgent.stopAfterFailedMatches = stopAfterFailedMatches
    jobAgent.matchFailedCount = matchFailedCount

    result = jobAgent._checkMatchingIssues(S_ERROR(issueMessage))
    assert result["OK"] == expectedResult


@pytest.mark.parametrize(
    "matcherInfo, matcherParams, expectedResult",
    [
        ({}, [], {"OK": True, "Value": None}),
        ({}, ["Param1"], {"OK": False, "Message": "Matcher Failed"}),
        ({"Param1": None}, ["Param1"], {"OK": False, "Message": "Matcher Failed"}),
        ({"Param1": "Value1"}, ["Param1"], {"OK": True, "Value": None}),
    ],
)
def test__checkMatcherInfo(mocker, matcherInfo, matcherParams, expectedResult):
    """Test JobAgent()._checkMatcherInfo()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setJobStatus")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = jobAgent._checkMatcherInfo(matcherInfo, matcherParams, JobReport(123))
    assert result["OK"] == expectedResult["OK"]
    if "Value" in result:
        assert result["Value"] == expectedResult["Value"]
    if "Message" in result:
        assert result["Message"] == expectedResult["Message"]


#############################################################################


@pytest.mark.parametrize(
    "mockGCReply, mockPMReply, expected",
    [
        (True, {"OK": True, "Value": "Test"}, {"OK": True, "Value": "Test"}),
        (
            True,
            {"OK": False, "Message": "Test"},
            {"OK": False, "Message": "Failed to setup proxy: Error retrieving proxy"},
        ),
        (
            False,
            {"OK": True, "Value": "Test"},
            {"OK": False, "Message": "Invalid Proxy"},
        ),
        (
            False,
            {"OK": False, "Message": "Test"},
            {"OK": False, "Message": "Invalid Proxy"},
        ),
    ],
)
def test__setupProxy(mocker, mockGCReply, mockPMReply, expected):
    """Testing JobAgent()._setupProxy()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.gConfig.getValue", return_value=mockGCReply)
    module_str = "DIRAC.WorkloadManagementSystem.Agent.JobAgent.gProxyManager.getPayloadProxyFromDIRACGroup"
    mocker.patch(module_str, return_value=mockPMReply)

    jobAgent = JobAgent("Test", "Test1")

    ownerDN = "DIRAC"
    ownerGroup = "DIRAC"

    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = jobAgent._setupProxy(ownerDN, ownerGroup)

    assert result["OK"] == expected["OK"]

    if result["OK"]:
        assert result["Value"] == expected["Value"]
    else:
        assert result["Message"] == expected["Message"]


@pytest.mark.parametrize(
    "mockGCReply, mockPMReply, expected",
    [
        (True, {"OK": True, "Value": "Test"}, {"OK": True, "Value": "Test"}),
        (
            True,
            {"OK": False, "Message": "Test"},
            {"OK": False, "Message": "Error retrieving proxy"},
        ),
        (False, {"OK": True, "Value": "Test"}, {"OK": True, "Value": "Test"}),
        (
            False,
            {"OK": False, "Message": "Test"},
            {"OK": False, "Message": "Error retrieving proxy"},
        ),
    ],
)
def test__requestProxyFromProxyManager(mocker, mockGCReply, mockPMReply, expected):
    """Testing JobAgent()._requestProxyFromProxyManager()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.gConfig.getValue", return_value=mockGCReply)
    module_str = "DIRAC.WorkloadManagementSystem.Agent.JobAgent.gProxyManager.getPayloadProxyFromDIRACGroup"
    mocker.patch(module_str, return_value=mockPMReply)

    jobAgent = JobAgent("Test", "Test1")

    ownerDN = "DIRAC"
    ownerGroup = "DIRAC"

    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = jobAgent._requestProxyFromProxyManager(ownerDN, ownerGroup)

    assert result["OK"] == expected["OK"]

    if result["OK"]:
        assert result["Value"] == expected["Value"]

    else:
        assert result["Message"] == expected["Message"]


#############################################################################


def test__checkInstallSoftware(mocker):
    """Testing JobAgent()._checkInstallSoftware()"""

    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = jobAgent._checkInstallSoftware(101, {}, {}, JobReport(123))

    assert result["OK"], result["Message"]
    assert result["Value"] == "Job has no software installation requirement"


#############################################################################


def test__getJDLParameters(mocker):
    """Testing JobAgent()._getJDLParameters()"""

    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    jdl = """
        [
            Executable = "dirac-jobexec";
            StdError = "std.err";
            LogLevel = "info";
            Site = "ANY";
            JobName = "helloWorld";
            Priority = "1";
            InputSandbox =
                {
                    "../../Integration/WorkloadManagementSystem/exe-script.py",
                    "exe-script.py",
                    "/tmp/tmpMQEink/jobDescription.xml",
                    "SB:FedericoSandboxSE|/SandBox/f/fstagni.lhcb_user/0c2/9f5/0c29f53a47d051742346b744c793d4d0.tar.bz2"
                };
            Arguments = "jobDescription.xml -o LogLevel=info";
            JobGroup = "lhcb";
            OutputSandbox =
                {
                    "helloWorld.log",
                    "std.err",
                    "std.out"
                };
            StdOutput = "std.out";
            InputData = "";
            JobType = "User";
            NumberOfProcessors = 16;
            Tags =
                {
                    "16Processors",
                    "MultiProcessor"
                };
        ]
        """

    result = jobAgent._getJDLParameters(jdl)

    assert result["OK"], result["Message"]
    assert result["Value"]["NumberOfProcessors"] == "16"
    assert result["Value"]["Tags"] == ["16Processors", "MultiProcessor"]


@pytest.mark.parametrize(
    "mockJMInput, expected",
    [
        ({"OK": True}, {"OK": True, "Value": "Problem Rescheduling Job"}),
        (
            {"OK": False, "Message": "Test"},
            {"OK": True, "Value": "Problem Rescheduling Job"},
        ),
    ],
)
def test__rescheduleFailedJob(mocker, mockJMInput, expected):
    """Testing JobAgent()._rescheduleFailedJob()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    jobAgent = JobAgent("Test", "Test1")

    jobID = 101
    message = "Test"

    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")

    result = jobAgent._rescheduleFailedJob(jobID, message)
    result = jobAgent._finish(result["Message"], False)

    assert result == expected


@pytest.mark.parametrize(
    "mockJWInput, expected", [({"OK": False, "Message": "Test"}, {"OK": False, "Message": "Test"})]
)
def test_submitJob(mocker, mockJWInput, expected):
    """Testing JobAgent()._submitJob()"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule._AgentModule__moduleProperties",
        side_effect=lambda x, y=None: y,
        create=True,
    )
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.createJobWrapper", return_value=mockJWInput)

    jobAgent = JobAgent("Test", "Test1")
    jobAgent.log = gLogger
    jobAgent.log.setLevel("DEBUG")
    jobAgent.ceName = "Test"

    result = jobAgent._submitJob(101, {}, {}, {}, "", 1)

    assert result["OK"] == expected["OK"]

    if not result["OK"]:
        assert result["Message"] == expected["Message"]
