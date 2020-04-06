""" Test class for Job Agent
"""

# imports
from __future__ import absolute_import
import pytest
from mock import MagicMock

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.JobAgent import JobAgent
from DIRAC import gLogger

gLogger.setLevel('DEBUG')

# Mock Objects
mockAM = MagicMock()
mockJM = MagicMock()
mockGCReply = MagicMock()
mockPMReply = MagicMock()
mockJW = MagicMock()
mockReply = MagicMock()


def test__getJDLParameters(mocker):
  """ Testing JobAgent()._getJDLParameters()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")

  jobAgent = JobAgent('Test', 'Test1')
  jobAgent.log = gLogger
  jobAgent.log.setLevel('DEBUG')

  jdl = """
        [
            Origin = "DIRAC";
            Executable = "$DIRACROOT/scripts/dirac-jobexec";
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

  assert result['OK']
  assert result['Value']['Origin'] == 'DIRAC'
  assert result['Value']['NumberOfProcessors'] == '16'
  assert result['Value']['Tags'] == ['16Processors', 'MultiProcessor']


@pytest.mark.parametrize("mockJMInput, expected", [({'OK': True}, {'OK': True, 'Value': 'Job Rescheduled'}), ({
                         'OK': False, 'Message': "Test"}, {'OK': True, 'Value': 'Problem Rescheduling Job'})])
def test__rescheduleFailedJob(mocker, mockJMInput, expected):
  """ Testing JobAgent()._rescheduleFailedJob()
  """

  mockJM.return_value = mockJMInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.JobManagerClient.executeRPC", side_effect=mockJM)

  jobAgent = JobAgent('Test', 'Test1')

  jobID = 101
  message = 'Test'

  jobAgent.log = gLogger
  jobAgent.log.setLevel('DEBUG')

  result = jobAgent._rescheduleFailedJob(jobID, message, stop=False)

  assert result == expected


@pytest.mark.parametrize(
    "mockGCReplyInput, mockPMReplyInput, expected", [
        (True, {
            'OK': True, 'Value': 'Test'}, {
            'OK': True, 'Value': 'Test'}), (True, {
                'OK': False, 'Message': 'Test'}, {
                'OK': False, 'Message': 'Failed to setup proxy: Error retrieving proxy'}), (False, {
                    'OK': True, 'Value': 'Test'}, {
                    'OK': False, 'Message': 'Invalid Proxy'}), (False, {
                        'OK': False, 'Message': 'Test'}, {
                        'OK': False, 'Message': 'Invalid Proxy'})])
def test__setupProxy(mocker, mockGCReplyInput, mockPMReplyInput, expected):
  """ Testing JobAgent()._setupProxy()
  """

  mockGCReply.return_value = mockGCReplyInput
  mockPMReply.return_value = mockPMReplyInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule", side_effect=mockAM)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.gConfig.getValue", side_effect=mockGCReply)
  module_str = "DIRAC.WorkloadManagementSystem.Agent.JobAgent.gProxyManager.getPayloadProxyFromDIRACGroup"
  mocker.patch(module_str, side_effect=mockPMReply)

  jobAgent = JobAgent('Test', 'Test1')

  ownerDN = 'DIRAC'
  ownerGroup = 'DIRAC'

  jobAgent.log = gLogger
  jobAgent.log.setLevel('DEBUG')

  result = jobAgent._setupProxy(ownerDN, ownerGroup)

  assert result['OK'] == expected['OK']

  if result['OK']:
    assert result['Value'] == expected['Value']

  else:
    assert result['Message'] == expected['Message']


@pytest.mark.parametrize("initTimeLeft, cpuFactor, cpuConsumed, addCpuConsumed, expected1, expected2", [
    (30000, 10, 5, 0, 29950, 29950),
    (30000, 10, 5, 5, 29950, 29900),
    (30000, 10, 60, 5, 29400, 29350),
    (30000, 10, 60, 77, 29400, 28630),
    (30000, 24, 5, 0, 29880, 29880),
    (30000, 24, 5, 5, 29880, 29760),
    (30000, 24, 60, 5, 28560, 28440),
    (30000, 24, 60, 77, 28560, 26712),
    (1000, 10, 99, 1, 10, 0)])
def test__getCPUTimeLeft(mocker, initTimeLeft, cpuFactor, cpuConsumed, addCpuConsumed, expected1, expected2):
  """ Testing JobAgent()._getCPUTimeLeft()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule", side_effect=mockAM)

  jobAgent = JobAgent('Test', 'Test1')
  jobAgent.log = gLogger
  jobAgent.log.setLevel('DEBUG')

  # initTimeLeft (hepspec06 seconds) is generally computed before launching the JobAgent
  jobAgent.initTimeLeft = initTimeLeft
  jobAgent.timeLeft = initTimeLeft
  # cpuFactor is the key to convert seconds in hepspec06 seconds
  jobAgent.cpuFactor = cpuFactor
  # cpuConsumed is cpu time (processor seconds), and represent time spend since the beginning of the JobAgent execution
  cpuConsumed = cpuConsumed

  # The result is in hepsec06 seconds
  result = jobAgent._getCPUTimeLeft(cpuConsumed)
  assert result['OK']
  jobAgent.timeLeft = result['Value']
  assert expected1 == jobAgent.timeLeft

  # Increase cpuConsumed to study the behavior of the JobAgent
  cpuConsumed += addCpuConsumed
  result = jobAgent._getCPUTimeLeft(cpuConsumed)
  assert result['OK']
  jobAgent.timeLeft = result['Value']
  assert expected2 == jobAgent.timeLeft


@pytest.mark.parametrize(
    "mockGCReplyInput, mockPMReplyInput, expected", [
        (True, {
            'OK': True, 'Value': 'Test'}, {
            'OK': True, 'Value': 'Test'}), (True, {
                'OK': False, 'Message': 'Test'}, {
                'OK': False, 'Message': 'Error retrieving proxy'}), (False, {
                    'OK': True, 'Value': 'Test'}, {
                    'OK': True, 'Value': 'Test'}), (False, {
                        'OK': False, 'Message': 'Test'}, {
                        'OK': False, 'Message': 'Error retrieving proxy'})])
def test__requestProxyFromProxyManager(mocker, mockGCReplyInput, mockPMReplyInput, expected):
  """ Testing JobAgent()._requestProxyFromProxyManager()
  """

  mockGCReply.return_value = mockGCReplyInput
  mockPMReply.return_value = mockPMReplyInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule", side_effect=mockAM)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.gConfig.getValue", side_effect=mockGCReply)
  module_str = "DIRAC.WorkloadManagementSystem.Agent.JobAgent.gProxyManager.getPayloadProxyFromDIRACGroup"
  mocker.patch(module_str, side_effect=mockPMReply)

  jobAgent = JobAgent('Test', 'Test1')

  ownerDN = 'DIRAC'
  ownerGroup = 'DIRAC'

  jobAgent.log = gLogger
  jobAgent.log.setLevel('DEBUG')

  result = jobAgent._requestProxyFromProxyManager(ownerDN, ownerGroup)

  assert result['OK'] == expected['OK']

  if result['OK']:
    assert result['Value'] == expected['Value']

  else:
    assert result['Message'] == expected['Message']


def test__checkInstallSoftware(mocker):
  """ Testing JobAgent()._checkInstallSoftware()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")

  jobAgent = JobAgent('Test', 'Test1')
  jobAgent.log = gLogger
  jobAgent.log.setLevel('DEBUG')

  result = jobAgent._checkInstallSoftware(101, {}, {})

  assert result['OK']
  assert result['Value'] == 'Job has no software installation requirement'


@pytest.mark.parametrize("mockJWInput, expected", [(
    {'OK': False, 'Message': 'Test'}, {'OK': False, 'Message': 'Test'})])
def test_submitJob(mocker, mockJWInput, expected):
  """ Testing JobAgent()._submitJob()
  """

  mockJW.return_value = mockJWInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.am_getOption", side_effect=mockAM)
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.createJobWrapper", side_effect=mockJW)

  jobAgent = JobAgent('Test', 'Test1')
  jobAgent.log = gLogger
  jobAgent.log.setLevel('DEBUG')
  jobAgent.ceName = 'Test'

  result = jobAgent._submitJob(101, {}, {}, {}, {}, {})

  assert result['OK'] == expected['OK']

  if not result['OK']:
    assert result['Message'] == expected['Message']
