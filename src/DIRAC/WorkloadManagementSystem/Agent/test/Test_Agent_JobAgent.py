""" Test class for Job Agent
"""

# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
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

  assert result['OK']
  assert result['Value']['NumberOfProcessors'] == '16'
  assert result['Value']['Tags'] == ['16Processors', 'MultiProcessor']


@pytest.mark.parametrize("mockJMInput, expected", [({'OK': True}, {'OK': True, 'Value': 'Job Rescheduled'}), ({
                         'OK': False, 'Message': "Test"}, {'OK': True, 'Value': 'Problem Rescheduling Job'})])
def test__rescheduleFailedJob(mocker, mockJMInput, expected):
  """ Testing JobAgent()._rescheduleFailedJob()
  """

  mockJM.return_value = mockJMInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient.JobStateUpdateClient.setJobStatusBulk",
      side_effect=mockJM,
  )
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Client.JobManagerClient.JobManagerClient.rescheduleJob",
      side_effect=mockJM,
  )

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


def test__getCPUWorkLeft(mocker):
  """ Testing JobAgent()._getCPUWorkLeft()
  """

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule", side_effect=mockAM)

  jobAgent = JobAgent('Test', 'Test1')
  jobAgent.log = gLogger
  jobAgent.log.setLevel('DEBUG')

  result = jobAgent._getCPUWorkLeft(0)

  assert 0 == result


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

  result = jobAgent._checkInstallSoftware(101, {}, {}, MagicMock())

  assert result['OK']
  assert result['Value'] == 'Job has no software installation requirement'


@pytest.mark.parametrize("mockJWInput, expected", [(
    {'OK': False, 'Message': 'Test'}, {'OK': False, 'Message': 'Test'})])
def test_submitJob(mocker, mockJWInput, expected):
  """ Testing JobAgent()._submitJob()
  """

  mockJW.return_value = mockJWInput

  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__")
  mocker.patch(
      "DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule._AgentModule__moduleProperties",
      side_effect=lambda x, y=None: y, create=True
  )
  mocker.patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.createJobWrapper", side_effect=mockJW)

  jobAgent = JobAgent('Test', 'Test1')
  jobAgent.log = gLogger
  jobAgent.log.setLevel('DEBUG')
  jobAgent.ceName = 'Test'

  result = jobAgent._submitJob(101, {}, {}, {}, '', 1)

  assert result['OK'] == expected['OK']

  if not result['OK']:
    assert result['Message'] == expected['Message']
