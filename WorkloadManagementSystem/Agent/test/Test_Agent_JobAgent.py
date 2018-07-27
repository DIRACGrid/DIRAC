""" Test class for Job Agent
"""

# imports
from mock import MagicMock, patch

# DIRAC Components
from DIRAC.WorkloadManagementSystem.Agent.JobAgent import JobAgent
from DIRAC import gLogger

gLogger.setLevel('DEBUG')

# Mock Objects
mockAM = MagicMock()
mockJM = MagicMock()
mockJM.rescheduleJob.return_value = {'OK': True}
mockGCReply = MagicMock()
mockGCReply.return_value = True
mockPMReply = MagicMock()
mockPMReply.return_value = {'OK': True, 'Value': 'Test'}


class TestJobAgent(object):
  """ Testing the single methods of JobAgent
  """

  @patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__", new=mockAM)
  def test__getJDLParameters(self, _patch1):
    """ Testing JobAgent()._getJDLParameters()
    """

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
        ]
        """

    result = jobAgent._getJDLParameters(jdl)

    assert result['OK']
    assert result['Value']['Origin'] == 'DIRAC'

  @patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.JobManagerClient.executeRPC", side_effect=mockJM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__", new=mockAM)
  def test__rescheduleFailedJob(self, _patch1, _patch2):
    """ Testing JobAgent()._rescheduleFailedJob()
    """

    jobAgent = JobAgent('Test', 'Test1')

    jobID = 101
    message = 'Test'

    jobAgent.log = gLogger
    jobAgent.log.setLevel('DEBUG')

    result = jobAgent._rescheduleFailedJob(jobID, message, stop=False)

    assert result['OK']
    assert result['Value'] == 'Job Rescheduled'

  str = "DIRAC.WorkloadManagementSystem.Agent.JobAgent.gProxyManager.getPayloadProxyFromDIRACGroup"

  @patch(str, side_effect=mockPMReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.gConfig.getValue", side_effect=mockGCReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.JobAgent.AgentModule.__init__", new=mockAM)
  def test__setupProxy(self, _patch1, _patch2, _patch3):
    """ Testing JobAgent()._setupProxy()
    """

    jobAgent = JobAgent('Test', 'Test1')

    ownerDN = 'DIRAC'
    ownerGroup = 'DIRAC'

    jobAgent.log = gLogger
    jobAgent.log.setLevel('DEBUG')

    result = jobAgent._setupProxy(ownerDN, ownerGroup)

    assert result['OK']
    assert result['Value'] == 'Test'
