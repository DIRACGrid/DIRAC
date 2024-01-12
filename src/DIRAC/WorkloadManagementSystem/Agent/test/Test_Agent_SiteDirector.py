""" Test class for SiteDirector
"""
# pylint: disable=protected-access

import datetime
import os
import pytest
from diraccfg import CFG

from DIRAC import gLogger, gConfig
from DIRAC.ConfigurationSystem.Client import ConfigurationData
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus

from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.Utilities.SubmissionPolicy import SUBMISSION_POLICIES


CONFIG = """
Resources
{
  Sites
  {
    LCG
    {
      LCG.Site1.com
      {
        VO = dteam
        CEs
        {
          ce1.site1.com
          {
            architecture = x86_64
            OS = linux_AlmaLinux_9
            CEType = HTCondorCE
            LocalCEType = Singularity
            MaxRAM = 6974
            Queues
            {
              condor
              {
                MaxTotalJobs = 1000
                MaxWaitingJobs = 100
                maxCPUTime = 1152
                VO = dteam
                NumberOfProcessors = 1
              }
            }
            Tag = Token
          }
          ce2.site1.com
          {
            architecture = x86_64
            OS = linux_AlmaLinux_9
            CEType = HTCondorCE
            LocalCEType = Singularity
            MaxRAM = 6974
            Queues
            {
              condor
              {
                MaxTotalJobs = 1000
                MaxWaitingJobs = 100
                maxCPUTime = 1152
                VO = dteam
                NumberOfProcessors = 1
              }
            }
          }
        }
      }
      LCG.Site2.site2
      {
        CEs
        {
          ce1.site2.com
          {
            architecture = x86_64
            OS = linux_AlmaLinux_9
            CEType = HTCondorCE
            LocalCEType = Singularity
            MaxRAM = 6974
            Queues
            {
              condor
              {
                MaxTotalJobs = 1000
                MaxWaitingJobs = 100
                maxCPUTime = 1152
                VO = dteam
                NumberOfProcessors = 1
              }
            }
            Tag = Token
          }
        }
      }
    }
    DIRAC
    {
      DIRAC.Site3.site3
      {
        CEs
        {
          ce1.site3.com
          {
            architecture = x86_64
            OS = linux_AlmaLinux_9
            CEType = HTCondorCE
            LocalCEType = Singularity
            MaxRAM = 6974
            Queues
            {
              condor
              {
                MaxTotalJobs = 1000
                MaxWaitingJobs = 100
                maxCPUTime = 1152
                VO = dteam
                NumberOfProcessors = 1
              }
            }
            Tag = Token
          }
        }
      }
    }
  }
}
"""


@pytest.fixture
def config():
    """Load a fake configuration"""
    ConfigurationData.localCFG = CFG()
    cfg = CFG()
    cfg.loadFromBuffer(CONFIG)
    gConfig.loadCFG(cfg)


@pytest.fixture
def sd(mocker, config):
    """Basic configuration of a SiteDirector. It tests the _buildQueueDict() method at the same time"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule.__init__")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.Operations.getValue", return_value="123")
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.getPilotAgentsDB")

    usableSites = (
        gConfig.getSections("Resources/Sites/LCG")["Value"] + gConfig.getSections("Resources/Sites/DIRAC")["Value"]
    )
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.SiteDirector.SiteStatus.getUsableSites", return_values=usableSites
    )
    sd = SiteDirector()

    # Set logger
    sd.log = gLogger
    sd.log.setLevel("DEBUG")

    # Set basic parameters
    sd.workingDirectory = ""

    # Set VO
    sd.vo = "dteam"

    # Set queueDict
    sd.siteClient = SiteStatus()
    sd._buildQueueDict()
    return sd


@pytest.fixture(scope="session")
def pilotWrapperDirectory(tmp_path_factory):
    """Create a temporary directory"""
    fn = tmp_path_factory.mktemp("pilotWrappers")
    return fn


def test_loadSubmissionPolicy(sd):
    """Load each submission policy and call it"""
    for submissionPolicyName in SUBMISSION_POLICIES:
        # Load the submission policy
        sd.submissionPolicyName = submissionPolicyName
        res = sd._loadSubmissionPolicy()
        assert res["OK"]

        # Call the submission policy with predefined parameters
        targetQueue = "ce1.site1.com_condor"
        res = sd.submissionPolicy.apply(50, ceParameters=sd.queueDict[targetQueue]["CE"].ceParameters)
        assert res >= 0 and res <= 50


def test_getPilotWrapper(mocker, sd, pilotWrapperDirectory):
    """Get pilot options for a specific queue and check the result, then generate the pilot wrapper"""
    mocker.patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gConfig.getValue", return_value="TestSetup")

    # Get pilot options
    pilotOptions = sd._getPilotOptions("ce1.site1.com_condor")
    assert {
        "--preinstalledEnv=123",
        "--pythonVersion=3",
        "--wnVO=dteam",
        "-n LCG.Site1.com",
        "-N ce1.site1.com",
        "-Q condor",
        "-S TestSetup",
        "-V 123",
        "-l 123",
        "-e 1,2,3",
    } == set(pilotOptions)

    # Write pilot script
    res = sd._writePilotScript(pilotWrapperDirectory, pilotOptions)

    # Make sure the file exists
    assert os.path.exists(res) and os.path.isfile(res)


def test_updatePilotStatus(sd):
    """Updating the status of some fake pilot references"""
    # 1. We have not submitted any pilots, there is nothing to update
    pilotDict = {}
    pilotCEDict = {}
    res = sd._getUpdatedPilotStatus(pilotDict, pilotCEDict)
    assert not res

    res = sd._getAbortedPilots(res)
    assert not res

    # 2. We just submitted a pilot, the remote system has not had the time to register the pilot
    pilotDict["pilotRef1"] = {"Status": PilotStatus.SUBMITTED, "LastUpdateTime": datetime.datetime.utcnow()}
    pilotCEDict = {}
    res = sd._getUpdatedPilotStatus(pilotDict, pilotCEDict)
    assert not res

    res = sd._getAbortedPilots(res)
    assert not res

    # 3. The pilot is now registered
    pilotCEDict["pilotRef1"] = PilotStatus.SUBMITTED
    res = sd._getUpdatedPilotStatus(pilotDict, pilotCEDict)
    assert not res

    res = sd._getAbortedPilots(res)
    assert not res

    # 4. The pilot waits in the queue of the remote CE
    pilotCEDict["pilotRef1"] = PilotStatus.WAITING
    res = sd._getUpdatedPilotStatus(pilotDict, pilotCEDict)
    assert res == {"pilotRef1": PilotStatus.WAITING}

    res = sd._getAbortedPilots(res)
    assert not res
    pilotDict["pilotRef1"]["Status"] = PilotStatus.WAITING

    # 5. CE issue: the pilot status becomes unknown
    pilotCEDict["pilotRef1"] = PilotStatus.UNKNOWN
    res = sd._getUpdatedPilotStatus(pilotDict, pilotCEDict)
    assert res == {"pilotRef1": PilotStatus.UNKNOWN}

    res = sd._getAbortedPilots(res)
    assert not res
    pilotDict["pilotRef1"]["Status"] = PilotStatus.UNKNOWN

    # 6. Engineers do not manage to fix the issue, the CE is still under maintenance
    pilotDict["pilotRef1"]["LastUpdateTime"] = datetime.datetime.utcnow() - datetime.timedelta(seconds=3610)
    res = sd._getUpdatedPilotStatus(pilotDict, pilotCEDict)
    assert res == {"pilotRef1": PilotStatus.ABORTED}

    res = sd._getAbortedPilots(res)
    assert res == ["pilotRef1"]
