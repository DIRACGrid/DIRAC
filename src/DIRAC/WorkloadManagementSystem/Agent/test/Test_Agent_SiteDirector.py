""" Test class for SiteDirector
"""
# pylint: disable=protected-access

import datetime
import os
from unittest.mock import MagicMock

import pytest
from diraccfg import CFG

from DIRAC import S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client import ConfigurationData
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector
from DIRAC.WorkloadManagementSystem.Client import PilotStatus

CONFIG = """
Registry
{
  Groups
  {
    dteam_user
    {
      VO = dteam
    }
    dummyVO_user
    {
      VO = dummyVO
    }
  }
}
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

mockPMProxy = MagicMock()
mockPMProxy.dumpAllToString.return_value = {"OK": True, "Value": "fakeProxy"}
mockPMProxyReply = MagicMock()
mockPMProxyReply.return_value = {"OK": True, "Value": mockPMProxy}


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
        "DIRAC.WorkloadManagementSystem.Agent.SiteDirector.SiteStatus.getUsableSites", return_value=S_OK(usableSites)
    )

    # Mock getElementStatus to return a properly formatted dictionary
    def mock_getElementStatus(ceNamesList, *args, **kwargs):
        return S_OK({ceName: {"all": "Active"} for ceName in ceNamesList})

    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.SiteDirector.ResourceStatus.getElementStatus",
        side_effect=mock_getElementStatus,
    )
    mocker.patch(
        "DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gProxyManager.downloadProxy", side_effect=mockPMProxyReply
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
    sd.rssClient = ResourceStatus()
    sd._buildQueueDict()
    return sd


@pytest.fixture(scope="session")
def pilotWrapperDirectory(tmp_path_factory):
    """Create a temporary directory"""
    fn = tmp_path_factory.mktemp("pilotWrappers")
    return fn


def test_getNumberOfJobsNeedingPilots(sd, mocker):
    """Make sure it returns the number of needed pilots"""

    # No waiting job, no waiting pilot
    # Because it requires an access to a DB, we mock the value returned by the Matcher
    mocker.patch.object(sd, "matcherClient", autospec=True)
    sd.matcherClient.getMatchingTaskQueues.return_value = S_OK({})
    numberToSubmit = sd._getNumberOfJobsNeedingPilots(waitingPilots=0, queue="ce1.site3.com_condor")
    assert numberToSubmit == 0

    # 10 waiting jobs, no waiting pilot, wrong VO: dummyVO != dteam
    sd.matcherClient.getMatchingTaskQueues.return_value = S_OK({"TQ1": {"Jobs": 10, "OwnerGroup": "dummyVO_user"}})
    numberToSubmit = sd._getNumberOfJobsNeedingPilots(waitingPilots=0, queue="ce1.site3.com_condor")
    assert numberToSubmit == 0

    # 10 waiting jobs, no waiting pilot, right VO
    sd.matcherClient.getMatchingTaskQueues.return_value = S_OK({"TQ1": {"Jobs": 10, "OwnerGroup": "dteam_user"}})
    numberToSubmit = sd._getNumberOfJobsNeedingPilots(waitingPilots=0, queue="ce1.site3.com_condor")
    assert numberToSubmit == 10

    # 10 waiting jobs split into 2 task queues, no waiting pilot, wrong VO: dummyVO != dteam
    sd.matcherClient.getMatchingTaskQueues.return_value = S_OK(
        {"TQ1": {"Jobs": 8, "OwnerGroup": "dteam_user"}, "TQ2": {"Jobs": 2, "OwnerGroup": "dummyVO_user"}}
    )
    numberToSubmit = sd._getNumberOfJobsNeedingPilots(waitingPilots=0, queue="ce1.site3.com_condor")
    assert numberToSubmit == 8

    # 10 waiting jobs split into 2 task queues, no waiting pilot, right VO
    sd.matcherClient.getMatchingTaskQueues.return_value = S_OK(
        {"TQ1": {"Jobs": 8, "OwnerGroup": "dteam_user"}, "TQ2": {"Jobs": 2, "OwnerGroup": "dteam_user"}}
    )
    numberToSubmit = sd._getNumberOfJobsNeedingPilots(waitingPilots=0, queue="ce1.site3.com_condor")
    assert numberToSubmit == 10

    # 10 waiting jobs, 5 waiting pilots
    sd.matcherClient.getMatchingTaskQueues.return_value = S_OK({"TQ1": {"Jobs": 10, "OwnerGroup": "dteam_user"}})
    numberToSubmit = sd._getNumberOfJobsNeedingPilots(waitingPilots=5, queue="ce1.site3.com_condor")
    assert numberToSubmit == 5

    # 10 waiting jobs split into 2 task queues, 10 waiting pilots
    sd.matcherClient.getMatchingTaskQueues.return_value = S_OK(
        {"TQ1": {"Jobs": 8, "OwnerGroup": "dteam_user"}, "TQ2": {"Jobs": 2, "OwnerGroup": "dteam_user"}}
    )
    numberToSubmit = sd._getNumberOfJobsNeedingPilots(waitingPilots=10, queue="ce1.site3.com_condor")
    assert numberToSubmit == 0

    # 10 waiting jobs, 20 waiting pilots
    sd.matcherClient.getMatchingTaskQueues.return_value = S_OK({"TQ1": {"Jobs": 10, "OwnerGroup": "dteam_user"}})
    numberToSubmit = sd._getNumberOfJobsNeedingPilots(waitingPilots=20, queue="ce1.site3.com_condor")
    assert numberToSubmit == 0


def test_getPilotWrapper(mocker, sd, pilotWrapperDirectory):
    """Get pilot options for a specific queue and check the result, then generate the pilot wrapper"""

    # Get pilot options
    pilotOptions = sd._getPilotOptions("ce1.site1.com_condor")
    assert {
        "--architectureScript=123",
        "--preinstalledEnv=123",
        "--wnVO=dteam",
        "-n LCG.Site1.com",
        "-N ce1.site1.com",
        "-Q condor",
        "-V 123",
        "-l 123",
        "-e 1,2,3",
    } == set(pilotOptions)

    proxyObject_mock = MagicMock()
    proxyObject_mock.dumpAllToString.return_value = S_OK("aProxy")

    # Write pilot script
    res = sd._writePilotScript(pilotWrapperDirectory, pilotOptions, proxyObject_mock)

    # Make sure the file exists
    assert os.path.exists(res) and os.path.isfile(res)


def test__submitPilotsToQueue(sd):
    """Testing SiteDirector()._submitPilotsToQueue()"""
    # Create a MagicMock that does not have the workingDirectory
    # attribute (https://cpython-test-docs.readthedocs.io/en/latest/library/unittest.mock.html#deleting-attributes)
    # This is to use the SiteDirector's working directory, not the CE one
    ceMock = MagicMock()
    del ceMock.workingDirectory
    proxyObject_mock = MagicMock()
    proxyObject_mock.dumpAllToString.return_value = S_OK("aProxy")
    ceMock.proxy = proxyObject_mock

    sd.queueCECache = {"ce1.site1.com_condor": {"CE": ceMock, "Hash": "3d0dd0c60fffa900c511d7442e9c7634"}}
    sd.queueSlots = {"ce1.site1.com_condor": {"AvailableSlots": 10}}
    sd._buildQueueDict()
    sd.sendSubmissionAccounting = False
    sd.sendSubmissionMonitoring = False
    assert sd._submitPilotsToQueue(1, ceMock, "ce1.site1.com_condor")["OK"]


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
