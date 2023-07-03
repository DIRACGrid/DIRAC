from diraccfg import CFG
import pytest

from DIRAC import gConfig
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Resources.Computing.ComputingElement import ComputingElement


pilotCfg1 = """
LocalSite
{
  Tag = Token
  CPUTime = 500
  Test
  {
    MaxTotalJobs = 20
    Tag = WholeNode
  }
}
Resources
{
  Computing
  {
    CEDefaults
    {
      CPUTime = 50
      Tag = Token
      Tag += /cvmfs/dirac/
    }
    ComputingElement
    {
        CPUTime = 5000
        Tag = Test
        Tag += Token
    }
  }
}
"""


pilotCfg2 = """
LocalSite
{
  Tag =
}
"""


def setupConfig(config):
    """Set up the configuration file

    :param str config: configuration content to load
    """
    gConfigurationData.localCFG = CFG()
    cfg = CFG()
    cfg.loadFromBuffer(config)
    gConfig.loadCFG(cfg)


@pytest.mark.parametrize(
    "config, expectedValue",
    [
        (
            pilotCfg1,
            {
                "Tag": {"/cvmfs/dirac/", "Token", "WholeNode"},
                "CPUTime": 500,
                "MaxTotalJobs": 20,
                "WaitingToRunningRatio": 0.5,
                "MaxWaitingJobs": 1,
            },
        ),
        (pilotCfg2, {"MaxTotalJobs": 1, "WaitingToRunningRatio": 0.5, "MaxWaitingJobs": 1}),
    ],
)
def test_initializeParameters(config, expectedValue):
    """Test the initialization of the CE parameters"""
    setupConfig(config)

    ce = ComputingElement("Test")
    ce.initializeParameters()
    assert ce.ceParameters == expectedValue
