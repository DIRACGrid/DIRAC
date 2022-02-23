""" Unit tests for PathFinder only for functions that I added
"""
import pytest
from diraccfg import CFG

from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.ConfigurationSystem.Client.Helpers import Operations
from DIRAC.ConfigurationSystem.private.ConfigurationData import ConfigurationData

localCFGData = ConfigurationData(False)
mergedCFG = CFG()
mergedCFG.loadFromBuffer(
    """
DIRAC
{
  Setup=TestSetup
  Setups
  {
    TestSetup
    {
      WorkloadManagement=MyWM
    }
  }
}
Systems
{
  WorkloadManagement
  {
    MyWM
    {
      URLs
      {
        Service1 = dips://server1:1234/WorkloadManagement/Service1
        Service2 = dips://$MAINSERVERS$:5678/WorkloadManagement/Service2
      }
      FailoverURLs
      {
        Service2 = dips://failover1:5678/WorkloadManagement/Service2
        Service2 += dips://failover2:5678/WorkloadManagement/Service2
      }
    }
  }
}
Operations{
  Defaults
  {
    MainServers = gw1, gw2
  }
}
"""
)
localCFGData.localCFG = mergedCFG
localCFGData.remoteCFG = mergedCFG
localCFGData.mergedCFG = mergedCFG
localCFGData.generateNewVersion()


@pytest.fixture
def pathFinder(monkeypatch):
    monkeypatch.setattr(PathFinder, "gConfigurationData", localCFGData)
    monkeypatch.setattr(Operations, "gConfigurationData", localCFGData)
    return PathFinder


def test_getDIRACSetup(pathFinder):
    """Test getDIRACSetup"""
    assert pathFinder.getDIRACSetup() == "TestSetup"


@pytest.mark.parametrize(
    "system, componentName, setup, componentType, result",
    [
        (
            "WorkloadManagement/SandboxStoreHandler",
            False,
            False,
            "Services",
            "/Systems/WorkloadManagement/MyWM/Services/SandboxStoreHandler",
        ),
        (
            "WorkloadManagement",
            "SandboxStoreHandler",
            False,
            "Services",
            "/Systems/WorkloadManagement/MyWM/Services/SandboxStoreHandler",
        ),
        # tricky case one could expect that if entity string is wrong
        # than some kind of error will be returned, but it is not the case
        (
            "WorkloadManagement/SimpleLogConsumer",
            False,
            False,
            "NonRonsumersNon",
            "/Systems/WorkloadManagement/MyWM/NonRonsumersNon/SimpleLogConsumer",
        ),
    ],
)
def test_getComponentSection(pathFinder, system, componentName, setup, componentType, result):
    """Test getComponentSection"""
    assert pathFinder.getComponentSection(system, componentName, setup, componentType) == result


@pytest.mark.parametrize(
    "system, setup, result",
    [
        ("WorkloadManagement", False, "/Systems/WorkloadManagement/MyWM/URLs"),
        ("WorkloadManagement", "TestSetup", "/Systems/WorkloadManagement/MyWM/URLs"),
    ],
)
def test_getSystemURLSection(pathFinder, system, setup, result):
    assert pathFinder.getSystemURLs(system, setup)


@pytest.mark.parametrize(
    "serviceName, service, result",
    [
        (
            "WorkloadManagement/Service1",
            None,
            {"dips://server1:1234/WorkloadManagement/Service1"},
        ),
        (
            "WorkloadManagement",
            "Service1",
            {"dips://server1:1234/WorkloadManagement/Service1"},
        ),
        (
            "WorkloadManagement",
            "Service2",
            {
                "dips://gw1:5678/WorkloadManagement/Service2",
                "dips://gw2:5678/WorkloadManagement/Service2",
            },
        ),
    ],
)
def test_getServiceURL(pathFinder, serviceName, service, result):
    assert set(List.fromChar(pathFinder.getServiceURL(serviceName, service=service))) == result


@pytest.mark.parametrize(
    "serviceName, service, result",
    [
        ("WorkloadManagement/Service1", None, ""),
        ("WorkloadManagement", "Service1", ""),
        (
            "WorkloadManagement",
            "Service2",
            "dips://failover1:5678/WorkloadManagement/Service2,dips://failover2:5678/WorkloadManagement/Service2",
        ),
    ],
)
def test_getServiceFailoverURL(pathFinder, serviceName, service, result):
    """Test getServiceFailoverURL"""
    assert pathFinder.getServiceFailoverURL(serviceName, service=service) == result


@pytest.mark.parametrize(
    "serviceName, service, failover, result",
    [
        (
            "WorkloadManagement/Service1",
            None,
            False,
            {"dips://server1:1234/WorkloadManagement/Service1"},
        ),
        (
            "WorkloadManagement",
            "Service1",
            False,
            {"dips://server1:1234/WorkloadManagement/Service1"},
        ),
        (
            "WorkloadManagement",
            "Service2",
            False,
            {
                "dips://gw1:5678/WorkloadManagement/Service2",
                "dips://gw2:5678/WorkloadManagement/Service2",
            },
        ),
        (
            "WorkloadManagement",
            "Service1",
            True,
            {"dips://server1:1234/WorkloadManagement/Service1"},
        ),
        (
            "WorkloadManagement",
            "Service2",
            True,
            {
                "dips://gw1:5678/WorkloadManagement/Service2",
                "dips://gw2:5678/WorkloadManagement/Service2",
                "dips://failover1:5678/WorkloadManagement/Service2",
                "dips://failover2:5678/WorkloadManagement/Service2",
            },
        ),
    ],
)
def test_getServiceURLs(pathFinder, serviceName, service, failover, result):
    """Test getServiceURLs"""
    assert set(pathFinder.getServiceURLs(serviceName, service=service, failover=failover)) == result


@pytest.mark.parametrize(
    "system, setup, failover, result",
    [
        (
            "WorkloadManagement",
            None,
            False,
            {
                "Service1": {"dips://server1:1234/WorkloadManagement/Service1"},
                "Service2": {
                    "dips://gw1:5678/WorkloadManagement/Service2",
                    "dips://gw2:5678/WorkloadManagement/Service2",
                },
            },
        ),
        (
            "WorkloadManagement",
            None,
            True,
            {
                "Service1": {"dips://server1:1234/WorkloadManagement/Service1"},
                "Service2": {
                    "dips://gw1:5678/WorkloadManagement/Service2",
                    "dips://gw2:5678/WorkloadManagement/Service2",
                    "dips://failover1:5678/WorkloadManagement/Service2",
                    "dips://failover2:5678/WorkloadManagement/Service2",
                },
            },
        ),
    ],
)
def test_getSystemURLs(pathFinder, system, setup, failover, result):
    sysDict = pathFinder.getSystemURLs(system, setup=setup, failover=failover)
    for service in sysDict:
        assert set(sysDict[service]) == result[service]


@pytest.mark.parametrize(
    "serviceURL, system, service, result",
    [
        (
            "dips://server.com:1234/WorkloadManagement/Service1",
            None,
            None,
            "dips://server.com:1234/WorkloadManagement/Service1",
        ),
        (
            "dips://server.com:1234/",
            "WorkloadManagement",
            "Service1",
            "dips://server.com:1234/WorkloadManagement/Service1",
        ),
        (
            "dips://server.com:1234",
            "WorkloadManagement",
            "Service1",
            "dips://server.com:1234/WorkloadManagement/Service1",
        ),
        ("dips://server.com:1234/", "WorkloadManagement", None, "raise:path"),
        ("dips://server.com/WorkloadManagement/Service1", None, None, "raise:port"),
        (
            "https://server.com/WorkloadManagement/Service1",
            None,
            None,
            "https://server.com:443/WorkloadManagement/Service1",
        ),
        (
            "http://server.com/WorkloadManagement/Service1",
            None,
            None,
            "http://server.com:80/WorkloadManagement/Service1",
        ),
    ],
)
def test_checkComponentURL(pathFinder, serviceURL, system, service, result):
    try:
        pathFinderResult = pathFinder.checkComponentURL(serviceURL, system, service, pathMandatory=True)
        assert pathFinderResult == result
    except RuntimeError as e:
        assert result.split(":")[1] in repr(e)
