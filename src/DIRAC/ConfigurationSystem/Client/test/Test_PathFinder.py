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
    r"""
DIRAC
{
  PreferredURLPatterns = dips://.*\.site:.*
  PreferredURLPatterns += dips://.*\.other:.*
}
Systems
{
  Configuration
  {
    URLs
    {
      Server = dips://server1.site:1234/Configuration/Server
      Server += dips://server2.site:1234/Configuration/Server
      Server += dips://server3.site:1234/Configuration/Server
      Server += dips://server4.site:1234/Configuration/Server
      Server += dips://server.other:1234/Configuration/Server
      Server += dips://server.external:1234/Configuration/Server
    }
  }
  WorkloadManagement
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


@pytest.mark.parametrize(
    "system, componentName, componentType, result",
    [
        (
            "WorkloadManagement/SandboxStoreHandler",
            False,
            "Services",
            "/Systems/WorkloadManagement/Services/SandboxStoreHandler",
        ),
        (
            "WorkloadManagement",
            "SandboxStoreHandler",
            "Services",
            "/Systems/WorkloadManagement/Services/SandboxStoreHandler",
        ),
        # tricky case one could expect that if entity string is wrong
        # than some kind of error will be returned, but it is not the case
        (
            "WorkloadManagement/SimpleLogConsumer",
            False,
            "NonRonsumersNon",
            "/Systems/WorkloadManagement/NonRonsumersNon/SimpleLogConsumer",
        ),
    ],
)
def test_getComponentSection(pathFinder, system, componentName, componentType, result):
    """Test getComponentSection"""
    assert pathFinder.getComponentSection(system, componentName, componentType) == result


@pytest.mark.parametrize(
    "system, result",
    [
        ("WorkloadManagement", "/Systems/WorkloadManagement/MyWM/URLs"),
    ],
)
def test_getSystemURLSection(pathFinder, system, result):
    assert pathFinder.getSystemURLs(system)


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


def test_getServiceURLsOrdering(pathFinder):
    """Ensure the PreferredURLPattern option is respected"""
    all_results = set()
    for _ in range(10_000):
        urls = pathFinder.getServiceURLs("Configuration", service="Server")
        assert set(urls) == {
            "dips://server1.site:1234/Configuration/Server",
            "dips://server2.site:1234/Configuration/Server",
            "dips://server3.site:1234/Configuration/Server",
            "dips://server4.site:1234/Configuration/Server",
            "dips://server.other:1234/Configuration/Server",
            "dips://server.external:1234/Configuration/Server",
        }
        # The second to last URL should always be "other"
        assert urls[-2] == "dips://server.other:1234/Configuration/Server"
        # The last URL should always be the one which isn't preferred
        assert urls[-1] == "dips://server.external:1234/Configuration/Server"
        all_results.add(tuple(urls))
    # There are 4! = 24 possible orderings of the preferred URLs, we should have seen all
    # of them at least once in 10_000 iterations
    assert len(all_results) >= 24


@pytest.mark.parametrize(
    "system, failover, result",
    [
        (
            "WorkloadManagement",
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
def test_getSystemURLs(pathFinder, system, failover, result):
    sysDict = pathFinder.getSystemURLs(system, failover=failover)
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
