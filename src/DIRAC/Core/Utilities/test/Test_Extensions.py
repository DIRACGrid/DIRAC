"""Tests for the DIRAC.Core.Utilities.Extensions module"""
import DIRAC
from DIRAC.Core.Utilities.Extensions import (
    findSystems,
    findAgents,
    findExecutors,
    findServices,
    findDatabases,
    extensionsByPriority,
    getExtensionMetadata,
)


def test_findSystems():
    systems = findSystems([DIRAC])
    assert len(systems) > 5
    assert all(system.endswith("System") for system in systems)


def test_findAgents():
    agents = findAgents([DIRAC])
    assert len(agents) > 5


def test_findExecutors():
    executors = findExecutors([DIRAC])
    assert len(executors) > 1


def test_findServices():
    services = findServices([DIRAC])
    assert len(services) > 5


def test_findDatabases():
    databases = findDatabases([DIRAC])
    assert len(databases) > 5
    assert all(str(fn).endswith(".sql") for system, fn in databases)


def test_extensionsByPriority():
    assert "DIRAC" in extensionsByPriority()


def test_getExtensionMetadata():
    metadata = getExtensionMetadata("DIRAC")
    assert metadata["priority"] == 0
