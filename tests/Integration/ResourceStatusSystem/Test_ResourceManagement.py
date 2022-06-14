""" This is a test of the chain
    ResourceManagementClient -> ResourceManagementHandler -> ResourceManagementDB
    It supposes that the DB is present, and that the service is running

    The DB is supposed to be empty when the test starts
"""
# pylint: disable=wrong-import-position, missing-docstring

import datetime

import pytest

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

gLogger.setLevel("DEBUG")

dateEffective = datetime.datetime.now()
lastCheckTime = datetime.datetime.now()


@pytest.fixture(name="rmClient")
def fixtureResourceManagementClient():
    yield ResourceManagementClient()


def test_AccountingCache(rmClient):
    """
    DowntimeCache table
    """

    res = rmClient.deleteAccountingCache("TestName12345")  # just making sure it's not there (yet)
    assert res["OK"] is True, res["Message"]

    # TEST addOrModifyAccountingCache
    res = rmClient.addOrModifyAccountingCache(
        "TestName12345", "plotType", "plotName", "result", datetime.datetime.now(), datetime.datetime.now()
    )
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectAccountingCache("TestName12345")
    assert res["OK"] is True, res["Message"]
    # check if the name that we got is equal to the previously added 'TestName12345'
    assert res["Value"][0][0] == "TestName12345"

    res = rmClient.addOrModifyAccountingCache(
        "TestName12345", "plotType", "plotName", "changedresult", dateEffective, lastCheckTime
    )
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectAccountingCache("TestName12345")
    # check if the result has changed
    rmClient.assertEqual(res["Value"][0][4], "changedresult")

    # TEST deleteAccountingCache
    # ...............................................................................
    res = rmClient.deleteAccountingCache("TestName12345")
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectAccountingCache("TestName12345")
    assert res["OK"] is True, res["Message"]
    assert not res["Value"], res["Value"]


def test_DowntimeCache(rmClient):
    """
    DowntimeCache table
    """

    res = rmClient.deleteDowntimeCache("TestName12345")  # just making sure it's not there (yet)
    assert res["OK"] is True, res["Message"]

    # TEST addOrModifyDowntimeCache
    res = rmClient.addOrModifyDowntimeCache(
        "TestName12345",
        "element",
        "name",
        datetime.datetime.now(),
        datetime.datetime.now(),
        "severity",
        "description",
        "link",
        datetime.datetime.now(),
        datetime.datetime.now(),
        "gOCDBServiceType",
    )
    assert res["OK"] is True, res["Message"]

    # check if the name that we got is equal to the previously added 'TestName12345'
    res = rmClient.selectDowntimeCache("TestName12345")
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "TestName12345"

    res = rmClient.addOrModifyDowntimeCache("TestName12345", "element", "name", severity="changedSeverity")
    assert res["OK"] is True, res["Message"]

    # check if the result has changed
    res = rmClient.selectDowntimeCache("TestName12345")
    assert res["Value"][0][4] == "changedSeverity"

    # TEST deleteDowntimeCache
    # ...............................................................................
    res = rmClient.deleteDowntimeCache("TestName12345")
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectDowntimeCache("TestName12345")
    assert res["OK"] is True, res["Message"]
    assert not res["Value"], res["Value"]


def test_GGUSTicketsCache(rmClient):
    """
    GGUSTicketsCache table
    """

    res = rmClient.deleteGGUSTicketsCache("TestName12345")  # just making sure it's not there (yet)
    assert res["OK"] is True, res["Message"]

    # TEST addOrModifyGGUSTicketsCache
    res = rmClient.addOrModifyGGUSTicketsCache("TestName12345", "link", 0, "tickets", datetime.datetime.now())
    assert res["OK"] is True, res["Message"]

    # check if the name that we got is equal to the previously added 'TestName12345'
    res = rmClient.selectGGUSTicketsCache("TestName12345")
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "TestName12345"

    res = rmClient.addOrModifyGGUSTicketsCache("TestName12345", "newLink")
    assert res["OK"] is True, res["Message"]

    # check if the result has changed
    res = rmClient.selectGGUSTicketsCache("TestName12345")
    assert res["Value"][0][3] == "newLink"

    # TEST deleteGGUSTicketsCache
    # ...............................................................................
    res = rmClient.deleteGGUSTicketsCache("TestName12345")
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectGGUSTicketsCache("TestName12345")
    assert res["OK"] is True, res["Message"]
    assert not res["Value"], res["Value"]


def test_JobCache(rmClient):
    """
    JobCache table
    """

    res = rmClient.deleteJobCache("TestName12345")  # just making sure it's not there (yet)
    assert res["OK"] is True, res["Message"]

    # TEST addOrModifyJobCache
    res = rmClient.addOrModifyJobCache("TestName12345", "maskstatus", 50.89, "status", datetime.datetime.now())
    assert res["OK"] is True, res["Message"]

    # check if the name that we got is equal to the previously added 'TestName12345'
    res = rmClient.selectJobCache("TestName12345")
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "TestName12345"

    res = rmClient.addOrModifyJobCache("TestName12345", status="newStatus")
    assert res["OK"] is True, res["Message"]

    # check if the result has changed
    res = rmClient.selectJobCache("TestName12345")
    assert res["Value"][0][1] == "newStatus"

    # TEST deleteJobCache
    # ...............................................................................
    res = rmClient.deleteJobCache("TestName12345")
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectJobCache("TestName12345")
    assert res["OK"] is True, res["Message"]
    assert not res["Value"], res["Value"]


def test_PilotCache(rmClient):
    """
    PilotCache table
    """

    res = rmClient.deletePilotCache("TestName12345")  # just making sure it's not there (yet)
    assert res["OK"] is True, res["Message"]

    # TEST addOrModifyPilotCache
    res = rmClient.addOrModifyPilotCache("TestName12345", "CE", 0.0, 25.5, "status", datetime.datetime.now())
    assert res["OK"] is True, res["Message"]

    # check if the name that we got is equal to the previously added 'TestName12345'
    res = rmClient.selectPilotCache("TestName12345")
    assert res["OK"] is True, res["Message"]
    assert res["Value"][0][0] == "TestName12345"
    assert res["Value"][0][6] == "all"  # default value for vo, as the last element

    res = rmClient.addOrModifyPilotCache("TestName12345", status="newStatus")
    assert res["OK"] is True, res["Message"]

    # check if the result has changed.
    res = rmClient.selectPilotCache("TestName12345")
    assert res["Value"][0][2] == "newStatus"

    # TEST deletePilotCache
    # ...............................................................................
    res = rmClient.deletePilotCache("TestName12345")
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectPilotCache("TestName12345")
    assert res["OK"] is True, res["Message"]
    assert not res["Value"], res["Value"]


def test_PolicyResult(rmClient):
    """
    PolicyResult table
    """

    res = rmClient.deletePolicyResult(
        "element", "TestName12345", "policyName", "statusType"
    )  # just making sure it's not there (yet)
    assert res["OK"] is True, res["Message"]

    # TEST addOrModifyPolicyResult
    res = rmClient.addOrModifyPolicyResult(
        "element",
        "TestName12345",
        "policyName",
        "statusType",
        "status",
        "reason",
        datetime.datetime.now(),
        datetime.datetime.now(),
    )
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectPolicyResult("element", "TestName12345", "policyName", "statusType")
    assert res["OK"] is True, res["Message"]
    # check if the name that we got is equal to the previously added 'TestName12345'
    assert res["Value"][0][1] == "statusType"
    assert res["Value"][0][8] == "all"  # default value for vo, as the last element

    res = rmClient.addOrModifyPolicyResult("element", "TestName12345", "policyName", "statusType", status="newStatus")
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectPolicyResult("element", "TestName12345", "policyName", "statusType")
    # check if the result has changed.
    assert res["Value"][0][4] == "newStatus"

    # TEST deletePolicyResult
    # ...............................................................................
    res = rmClient.deletePolicyResult("element", "TestName12345", "policyName", "statusType")
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectPolicyResult("element", "TestName12345", "policyName", "statusType")
    assert res["OK"] is True, res["Message"]
    assert not res["Value"], res["Value"]


def test_SpaceTokenOccupancy(rmClient):
    """
    SpaceTokenOccupancy table
    """

    res = rmClient.deleteSpaceTokenOccupancyCache("endpoint", "token")  # just making sure it's not there (yet)
    assert res["OK"] is True, res["Message"]

    # TEST addOrModifySpaceTokenOccupancy
    res = rmClient.addOrModifySpaceTokenOccupancyCache(
        "endpoint", "token", 500.0, 1000.0, 200.0, datetime.datetime.now()
    )
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectSpaceTokenOccupancyCache("endpoint", "token")
    assert res["OK"] is True, res["Message"]
    # check if the name that we got is equal to the previously added 'token'
    assert res["Value"][0][1] == "token"

    res = rmClient.addOrModifySpaceTokenOccupancyCache("endpoint", "token", free=100.0)
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectSpaceTokenOccupancyCache("endpoint", "token")
    # check if the result has changed
    assert res["Value"][0][3] == 100.0

    # TEST deleteSpaceTokenOccupancy
    # ...............................................................................
    res = rmClient.deleteSpaceTokenOccupancyCache("endpoint", "token")
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectSpaceTokenOccupancyCache("endpoint", "token")
    assert res["OK"] is True, res["Message"]
    assert not res["Value"], res["Value"]


def test_Transfer(rmClient):
    """
    TransferOccupancy table
    """

    res = rmClient.deleteTransferCache("sourcename", "destinationname")  # just making sure it's not there (yet)
    assert res["OK"] is True, res["Message"]

    # TEST addOrModifyTransferOccupancy
    res = rmClient.addOrModifyTransferCache("sourcename", "destinationname", "metric", 1000.0, datetime.datetime.now())
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectTransferCache("sourcename", "destinationname")
    assert res["OK"] is True, res["Message"]
    # check if the name that we got is equal to the previously added 'destinationname'
    assert res["Value"][0][2] == "metric"

    res = rmClient.addOrModifyTransferCache("sourcename", "destinationname", value=200.0)
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectTransferCache("sourcename", "destinationname")
    # check if the result has changed
    assert res["Value"][0][3] == 200.0

    # TEST deleteTransferOccupancy
    # ...............................................................................
    res = rmClient.deleteTransferCache("sourcename", "destinationname")
    assert res["OK"] is True, res["Message"]

    res = rmClient.selectTransferCache("sourcename", "destinationname")
    assert res["OK"] is True, res["Message"]
    assert not res["Value"], res["Value"]
