""" This is a test of using PilotManagerClient

    In order to run this test we need the following DBs installed:
    - PilotAgentsDB

    And the following services should also be on:
    - PilotManager

   this is pytest!

"""
import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.MonitoringSystem.Client.WebAppClient import WebAppClient
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient
import os

gLogger.setLevel("VERBOSE")


def test_PilotsDB():
    pilots = PilotManagerClient()
    is_diracx_enabled = os.getenv("TEST_DIRACX") == "Yes"
    # webapp = WebAppClient() # ?

    # This will allow you to run the test again if necessary
    for pilot_ref in ["aPilot", "anotherPilot"]:
        res = pilots.deletePilot(pilot_ref)

    res = pilots.addPilotReferences(["aPilot"], "vo")
    assert res["OK"], res["Message"]
    res = pilots.addPilotReferences(["aPilot"], "vo")

    # Duplicates to see if we have a conflict
    # If supports diracx, then it should be detected
    # But old DIRAC doesn't.
    if is_diracx_enabled:
        assert not res["OK"], res
        assert "Conflict" in res["Message"]

    res = pilots.deletePilots("aPilot")
    assert res["OK"], res["Message"]

    res = pilots.addPilotReferences(["anotherPilot"], "vo")
    assert res["OK"], res["Message"]

    res = pilots.getPilotInfo("anotherPilot")
    assert res["OK"], res["Message"]
    assert res["Value"]["anotherPilot"]["AccountingSent"] == "False"
    assert res["Value"]["anotherPilot"]["PilotJobReference"] == "anotherPilot"

    res = pilots.getPilotSummary("", "")
    assert res["OK"], res["Message"]
    assert res["Value"]["Total"]["Submitted"] >= 1

    res = pilots.setPilotStatus("anotherPilot", "Running")
    assert res["OK"], res["Message"]
    res = pilots.getPilotInfo("anotherPilot")
    assert res["OK"], res["Message"]
    assert res["Value"]["anotherPilot"]["Status"] == "Running"

    if is_diracx_enabled:
        res = pilots.getGroupedPilotSummary(["GridSite", "DestinationSite", "VO"])
        assert res["OK"], res["Message"]  # We won't test result (hopefully tested in DiracX)

    res = pilots.deletePilots("anotherPilot")
    assert res["OK"], res["Message"]

    # Delete twice, second time an error is raised
    # DiracX feature
    if is_diracx_enabled:
        res = pilots.deletePilots("anotherPilot")
        assert not res["OK"], res["Message"]
