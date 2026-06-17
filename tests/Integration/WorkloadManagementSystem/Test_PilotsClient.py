"""This is a test of using PilotManagerClient

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

gLogger.setLevel("VERBOSE")


def test_PilotsDB():
    pilots = PilotManagerClient()
    webapp = WebAppClient()

    res = pilots.addPilotReferences(["aPilot"], "VO")
    assert res["OK"], res["Message"]
    res = pilots.addPilotReferences(["anotherPilot"], "VO")
    assert res["OK"], res["Message"]
    res = pilots.getPilotInfo("anotherPilot")
    assert res["OK"], res["Message"]
    assert res["Value"]["anotherPilot"]["AccountingSent"] == "False"
    assert res["Value"]["anotherPilot"]["PilotJobReference"] == "anotherPilot"

    res = pilots.getPilotSummary("", "")
    assert res["OK"], res["Message"]
    assert res["Value"]["Total"]["Submitted"] >= 1
    res = webapp.getPilotMonitorWeb({}, [], 0, 100)
    assert res["OK"], res["Message"]
    assert res["Value"]["TotalRecords"] >= 1
    res = webapp.getPilotMonitorSelectors()
    assert res["OK"], res["Message"]
    res = webapp.getPilotSummaryWeb({}, [], 0, 100)
    assert res["OK"], res["Message"]
    assert res["Value"]["TotalRecords"] >= 1

    res = pilots.setPilotStatus("anotherPilot", "Running")
    assert res["OK"], res["Message"]
    res = pilots.getPilotInfo("anotherPilot")
    assert res["OK"], res["Message"]
