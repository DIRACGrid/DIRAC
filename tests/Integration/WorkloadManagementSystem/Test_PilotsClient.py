""" This is a test of using PilotManagerClient

    In order to run this test we need the following DBs installed:
    - PilotAgentsDB

    And the following services should also be on:
    - Pilots

   this is pytest!

"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient


gLogger.setLevel('VERBOSE')


def test_PilotsDB():

  pilots = PilotManagerClient()

  res = pilots.addPilotTQReference(['aPilot'], 1, '/a/ownerDN', 'a/owner/Group')
  assert res['OK'] is True
  res = pilots.getCurrentPilotCounters({})
  assert res['OK'] is True
  assert res['Value'] == {'Submitted': 1}
  res = pilots.deletePilots('aPilot')
  assert res['OK'] is True
  res = pilots.getCurrentPilotCounters({})
  assert res['OK'] is True
  assert res['Value'] == {}

  res = pilots.addPilotTQReference(['anotherPilot'], 1, '/a/ownerDN', 'a/owner/Group')
  assert res['OK'] is True
  res = pilots.storePilotOutput('anotherPilot', 'This is an output', 'this is an error')
  assert res['OK'] is True
  res = pilots.getPilotOutput('anotherPilot')
  assert res['OK'] is True
  # Added new key Owner(used not everywhere)
  expectedDict = {'FileList': [],
                  'OwnerDN': '/a/ownerDN',
                  'OwnerGroup': 'a/owner/Group',
                  'StdErr': 'this is an error',
                  'StdOut': 'This is an output'}
  assert all([res['Value'][k] == v for k, v in expectedDict.items()])
  res = pilots.getPilotInfo('anotherPilot')
  assert res['OK'] is True
  assert res['Value']['anotherPilot']['AccountingSent'] == 'False'
  assert res['Value']['anotherPilot']['PilotJobReference'] == 'anotherPilot'

  res = pilots.selectPilots({})
  assert res['OK'] is True
  res = pilots.getPilotSummary('', '')
  assert res['OK'] is True
  assert res['Value']['Total']['Submitted'] == 1
  res = pilots.getPilotMonitorWeb({}, [], 0, 100)
  assert res['OK'] is True
  assert res['Value']['TotalRecords'] == 1
  res = pilots.getPilotMonitorSelectors()
  assert res['OK'] is True
  assert res['Value'] == {'GridType': ['DIRAC'],
                          'OwnerGroup': ['a/owner/Group'],
                          'DestinationSite': ['NotAssigned'],
                          'Broker': ['Unknown'], 'Status': ['Submitted'],
                          'OwnerDN': ['/a/ownerDN'],
                          'GridSite': ['Unknown'],
                          'Owner': []}
  res = pilots.getPilotSummaryWeb({}, [], 0, 100)
  assert res['OK'] is True
  assert res['Value']['TotalRecords'] == 1

  res = pilots.setAccountingFlag('anotherPilot', 'True')
  assert res['OK'] is True
  res = pilots.setPilotStatus('anotherPilot', 'Running')
  assert res['OK'] is True
  res = pilots.getPilotInfo('anotherPilot')
  assert res['OK'] is True
  assert res['Value']['anotherPilot']['AccountingSent'] == 'True'
  assert res['Value']['anotherPilot']['Status'] == 'Running'

  res = pilots.setJobForPilot(123, 'anotherPilot')
  assert res['OK'] is True
  res = pilots.setPilotBenchmark('anotherPilot', 12.3)
  assert res['OK'] is True
  res = pilots.countPilots({})
  assert res['OK'] is True
#     res = pilots.getCounters()
#     # getPilotStatistics

  res = pilots.deletePilots('anotherPilot')
  assert res['OK'] is True
  res = pilots.getCurrentPilotCounters({})
  assert res['OK'] is True
  assert res['Value'] == {}
