""" This is a test of using PilotManagerClient

    In order to run this test we need the following DBs installed:
    - PilotAgentsDB

    And the following services should also be on:
    - PilotManager

   this is pytest!

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient


gLogger.setLevel('VERBOSE')


def test_PilotsDB():
  realDN = '/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch'
  realGroup = 'dirac_user'
  pilots = PilotManagerClient()
  res = pilots.addPilotTQReference(['aPilot'], 1, realDN, realGroup)
  assert res['OK'] is True, res['Message']
  res = pilots.getCurrentPilotCounters({})
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {'Submitted': 1}, res['Value']
  res = pilots.deletePilots('aPilot')
  assert res['OK'] is True, res['Message']
  res = pilots.getCurrentPilotCounters({})
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {}, res['Value']

  res = pilots.addPilotTQReference(['anotherPilot'], 1, realDN, realGroup)
  assert res['OK'] is True, res['Message']
  res = pilots.storePilotOutput('anotherPilot', 'This is an output', 'this is an error')
  assert res['OK'] is True, res['Message']
  res = pilots.getPilotOutput('anotherPilot')
  assert res['OK'] is True, res['Message']
  # There are new "Owner" key ... Therefore, if the main keys match then all is well
  expectedDict = {'FileList': [],
                  'OwnerDN': realDN,
                  'OwnerGroup': realGroup,
                  'StdErr': 'this is an error',
                  'StdOut': 'This is an output'}
  assert all([res['Value'][k] == v for k, v in expectedDict.items()])
  res = pilots.getPilotInfo('anotherPilot')
  assert res['OK'] is True, res['Message']
  assert res['Value']['anotherPilot']['AccountingSent'] == 'False', res['Value']
  assert res['Value']['anotherPilot']['PilotJobReference'] == 'anotherPilot', res['Value']

  res = pilots.selectPilots({})
  assert res['OK'] is True, res['Message']
  res = pilots.getPilotSummary('', '')
  assert res['OK'] is True, res['Message']
  assert res['Value']['Total']['Submitted'] == 1
  res = pilots.getPilotMonitorWeb({}, [], 0, 100)
  assert res['OK'] is True, res['Message']
  assert res['Value']['TotalRecords'] == 1
  res = pilots.getPilotMonitorSelectors()
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {'GridType': ['DIRAC'],
                          'OwnerGroup': [realGroup],
                          'DestinationSite': ['NotAssigned'],
                          'Broker': ['Unknown'], 'Status': ['Submitted'],
                          'OwnerDN': [realDN],
                          'GridSite': ['Unknown'],
                          'Owner': ['adminusername']}, res['Value']
  res = pilots.getPilotSummaryWeb({}, [], 0, 100)
  assert res['OK'] is True, res['Message']
  assert res['Value']['TotalRecords'] == 1, res['Value']

  res = pilots.setAccountingFlag('anotherPilot', 'True')
  assert res['OK'] is True, res['Message']
  res = pilots.setPilotStatus('anotherPilot', 'Running')
  assert res['OK'] is True, res['Message']
  res = pilots.getPilotInfo('anotherPilot')
  assert res['OK'] is True, res['Message']
  assert res['Value']['anotherPilot']['AccountingSent'] == 'True', res['Value']
  assert res['Value']['anotherPilot']['Status'] == 'Running', res['Value']

  res = pilots.setJobForPilot(123, 'anotherPilot')
  assert res['OK'] is True, res['Message']
  res = pilots.setPilotBenchmark('anotherPilot', 12.3)
  assert res['OK'] is True, res['Message']
  res = pilots.countPilots({})
  assert res['OK'] is True, res['Message']
#     res = pilots.getCounters()
#     # getPilotStatistics

  res = pilots.deletePilots('anotherPilot')
  assert res['OK'] is True, res['Message']
  res = pilots.getCurrentPilotCounters({})
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {}, res['Value']
