""" This is a test of using PilotsClient

    In order to run this test we need the following DBs installed:
    - PilotAgentsDB

    And the following services should also be on:
    - Pilots
"""


# pylint: disable=wrong-import-position,invalid-name

import unittest
import sys

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.Client.PilotsClient import PilotsClient


class TestWMSTestCase(unittest.TestCase):

  def setUp(self):
    self.maxDiff = None

    gLogger.setLevel('VERBOSE')


class pilotsPilots(TestWMSTestCase):
  """ testing WMSAdmin - for PilotAgentsDB
  """

  def test_PilotsDB(self):

    pilots = PilotsClient()

    res = pilots.addPilotTQReference(['aPilot'], 1, '/a/ownerDN', 'a/owner/Group')
    self.assertTrue(res['OK'])
    res = pilots.getCurrentPilotCounters({})
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {'Submitted': 1})
    res = pilots.deletePilots('aPilot')
    self.assertTrue(res['OK'])
    res = pilots.getCurrentPilotCounters({})
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {})

    res = pilots.addPilotTQReference(['anotherPilot'], 1, '/a/ownerDN', 'a/owner/Group')
    self.assertTrue(res['OK'])
    res = pilots.storePilotOutput('anotherPilot', 'This is an output', 'this is an error')
    self.assertTrue(res['OK'])
    res = pilots.getPilotOutput('anotherPilot')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {'OwnerDN': '/a/ownerDN',
                                    'OwnerGroup': 'a/owner/Group',
                                    'StdErr': 'this is an error',
                                    'FileList': [],
                                    'StdOut': 'This is an output'})
    # need a job for the following
#     res = pilots.getJobPilotOutput( 1 )
#     self.assertEqual( res['Value'], {'OwnerDN': '/a/ownerDN', 'OwnerGroup': 'a/owner/Group',
#                                      'StdErr': 'this is an error', 'FileList': [], 'StdOut': 'This is an output'} )
#     self.assertTrue(res['OK'])
    res = pilots.getPilotInfo('anotherPilot')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['anotherPilot']['AccountingSent'], 'False')
    self.assertEqual(res['Value']['anotherPilot']['PilotJobReference'], 'anotherPilot')

    res = pilots.selectPilots({})
    self.assertTrue(res['OK'])
#     res = pilots.getPilotLoggingInfo( 'anotherPilot' )
#     self.assertTrue(res['OK'])
    res = pilots.getPilotSummary('', '')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Total']['Submitted'], 1)
    res = pilots.getPilotMonitorWeb({}, [], 0, 100)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['TotalRecords'], 1)
    res = pilots.getPilotMonitorSelectors()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {'GridType': ['DIRAC'],
                                    'OwnerGroup': ['a/owner/Group'],
                                    'DestinationSite': ['NotAssigned'],
                                    'Broker': ['Unknown'], 'Status': ['Submitted'],
                                    'OwnerDN': ['/a/ownerDN'],
                                    'GridSite': ['Unknown'],
                                    'Owner': []})
    res = pilots.getPilotSummaryWeb({}, [], 0, 100)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['TotalRecords'], 1)

    res = pilots.setAccountingFlag('anotherPilot', 'True')
    self.assertTrue(res['OK'])
    res = pilots.setPilotStatus('anotherPilot', 'Running')
    self.assertTrue(res['OK'])
    res = pilots.getPilotInfo('anotherPilot')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['anotherPilot']['AccountingSent'], 'True')
    self.assertEqual(res['Value']['anotherPilot']['Status'], 'Running')

    res = pilots.setJobForPilot(123, 'anotherPilot')
    self.assertTrue(res['OK'])
    res = pilots.setPilotBenchmark('anotherPilot', 12.3)
    self.assertTrue(res['OK'])
    res = pilots.countPilots({})
    self.assertTrue(res['OK'])
#     res = pilots.getCounters()
#     # getPilotStatistics

    res = pilots.deletePilots('anotherPilot')
    self.assertTrue(res['OK'])
    res = pilots.getCurrentPilotCounters({})
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {})


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestWMSTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(pilotsPilots))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
