""" Test class for WMS clients
"""

# imports
import unittest
import mock

from DIRAC.WorkloadManagementSystem.Client.Matcher import Matcher

class ClientsTestCase( unittest.TestCase ):
  """ Base class for the Clients test cases
  """
  def setUp( self ):
    self.pilotAgentsDBMock = mock.MagicMock()
    self.jobDBMock = mock.MagicMock()
    self.tqDBMock = mock.MagicMock()
    self.jlDBMock = mock.MagicMock()
    self.opsHelperMock = mock.MagicMock()
    self.matcher = Matcher( pilotAgentsDB = self.pilotAgentsDBMock,
                            jobDB = self.jobDBMock,
                            tqDB = self.tqDBMock,
                            jlDB = self.jlDBMock,
                            opsHelper = self.opsHelperMock )
  
  def tearDown( self ):
    pass


class MatcherTestCase( ClientsTestCase ):

  def test__processResourceDescription( self ):

    resourceDescription = {'Architecture': 'x86_64-slc6',
                           'CEQueue': 'jenkins-queue_not_important',
                           'CPUNormalizationFactor': '9.5',
                           'CPUScalingFactor': '9.5',
                           'CPUTime': 1080000,
                           'CPUTimeLeft': 5000,
                           'DIRACVersion': 'v8r0p1',
                           'FileCatalog': 'LcgFileCatalogCombined',
                           'GridCE': 'jenkins.cern.ch',
                           'GridMiddleware': 'DIRAC',
                           'LHCbPlatform': 'x86_64-slc5-gcc43-opt',
                           'LocalSE': ['CERN-SWTEST'],
                           'MaxTotalJobs': 100,
                           'MaxWaitingJobs': 10,
                           'OutputURL': 'gsiftp://localhost',
                           'PilotBenchmark': 9.5,
                           'PilotReference': 'somePilotReference',
                           'Platform': 'x86_64-slc6',
                           'ReleaseProject': 'LHCb',
                           'ReleaseVersion': 'v8r0p1',
                           'Setup': 'LHCb-Certification',
                           'Site': 'DIRAC.Jenkins.ch',
                           'WaitingToRunningRatio': 0.05}

    res = self.matcher._processResourceDescription( resourceDescription )
    resExpected = {'Setup': 'LHCb-Certification',
                   'ReleaseVersion': 'v8r0p1',
                   'CPUTime': 1080000,
                   'DIRACVersion': 'v8r0p1',
                   'PilotReference': 'somePilotReference',
                   'PilotBenchmark': 9.5,
                   'ReleaseProject': 'LHCb'}

    self.assertEqual( res, resExpected )

  def test_(self):



#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MatcherTestCase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

