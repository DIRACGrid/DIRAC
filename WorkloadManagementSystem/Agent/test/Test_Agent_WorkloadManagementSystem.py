""" Test class for WMS agents
"""

# imports
import unittest
from mock import MagicMock, patch

from DIRAC import gLogger

# sut
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector

mockAM = MagicMock()
mockGC = MagicMock()
mockGC.getValue.return_value = 'TestSetup'
mockOPSObject = MagicMock()
mockOPSObject.getValue.return_value = '123'
mockOPS = MagicMock()
mockOPS.Operations.return_value = mockOPSObject
mockPM = MagicMock()
mockPM.pippo.return_value = {'OK':True, 'Value': ('token', 1)}
mockPM.requestToken.return_value = {'OK':True, 'Value': ('token', 1)}

class AgentsTestCase( unittest.TestCase ):
  """ Base class for the Agents test cases
  """
  def setUp( self ):
    pass

  def tearDown( self ):
    pass

class SiteDirectorBaseSuccess( AgentsTestCase ):

  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gConfig", side_effect = mockGC)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.Operations", side_effect = mockOPS)
  # @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gProxyManager", side_effect = mockPM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule", side_effect = mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule.__init__", new = mockAM)
  def test__getPilotOptions( self, _patch1, _patch2, _patch3 ):
    sd = SiteDirector()
    sd.log = gLogger
    sd.am_getOption = mockAM
    sd.log.setLevel( 'DEBUG' )
    sd.queueDict = {'aQueue':{'ParametersDict':{'CPUTime':12345}}}
    sd.queueDict['aQueue'] = {}
    sd.queueDict['aQueue']['ParametersDict'] = {}
    res = sd._getPilotOptions( 'aQueue', 10 )
    #FIXME: incomplete
    self.assertEqual(res, [None, None])



#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( AgentsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SiteDirectorBaseSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
