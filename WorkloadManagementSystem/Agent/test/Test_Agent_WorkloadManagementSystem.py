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
mockGCReply = MagicMock()
mockGCReply.return_value = 'TestSetup'
mockOPSObject = MagicMock()
mockOPSObject.getValue.return_value = '123'
mockOPSReply = MagicMock()
mockOPSReply.return_value = '123'

mockOPS = MagicMock()
mockOPS.Operations.return_value = mockOPSObject
mockPM = MagicMock()
mockPM.requestToken.return_value = {'OK':True, 'Value': ('token', 1)}
mockPMReply = MagicMock()
mockPMReply.return_value = {'OK':True, 'Value': ('token', 1)}


gLogger.setLevel('DEBUG')

class AgentsTestCase( unittest.TestCase ):
  """ Base class for the Agents test cases
  """
  def setUp( self ):
    pass

  def tearDown( self ):
    pass

class SiteDirectorBaseSuccess( AgentsTestCase ):

  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gConfig.getValue", side_effect = mockGCReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.Operations", side_effect = mockOPS)
  # @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gProxyManager", side_effect = mockPM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gProxyManager.requestToken", side_effect =mockPMReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule", side_effect = mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule.__init__", new = mockAM)
  def test__getPilotOptions( self, _patch1, _patch2, _patch3, _patch4 ):
    sd = SiteDirector()
    sd.log = gLogger
    sd.am_getOption = mockAM
    sd.log.setLevel( 'DEBUG' )
    sd.queueDict = {'aQueue':{'CEName': 'aCE',
			      'QueueName': 'aQueue',
			      'ParametersDict':{'CPUTime':12345,
						'Community': 'lhcb',
						'OwnerGroup': ['lhcb_user'],
						'Setup': 'LHCb-Production',
						'Site': ['LCG.CERN.cern', 'LCG.CNAF.it'],
						'SubmitPool': ''}}}
    res = sd._getPilotOptions( 'aQueue', 10 )
    self.assertEqual(res, [None, None])



#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( AgentsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SiteDirectorBaseSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
