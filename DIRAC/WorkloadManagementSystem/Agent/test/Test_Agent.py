""" Test class for WMS agents
"""

# imports
import unittest, importlib
from mock import MagicMock

from DIRAC import gLogger

# sut
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector

class AgentsTestCase( unittest.TestCase ):
  """ Base class for the Agents test cases
  """
  def setUp( self ):
    self.mockAM = MagicMock()
    self.sd_m = importlib.import_module( 'DIRAC.WorkloadManagementSystem.Agent.SiteDirector' )
    self.sd_m.AgentModule = self.mockAM
    self.sd = SiteDirector()
    self.sd.log = gLogger
    self.sd.am_getOption = self.mockAM
    self.sd.log.setLevel( 'DEBUG' )

    self.tc_mock = MagicMock()
    self.tm_mock = MagicMock()

  def tearDown( self ):
    pass

class SiteDirectorBaseSuccess( AgentsTestCase ):

  def test__getPilotOptions( self ):
    self.sd.queueDict = {}
    self.sd.queueDict['aQueue'] = {}
    self.sd.queueDict['aQueue']['ParametersDict'] = {}
    _res = self.sd._getPilotOptions( 'aQueue', 10 )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( AgentsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SiteDirectorBaseSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
