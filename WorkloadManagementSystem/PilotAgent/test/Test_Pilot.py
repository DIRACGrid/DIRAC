""" Test class for agents
"""

# imports
import unittest
import json
import os
# from mock import MagicMock

from pilotTools import PilotParams
from pilotCommands import GetPilotVersion

class PilotTestCase( unittest.TestCase ):
  """ Base class for the Agents test cases
  """
  def setUp( self ):
    self.pp = PilotParams()
  
  def tearDown( self ):
    try:
      os.remove('pilot.out')
    except IOError:
      pass


class CommandsTestCase( PilotTestCase ):

  def test_GetPilotVersion( self ):

    self.pp.releaseVersion = 'someVer'
    gpv = GetPilotVersion( self.pp )
    self.assertIsNone( gpv.execute() )

    self.pp.releaseVersion = ''
    gpv = GetPilotVersion( self.pp )
    # no project defined
    self.assertRaises( IOError, gpv.execute )

    # Now defining a local file for test, and all the necessary parameters
    fp = open( 'Test-pilot.json', 'w' )
    json.dump( {'TestSetup':{'Version':['v1r1', 'v2r2']}}, fp )
    fp.close()
    self.pp.releaseProject = 'Test'
    self.pp.setup = 'TestSetup'
    gpv = GetPilotVersion( self.pp )
    gpv.pilotCFGFileLocation = 'file://%s' % os.getcwd()
    self.assertIsNone( gpv.execute() )
    self.assertEqual( gpv.pp.releaseVersion, 'v1r1' )
    os.remove( 'Test-pilot.json' )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( PilotTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( CommandsTestCase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
