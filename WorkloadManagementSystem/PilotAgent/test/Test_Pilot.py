""" Test class for agents
"""

# imports
import unittest
import json
import os

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
      os.remove( 'pilot.json' )
      os.remove( 'pilot.json-local' )
    except IOError:
      pass


class CommandsTestCase( PilotTestCase ):

  def test_GetPilotVersion( self ):

    # Now defining a local file for test, and all the necessary parameters
    fp = open( 'pilot.json', 'w' )
    json.dump( {'TestSetup':{'Version':['v1r1', 'v2r2']}}, fp )
    fp.close()
    self.pp.setup = 'TestSetup'
    self.pp.pilotCFGFileLocation = 'file://%s' % os.getcwd()
    gpv = GetPilotVersion( self.pp )
    self.assertIsNone( gpv.execute() )
    self.assertEqual( gpv.pp.releaseVersion, 'v1r1' )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( PilotTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( CommandsTestCase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
