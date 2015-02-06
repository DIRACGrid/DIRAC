import unittest, types, os

from mock import MagicMock

from DIRAC import gLogger
from DIRAC.Core.Utilities.Subprocess import Subprocess
from DIRAC.WorkloadManagementSystem.JobWrapper.WatchdogFactory import WatchdogFactory
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import ExecutionThread

class JobWrapperTestCase( unittest.TestCase ):
  """ Base class for the JobWrapper test cases
  """
  def setUp( self ):
    gLogger.setLevel( 'DEBUG' )
    self.currentPID = os.getpid()

  def tearDown( self ):
    pass

class WatchdogSuccess( JobWrapperTestCase ):

  #################################################

  def test_getWatchdog( self ):
    spObject = Subprocess( timeout = False, bufferLimit = 10485760 )
    exeThread = ExecutionThread( spObject, 'ls', 20, 'std.out', 'std.err', dict( os.environ ) )
    res = WatchdogFactory().getWatchdog( self.currentPID, exeThread, spObject, 100000, 10485760 )
    self.assert_( res['OK'] )
    self.assert_( isinstance( res['Value'], types.ObjectType ) )

  def test_WatchdogSuccess( self ):
    spObject = Subprocess( timeout = False, bufferLimit = 10485760 )
    exeThread = ExecutionThread( spObject, 'ls', 20, 'std.out', 'std.err', dict( os.environ ) )
    res = WatchdogFactory().getWatchdog( self.currentPID, exeThread, spObject, 100000, 10485760 )
    wdl = res['Value']

    wdl.getLoadAverage()

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( JobWrapperTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( WatchdogSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
