""" Testing the API and a bit more.
    It will submit a number of test jobs locally (via runLocal), using the python unittest to assess the results.
    Can be automatized.
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest

from TestDIRAC.Utilities.IntegrationTest import IntegrationTest
from TestDIRAC.Utilities.utils import find_all

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

class UserJobTestCase( IntegrationTest ):
  """ Base class for the UserJob test cases
  """
  def setUp( self ):
    super( IntegrationTest, self ).setUp()

    self.d = Dirac()
    self.exeScriptLocation = find_all( 'exe-script.py', '.', 'Integration' )[0]
    self.mpExe = find_all( 'testMpJob.sh', '.', 'Utilities' )[0]

class HelloWorldSuccess( UserJobTestCase ):
  def test_execute( self ):

    j = Job()

    j.setName( "helloWorld-test" )
    j.setExecutable( self.exeScriptLocation )
    res = j.runLocal( self.d )
    self.assertTrue( res['OK'] )


class HelloWorldPlusSuccess( UserJobTestCase ):
  """ Adding quite a lot of calls from the API, for pure test purpose
  """

  def test_execute( self ):

    job = Job()

    job.setName( "helloWorld-test" )
    job.setExecutable( find_all( "helloWorld.py", '.', 'Integration' )[0],
                       arguments = "This is an argument",
                       logFile = "aLogFileForTest.txt" ,
                       parameters=[('executable', 'string', '', "Executable Script"), 
                                   ('arguments', 'string', '', 'Arguments for executable Script'), 
                                   ( 'applicationLog', 'string', '', "Log file name" ),
                                   ( 'someCustomOne', 'string', '', "boh" )],
                       paramValues = [( 'someCustomOne', 'aCustomValue' )] )
    job.setBannedSites( ['LCG.SiteA.com', 'DIRAC.SiteB.org'] )
    job.setOwner( 'ownerName' )
    job.setOwnerGroup( 'ownerGroup' )
    job.setName( 'jobName' )
    job.setJobGroup( 'jobGroup' )
    job.setType( 'jobType' )
    job.setDestination( 'DIRAC.someSite.ch' )
    job.setCPUTime( 12345 )
    job.setLogLevel( 'DEBUG' )

    res = job.runLocal( self.d )
    self.assertTrue( res['OK'] )


class LSSuccess( UserJobTestCase ):
  def test_execute( self ):
    """ just testing unix "ls"
    """

    job = Job()

    job.setName( "ls-test" )
    job.setExecutable( "/bin/ls", '-l' )
    res = job.runLocal( self.d )
    self.assertTrue( res['OK'] )


class MPSuccess( UserJobTestCase ):
  def test_execute( self ):
    """ this one tests that I can execute a job that requires multi-processing
    """

    j = Job()

    j.setName( "MP-test" )
    j.setExecutable( self.mpExe )
    j.setInputSandbox( find_all( 'mpTest.py', '.', 'Utilities' )[0] )
    j.setTag( 'MultiProcessor' )
    res = j.runLocal( self.d )
    self.assertTrue( res['OK'] )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UserJobTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldPlusSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( LSSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MPSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

