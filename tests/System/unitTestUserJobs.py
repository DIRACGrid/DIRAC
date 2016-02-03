""" Collection of user jobs for testing purposes
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import time

from DIRAC import gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

from TestDIRAC.Utilities.testJobDefinitions import helloWorld, mpJob

gLogger.setLevel( 'VERBOSE' )

jobsSubmittedList = []

class GridSubmissionTestCase( unittest.TestCase ):
  """ Base class for the Regression test cases
  """
  def setUp( self ):
    result = getProxyInfo()
    if result['Value']['group'] not in ['dirac_user']:
      print "GET A USER GROUP"
      exit( 1 )

  def tearDown( self ):
    pass

class submitSuccess( GridSubmissionTestCase ):

  def test_submit( self ):

    res = helloWorld()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = mpJob()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )



# FIXME: This is also in the extension...? To try!
class monitorSuccess( GridSubmissionTestCase ):

  def test_monitor( self ):

    toRemove = []
    fail = False

    # we will check every 10 minutes, up to 6 hours
    counter = 0
    while counter < 36:
      jobStatus = self.dirac.status( jobsSubmittedList )
      self.assert_( jobStatus['OK'] )
      for jobID in jobsSubmittedList:
        status = jobStatus['Value'][jobID]['Status']
        minorStatus = jobStatus['Value'][jobID]['MinorStatus']
        if status == 'Done':
          self.assert_( minorStatus in ['Execution Complete', 'Requests Done'] )
          jobsSubmittedList.remove( jobID )
          res = self.dirac.getJobOutputLFNs( jobID )
          if res['OK']:
            lfns = res['Value']
            toRemove += lfns
        if status in ['Failed', 'Killed', 'Deleted']:
          fail = True
          jobsSubmittedList.remove( jobID )
      if jobsSubmittedList:
        time.sleep( 600 )
        counter = counter + 1
      else:
        break

    # removing produced files
    res = self.dirac.removeFile( toRemove )
    self.assert_( res['OK'] )

    if fail:
      self.assertFalse( True )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( GridSubmissionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( submitSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( monitorSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
