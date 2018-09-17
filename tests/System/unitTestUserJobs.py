""" Collection of user jobs for testing purposes
"""

# pylint: disable=wrong-import-position, invalid-name

import unittest
import time

from DIRAC.tests.Utilities.testJobDefinitions import helloWorld, mpJob, parametricJob
from DIRAC import gLogger
gLogger.setLevel('DEBUG')

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.Core.Security.ProxyInfo import getProxyInfo

time.sleep(3)  # in theory this should not be needed, but I don't know why, without, it fails.
result = getProxyInfo()
if result['Value']['group'] not in ['lhcb_user', 'dirac_user']:
  print "GET A USER GROUP"
  exit(1)


jobsSubmittedList = []


class GridSubmissionTestCase(unittest.TestCase):
  """ Base class for the Regression test cases
  """

  def setUp(self):
    pass

  def tearDown(self):
    pass


class submitSuccess(GridSubmissionTestCase):
  """ submit jobs
  """

  def test_submit(self):
    """ submit jobs defined in DIRAC.tests.Utilities.testJobDefinitions
    """
    res = helloWorld()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = mpJob()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = parametricJob()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    print "submitted %d jobs: %s" % (len(jobsSubmittedList), ','.join(str(js) for js in jobsSubmittedList))


# FIXME: This is also in the extension...? To try!
# class monitorSuccess( GridSubmissionTestCase ):
#
#   def test_monitor( self ):
#
#     toRemove = []
#     fail = False
#
#     # we will check every 10 minutes, up to 6 hours
#     counter = 0
#     while counter < 36:
#       jobStatus = self.dirac.status( jobsSubmittedList )
#       self.assertTrue( jobStatus['OK'] )
#       for jobID in jobsSubmittedList:
#         status = jobStatus['Value'][jobID]['Status']
#         minorStatus = jobStatus['Value'][jobID]['MinorStatus']
#         if status == 'Done':
#           self.assertTrue( minorStatus in ['Execution Complete', 'Requests Done'] )
#           jobsSubmittedList.remove( jobID )
#           res = self.dirac.getJobOutputLFNs( jobID )
#           if res['OK']:
#             lfns = res['Value']
#             toRemove += lfns
#         if status in ['Failed', 'Killed', 'Deleted']:
#           fail = True
#           jobsSubmittedList.remove( jobID )
#       if jobsSubmittedList:
#         time.sleep( 600 )
#         counter = counter + 1
#       else:
#         break
#
#     # removing produced files
#     res = self.dirac.removeFile( toRemove )
#     self.assertTrue( res['OK'] )
#
#     if fail:
#       self.assertFalse( True )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(GridSubmissionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(submitSuccess))
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( monitorSuccess ) )
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
