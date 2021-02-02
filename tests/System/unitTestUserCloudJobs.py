""" Collection of user jobs for testing purposes
"""

# pylint: disable=wrong-import-position, invalid-name

from __future__ import print_function, absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import unittest
import time


from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.tests.Utilities.testJobDefinitions import *

gLogger.setLevel('DEBUG')

time.sleep(3)  # in theory this should not be needed, but I don't know why, without, it fails.

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
    res = helloWorldCloud()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    print("submitted %d jobs: %s" % (len(jobsSubmittedList), ','.join(str(js) for js in jobsSubmittedList)))



#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(GridSubmissionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(submitSuccess))
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( monitorSuccess ) )
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
