""" Collection of user jobs for testing purposes
"""
# pylint: disable=wrong-import-position, invalid-name
import unittest
import time

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.tests.Utilities.testJobDefinitions import *

gLogger.setLevel("DEBUG")

time.sleep(3)  # in theory this should not be needed, but I don't know why, without, it fails.

jobsSubmittedList = []


class GridSubmissionTestCase(unittest.TestCase):
    """Base class for the Regression test cases"""

    def setUp(self):
        pass

    def tearDown(self):
        pass


class submitSuccess(GridSubmissionTestCase):
    """submit jobs"""

    def test_submit_cloudce(self):
        """submit jobs defined in DIRAC.tests.Utilities.testJobDefinitions"""
        res = helloWorldCloudCE()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        print("submitted %d jobs: %s" % (len(jobsSubmittedList), ",".join(str(js) for js in jobsSubmittedList)))


#############################################################################
# Test Suite run
#############################################################################


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(GridSubmissionTestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(submitSuccess))
    # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( monitorSuccess ) )
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
