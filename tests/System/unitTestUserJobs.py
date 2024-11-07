""" Collection of user jobs for testing purposes
"""
# pylint: disable=wrong-import-position, invalid-name
import sys
import time
import unittest

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.tests.Utilities.testJobDefinitions import *
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

gLogger.setLevel("DEBUG")

time.sleep(3)  # in theory this should not be needed, but I don't know why, without, it fails.

jobsSubmittedList = []


class GridSubmissionTestCase(unittest.TestCase):
    """Base class for the Regression test cases"""

    def setUp(self):
        result = getProxyInfo()
        if result["Value"]["group"] not in ["dteam_user", "gridpp_user"]:
            print("GET A USER GROUP")
            sys.exit(1)

        res = DataManager().getReplicas(
            [
                "/dteam/user/f/fstagni/test/testInputFileSingleLocation.txt",
                "/dteam/user/f/fstagni/test/testInputFile.txt",
            ]
        )
        if not res["OK"]:
            print(f"DATAMANAGER.getRepicas failure: {res['Message']}")
            sys.exit(1)
        if res["Value"]["Failed"]:
            print(f"DATAMANAGER.getRepicas failed for something: {res['Value']['Failed']}")
            sys.exit(1)

        replicas = res["Value"]["Successful"]
        if list(replicas["/dteam/user/f/fstagni/test/testInputFile.txt"]) != ["RAL-SE", "UKI-LT2-IC-HEP-disk"]:
            print("/dteam/user/f/fstagni/test/testInputFile.txt locations are not correct")
        if list(replicas["/dteam/user/f/fstagni/test/testInputFileSingleLocation.txt"]) != ["RAL-SE"]:
            print("/dteam/user/f/fstagni/test/testInputFileSingleLocation.txt locations are not correct")

    def tearDown(self):
        pass


class submitSuccess(GridSubmissionTestCase):
    """submit jobs"""

    def test_submit(self):
        """submit jobs defined in DIRAC.tests.Utilities.testJobDefinitions"""
        res = helloWorld()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = helloWorldJenkins()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = helloWorld_input()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = helloWorld_input_single()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = helloWorldCERN()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = helloWorldNCBJ()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = helloWorldGRIDKA()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = helloWorldRAL()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = helloWorldRALPP()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = helloWorldGRIF()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = helloWorldSSHBatch()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = helloWorldARM()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = mpJob()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = mp3Job()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = min2max4Job()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = wholeNodeJob()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = parametricJob()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = parametricJobInputData()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = jobWithOutput()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        res = jobWithOutputs()
        self.assertTrue(res["OK"])
        jobsSubmittedList.append(res["Value"])

        print(f"submitted {len(jobsSubmittedList)} jobs: {','.join(str(js) for js in jobsSubmittedList)}")


#############################################################################
# Test Suite run
#############################################################################

if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(GridSubmissionTestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(submitSuccess))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
