""" Test_RSS_Policy_FreeDiskSpacePolicy
"""
# pylint: disable=protected-access

import unittest

import DIRAC.ResourceStatusSystem.Policy.FreeDiskSpacePolicy as moduleTested

################################################################################


class FreeDiskSpacePolicy_TestCase(unittest.TestCase):
    def setUp(self):
        """
        Setup
        """

        self.moduleTested = moduleTested
        self.testClass = self.moduleTested.FreeDiskSpacePolicy

    def tearDown(self):
        """
        Tear down
        """

        del self.moduleTested
        del self.testClass


################################################################################


class FreeDiskSpacePolicy_Success(FreeDiskSpacePolicy_TestCase):
    def test_instantiate(self):
        """tests that we can instantiate one object of the tested class"""

        module = self.testClass()
        self.assertEqual("FreeDiskSpacePolicy", module.__class__.__name__)

    def test_evaluate(self):
        """tests the method _evaluate"""

        module = self.testClass()

        res = module._evaluate({"OK": False, "Message": "Bo!"})
        self.assertTrue(res["OK"])
        self.assertEqual("Error", res["Value"]["Status"])
        self.assertEqual("Bo!", res["Value"]["Reason"])

        res = module._evaluate({"OK": True, "Value": None})
        self.assertTrue(res["OK"])
        self.assertEqual("Unknown", res["Value"]["Status"])
        self.assertEqual("No values to take a decision", res["Value"]["Reason"])

        res = module._evaluate({"OK": True, "Value": []})
        self.assertTrue(res["OK"])
        self.assertEqual("Unknown", res["Value"]["Status"])
        self.assertEqual("No values to take a decision", res["Value"]["Reason"])

        res = module._evaluate({"OK": True, "Value": [{"A": 1}]})
        self.assertTrue(res["OK"])
        self.assertEqual("Error", res["Value"]["Status"])
        self.assertEqual("Key Total missing", res["Value"]["Reason"])

        res = module._evaluate({"OK": True, "Value": {"Total": 1}})
        self.assertTrue(res["OK"])
        self.assertEqual("Error", res["Value"]["Status"])
        self.assertEqual("Key Free missing", res["Value"]["Reason"])

        res = module._evaluate({"OK": True, "Value": {"Total": 100, "Fre": 0.0}})
        self.assertTrue(res["OK"])
        self.assertEqual("Error", res["Value"]["Status"])
        self.assertEqual("Key Free missing", res["Value"]["Reason"])

        res = module._evaluate({"OK": True, "Value": {"Total": 100, "Free": 0.0}})
        self.assertTrue(res["OK"])
        self.assertEqual("Banned", res["Value"]["Status"])
        self.assertEqual("Too little free space", res["Value"]["Reason"])

        res = module._evaluate({"OK": True, "Value": {"Total": 100, "Free": 4.0, "Guaranteed": 1}})
        self.assertTrue(res["OK"])
        self.assertEqual("Degraded", res["Value"]["Status"])
        self.assertEqual("Little free space", res["Value"]["Reason"])

        res = module._evaluate({"OK": True, "Value": {"Total": 100, "Free": 100, "Guaranteed": 1}})
        self.assertTrue(res["OK"])
        self.assertEqual("Active", res["Value"]["Status"])
        self.assertEqual("Enough free space", res["Value"]["Reason"])


################################################################################

if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(FreeDiskSpacePolicy_TestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(FreeDiskSpacePolicy_Success))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
