"""
It is used to test the database modules...
"""
import unittest

import DIRAC.MonitoringSystem.private.DBUtils as moduleTested

################################################################################


class Test_DB(unittest.TestCase):
    """ """

    def setUp(self):
        """
        Setup
        """

        self.moduleTested = moduleTested
        self.testClass = self.moduleTested.DBUtils

    def tearDown(self):
        """
        Tear down
        """

        del self.moduleTested
        del self.testClass


################################################################################


class TestDbUtilsChain(Test_DB):
    """ """

    ################################################################################
    def test_instantiate(self):
        """tests that we can instantiate one object of the tested class"""

        module = self.testClass("MonitoringDB")
        self.assertEqual("DBUtils", module.__class__.__name__)

    ################################################################################
    def test_determineBucketSize(self):

        client = self.testClass("MonitoringDB")
        result = client._determineBucketSize(1458130176000, 1458226213000)
        self.assertTrue(result["OK"])
        self.assertEqual(result["Value"], ("30m", 1800000))

        result = client._determineBucketSize(1458055269000, 1458660069000)
        self.assertTrue(result["OK"])
        self.assertEqual(result["Value"], ("2h", 7200000))

        result = client._determineBucketSize(1458617047000, 1458660247000)
        self.assertTrue(result["OK"])
        self.assertEqual(result["Value"], ("10m", 600000))

        result = client._determineBucketSize(1456068334000, 1458660334000)
        self.assertTrue(result["OK"])
        self.assertEqual(result["Value"], ("8h", 28800000))

        result = client._determineBucketSize(1453476373000, 1458660373000)
        self.assertTrue(result["OK"])
        self.assertEqual(result["Value"], ("12h", 43200000))

        result = client._determineBucketSize(1450884571000, 1458660571000)
        self.assertTrue(result["OK"])
        self.assertEqual(result["Value"], ("1d", 86400000))

        result = client._determineBucketSize(1427038370000, 1458660770000)
        self.assertTrue(result["OK"])
        self.assertEqual(result["Value"], ("4d", 345600000))

        result = client._determineBucketSize(1300807841000, 1458660641000)
        self.assertTrue(result["OK"])
        self.assertEqual(result["Value"], ("7d", 604800000))


if __name__ == "__main__":
    testSuite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_DB)
    testSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestDbUtilsChain))
    unittest.TextTestRunner(verbosity=2).run(testSuite)
