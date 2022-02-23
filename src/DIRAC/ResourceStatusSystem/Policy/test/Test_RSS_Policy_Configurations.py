""" Test_RSS_Policy_Configurations
"""
import unittest

import DIRAC.ResourceStatusSystem.Policy.Configurations as moduleTested


################################################################################


class Configurations_TestCase(unittest.TestCase):
    def setUp(self):
        """
        Setup
        """

        self.moduleTested = moduleTested

    def tearDown(self):
        """
        TearDown
        """
        del self.moduleTested


################################################################################
# Tests


class Configurations_Success(Configurations_TestCase):
    def test_policiesMeta(self):
        """tests that the configuration does not have any funny key"""

        self.assertEqual(True, hasattr(self.moduleTested, "POLICIESMETA"))

        policiesMeta = self.moduleTested.POLICIESMETA

        for _policyName, policyMeta in policiesMeta.items():
            self.assertEqual({"args", "command", "description", "module"}, set(policyMeta))


################################################################################
################################################################################

if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(Configurations_TestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Configurations_Success))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
