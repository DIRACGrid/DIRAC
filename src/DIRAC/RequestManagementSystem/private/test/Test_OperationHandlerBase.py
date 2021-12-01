""" tests for Graph OperationHandlerBase module
"""
import unittest

# # SUT
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import DynamicProps


class DynamicPropTests(unittest.TestCase):
    """
    ..  class:: DynamicPropTests
    """

    def testDynamicProps(self):
        """test dynamic props"""

        class TestClass(metaclass=DynamicProps):
            """
            .. class:: TestClass

            dummy class
            """

            pass

        # # dummy instance
        testObj = TestClass()
        # # makeProperty in
        self.assertEqual(hasattr(testObj, "makeProperty"), True)
        self.assertEqual(callable(getattr(testObj, "makeProperty")), True)
        # # .. and works  for rw properties
        testObj.makeProperty("rwTestProp", 10)  # pylint: disable=no-member
        self.assertEqual(hasattr(testObj, "rwTestProp"), True)
        self.assertEqual(getattr(testObj, "rwTestProp"), 10)
        testObj.rwTestProp += 1  # pylint: disable=no-member
        self.assertEqual(getattr(testObj, "rwTestProp"), 11)
        # # .. and ro as well
        testObj.makeProperty("roTestProp", "I'm read only", True)  # pylint: disable=no-member
        self.assertEqual(hasattr(testObj, "roTestProp"), True)
        self.assertEqual(getattr(testObj, "roTestProp"), "I'm read only")
        # # AttributeError for read only property setattr
        try:
            testObj.roTestProp = 11
        except AttributeError as error:
            self.assertEqual(str(error), "can't set attribute")


# # test execution
if __name__ == "__main__":
    testLoader = unittest.TestLoader()
    tests = testLoader.loadTestsFromTestCase(DynamicPropTests)
    unittest.TextTestRunner(verbosity=3).run(tests)
