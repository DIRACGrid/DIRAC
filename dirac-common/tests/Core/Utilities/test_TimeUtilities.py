"""Test class for TimeUtilities"""
# imports
import unittest

# sut
from DIRACCommon.Core.Utilities.TimeUtilities import timeThis


class logClass:
    def __init__(self):
        self._systemName = "aSystemName"
        self._subName = "sSubName"


@timeThis
def myMethod():
    print("boh")


class myClass:
    def __init__(self):
        self.log = logClass()

    @timeThis
    def myMethodInAClass(self):
        print("boh")


class myBetterClass:
    def __init__(self):
        self.log = logClass()
        self.log._subName = "anotherSubName"

    @timeThis
    def myMethodInAClass(self):
        print("boh")


class myEvenBetterClass:
    def __init__(self):
        self.log = logClass()
        self.transString = "this is a transString"

    @timeThis
    def myMethodInAClass(self, a, b=None):
        print("boh")


class TimeTestCase(unittest.TestCase):
    """Base class for the TimeUtilities test cases"""

    def setUp(self):
        pass

    def tearDown(self):
        pass


class TimeSuccess(TimeTestCase):
    def test_timeThis(self):
        self.assertIsNone(myMethod())
        self.assertIsNone(myClass().myMethodInAClass())
        self.assertIsNone(myBetterClass().myMethodInAClass())
        a1 = ["aa", "bb"]
        a2 = "bb"
        self.assertIsNone(myEvenBetterClass().myMethodInAClass(a1, b=a2))
        a1 = "aa"
        a2 = {"a": "aa", "b": "bb"}
        self.assertIsNone(myEvenBetterClass().myMethodInAClass(a1, b=a2))
        a1 = "aa"
        a2 = {"a": "aa", "b": "bb"}
        self.assertIsNone(myEvenBetterClass().myMethodInAClass(a=a1, b=a2))
        a1 = "aa"
        a2 = {"a": "aa", "b": "bb", "c": "cc"}
        self.assertIsNone(myEvenBetterClass().myMethodInAClass(a=a2, b=a2))


#############################################################################
# Test Suite run
#############################################################################

if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TimeTestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TimeSuccess))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
