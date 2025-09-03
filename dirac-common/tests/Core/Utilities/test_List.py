########################################################################
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/01/17 08:17:58
########################################################################

""".. module:: ListTestCase

Test cases for DIRACCommon.Core.Utilities.List module.

"""
import unittest

# sut
from DIRACCommon.Core.Utilities import List


########################################################################
class ListTestCase(unittest.TestCase):
    """py:class ListTestCase
    Test case for DIRACCommon.Core.Utilities.List module.
    """

    def testUniqueElements(self):
        """uniqueElements tests"""
        # empty list
        aList = []
        self.assertEqual(List.uniqueElements(aList), [])
        # redundant elements
        aList = [1, 1, 2, 3]
        self.assertEqual(List.uniqueElements(aList), [1, 2, 3])

    def testAppendUnique(self):
        """appendUnique tests"""
        # empty
        aList = []
        List.appendUnique(aList, None)
        self.assertEqual(aList, [None])
        # redundant element
        aList = [1, 2, 3]
        List.appendUnique(aList, 1)
        self.assertEqual(aList, [1, 2, 3])
        # all unique
        aList = [1, 2]
        List.appendUnique(aList, 3)
        self.assertEqual(aList, [1, 2, 3])

    def testRandomize(self):
        """randomize tests"""
        # empty list
        aList = []
        randList = List.randomize(aList)
        self.assertEqual(randList, [])
        # non empty
        aList = ["1", "2", "3"]
        randList = List.randomize(aList)
        self.assertEqual(len(aList), len(randList))
        for x in aList:
            self.assertEqual(x in randList, True)
        for x in randList:
            self.assertEqual(x in aList, True)

    def testPop(self):
        """pop tests"""
        # empty list
        aList = []
        x = List.pop(aList, 1)
        self.assertEqual(aList, [])
        self.assertEqual(x, None)
        # pop
        aList = [1, 2, 3]
        x = List.pop(aList, 2)
        self.assertEqual(x, 2)
        self.assertEqual(aList, [1, 3])

    def testStringListToString(self):
        """stringListToString tests"""
        # empty list
        aList = []
        aStr = List.stringListToString(aList)
        self.assertEqual(aStr, "")
        # not string elements (should it raise an exception???)
        aList = ["a", 1]
        aStr = List.stringListToString(aList)
        self.assertEqual(aStr, "'a','1'")
        # normal list
        aList = ["a", "b", "c"]
        aStr = List.stringListToString(aList)
        self.assertEqual(aStr, "'a','b','c'")

    def testIntListToString(self):
        """intListToString"""
        # empty list
        aList = []
        aStr = List.intListToString(aList)
        self.assertEqual(aStr, "")
        # int list
        aList = [1, 2, 3]
        aStr = List.intListToString(aList)
        self.assertEqual(aStr, "1,2,3")
        # mixture elements (should it raise an exception???)
        aList = ["1", 2, 3]
        aStr = List.intListToString(aList)
        self.assertEqual(aStr, "1,2,3")

    def testFromChar(self):
        """fromChar tests"""
        # empty string
        aStr = ""
        self.assertEqual(List.fromChar(aStr, "-"), [])
        # wrong sep (should it raise an exception???)
        aStr = "a:b:c"
        self.assertEqual(List.fromChar(aStr, "-"), ["a:b:c"])
        # norman behavior
        aStr = "a:b:c"
        self.assertEqual(List.fromChar(aStr, ":"), ["a", "b", "c"])
        # only sep
        aStr = ","
        self.assertEqual(List.fromChar(aStr, ","), [])
        # too many separators
        aStr = "a,,b,,c,,,"
        self.assertEqual(List.fromChar(aStr, ","), ["a", "b", "c"])

    def testBreakListIntoChunks(self):
        """breakListIntoChunks tests"""
        # empty list
        aList = []
        self.assertEqual(List.breakListIntoChunks(aList, 5), [])
        # negative number of chunks
        try:
            List.breakListIntoChunks([], -2)
        except Exception as val:
            self.assertEqual(isinstance(val, RuntimeError), True)
            self.assertEqual(str(val), "chunkSize cannot be less than 1")

        # normal behavior
        aList = list(range(10))
        self.assertEqual(List.breakListIntoChunks(aList, 5), [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]])
        # and once again this time with a rest
        aList = list(range(10))
        self.assertEqual(List.breakListIntoChunks(aList, 4), [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]])
        # almost empty list, too many chunks
        aList = [1]
        self.assertEqual(List.breakListIntoChunks(aList, 2), [[1]])


# test suite execution
if __name__ == "__main__":
    TESTLOADER = unittest.TestLoader()
    SUITE = TESTLOADER.loadTestsFromTestCase(ListTestCase)
    unittest.TextTestRunner(verbosity=3).run(SUITE)
