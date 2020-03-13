########################################################################
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/01/17 12:42:07
########################################################################

""" :mod: StatisticsTestCase
    =======================

    .. module: StatisticsTestCase
    :synopsis: Test cases for DIRAC.Core.Utilities.Statistics module
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Test cases for DIRAC.Core.Utilities.Statistics module
"""

# imports
import unittest
from DIRAC.Core.Utilities.Statistics import *

__RCSID__ = "$Id$"


########################################################################
class StatisticsTestCase(unittest.TestCase):

  """
  .. class:: StatisticsTestCase

  Test cases for DIRAC.Core.Utilities.Statistics

  """

  def testGetMean(self):
    """ getMean tests
    """
    # empty list
    aList = []
    self.assertEqual(getMean(aList), None)
    # nan in list
    try:
      aList = [0, 1, 2, "a"]
      getMean(aList)
    except ValueError:
      pass
    # correct
    aList = [1, 1, 1, 1]
    self.assertEqual(getMean(aList), 1.0)

  def testGetMedian(self):
    """ getMedian tests
    """
    # empty list
    aList = []
    self.assertEqual(getMedian(aList), None)
    # nan in list
    try:
      aList = [1, 2, "a"]
      self.assertEqual(getMedian(aList), None)
    except ValueError:
      pass
    # odd
    aList = [1]
    self.assertEqual(getMedian(aList), 1.0)
    # even
    aList = [1, 2]
    self.assertEqual(getMedian(aList), 1.5)

  def testGetVariance(self):
    """ getVariance tests
    """
    # empty list
    aList = []
    self.assertEqual(getVariance(aList), None)
    # nan in list
    try:
      aList = [1, "a"]
      getVariance(aList)
    except ValueError:
      pass
    # the simplest
    aList = [0]
    self.assertEqual(getVariance(aList), 0.0)
    # normal execution
    aList = [1, 2, 3]
    self.assertEqual(getVariance(aList), 2.0 / 3.0)
    self.assertEqual(getVariance(aList, 2.0), 2.0 / 3.0)
    # around mean = 1.0, should be 5/3
    self.assertEqual(getVariance(aList, 1.0), 5.0 / 3.0)

  def testgetStandardDeviation(self):
    """ getStandardDeviation tests
    """
    # empty list
    aList = []
    self.assertEqual(getStandardDeviation(aList), None)
    try:
      # nan in list
      aList = [1, 2, None]
      self.assertEqual(getStandardDeviation(aList), None)
    except TypeError:
      pass
    # one element
    aList = [1]
    self.assertEqual(getStandardDeviation(aList), 0.0)
    # more elements
    aList = [1, 2, 3]
    self.assertEqual(getStandardDeviation(aList), sqrt(2.0 / 3.0))
    self.assertEqual(getStandardDeviation(aList, variance=2.0 / 3.0), sqrt(2.0 / 3.0))
    self.assertEqual(getStandardDeviation(aList, variance=2.0 / 3.0, mean=1.0), sqrt(2.0 / 3.0))
    self.assertEqual(getStandardDeviation(aList, variance="Empty", mean=1.0), sqrt(5.0 / 3.0))


# test suite execution
if __name__ == "__main__":

  TESTLOADER = unittest.TestLoader()
  SUITE = TESTLOADER.loadTestsFromTestCase(StatisticsTestCase)
  unittest.TextTestRunner(verbosity=3).run(SUITE)
