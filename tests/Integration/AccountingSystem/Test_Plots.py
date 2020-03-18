###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
""" It is used to test Plotting utilities used to create different plots.
"""

# pylint: disable=invalid-name,wrong-import-position

import unittest

import os
import sys
import math
import operator
from PIL import Image

from DIRAC.Core.Utilities.Plotting.Plots import generateHistogram, generateStackedLinePlot, \
                                                generatePiePlot, generateCumulativePlot

from functools import reduce

plots_directory = os.path.join(os.path.dirname(__file__), 'plots')


def compare(file1Path, file2Path):
  """
  Function used to compare two plots
  returns 0.0 if both are identical
  """

  # Crops image to remove the "Generated on xxxx UTC" string
  image1 = Image.open(file1Path).crop((0, 0, 800, 570))
  image2 = Image.open(file2Path).crop((0, 0, 800, 570))

  h1 = image1.histogram()
  h2 = image2.histogram()
  rms = math.sqrt(reduce(operator.add,
                         map(lambda a, b: (a - b) ** 2, h1, h2)) / len(h1))
  return rms


class PlotsTestCase(unittest.TestCase):
  """
  It is used to test different plots.
  """

  def setUp(self):
    """
    Setup the test case
    """

    self.filename = "plot.png"

  def test_histogram(self):
    """
    test histogram
    """

    res = generateHistogram(self.filename, [2, 2, 3, 4, 5, 5], {})
    self.assertEqual(res['OK'], True)

    res = compare(self.filename, os.path.join(plots_directory, 'histogram1.png'))
    self.assertEqual(0.0, res)

    res = generateHistogram(self.filename,
                            [{'a': [1, 2, 3, 1, 2, 2, 4, 2]}, {'b': [2, 2, 2, 4, 4, 1, 1]}], {'plot_grid': '2:1'})
    self.assertEqual(res['OK'], True)

    res = compare(self.filename, os.path.join(plots_directory, 'histogram2.png'))
    self.assertEqual(0.0, res)

  def test_stackedlineplots(self):
    """
    test stacked line plot
    """

    res = generateStackedLinePlot(self.filename,
                                  {'LCG.Zoltan.hu': {1584460800: 1.0,
                                                     1584489600: 2.0,
                                                     1584511200: 1.0,
                                                     1584464400: 1.0,
                                                     1584540000: 0.022222222222222223,
                                                     1584500400: 2.2,
                                                     1584529200: 1.2,
                                                     1584468000: 0.0,
                                                     1584486000: 1.1,
                                                     1584518400: 0.2,
                                                     1584471600: 1.0,
                                                     1584532800: 0.0022222222222222222,
                                                     1584507600: 0.3,
                                                     1584475200: 1.2,
                                                     1584482400: 0.4,
                                                     1584496800: 5.0,
                                                     1584525600: 0.5,
                                                     1584536400: 0.012777777777777779,
                                                     1584514800: 2.0,
                                                     1584453600: 3.0,
                                                     1584478800: 0.09,
                                                     1584504000: 1.0,
                                                     1584457200: 3.0,
                                                     1584493200: 1.0,
                                                     1584522000: 1.8},
                                   'LCG.CERN.cern': {1584460800: 1.6,
                                                     1584489600: 2.8,
                                                     1584511200: 3.0,
                                                     1584464400: 4.0,
                                                     1584540000: 1.022222222222222223,
                                                     1584500400: 3.2,
                                                     1584529200: 0.2,
                                                     1584468000: 1.0,
                                                     1584486000: 1.1}}, {})

    self.assertEqual(res['OK'], True)

    res = compare(self.filename, os.path.join(plots_directory, 'stackedline.png'))
    self.assertEqual(0.0, res)

  def test_piechartplot(self):
    """
    test pie chart plots
    """
    res = generatePiePlot(self.filename, {'a': 16.0, 'b': 56.0, 'c': 15, 'd': 20}, {})
    self.assertEqual(res['OK'], True)

    res = compare(self.filename, os.path.join(plots_directory, 'piechart.png'))
    self.assertEqual(0.0, res)

  def test_cumulativeplot(self):
    """
    test cumulative stracked line plot
    """

    res = generateCumulativePlot(self.filename,
                                 {'User': {1584460800: 0.0,
                                           1584489600: 0.0,
                                           1584511200: 0.0,
                                           1584464400: 0.0,
                                           1584540000: 16.0,
                                           1584500400: 0.0,
                                           1584529200: 0.0,
                                           1584468000: 0.0,
                                           1584457200: 0.0,
                                           1584518400: 0.0,
                                           1584471600: 0.0,
                                           1584507600: 0.0,
                                           1584475200: 0.0,
                                           1584496800: 0.0,
                                           1584525600: 0.0,
                                           1584536400: 6.0,
                                           1584486000: 0.0,
                                           1584514800: 0.0,
                                           1584482400: 0.0,
                                           1584478800: 0.0,
                                           1584504000: 0.0,
                                           1584532800: 1.0,
                                           1584543600: 21.0,
                                           1584493200: 0.0,
                                           1584522000: 0.0}},
                                 {'span': 3600,
                                  'title': 'Cumulative Jobs by JobType',
                                  'starttime': 1584457326,
                                  'ylabel': 'jobs',
                                  'sort_labels': 'max_value',
                                  'endtime': 1584543726})

    self.assertEqual(res['OK'], True)

    res = compare(self.filename, os.path.join(plots_directory, 'cumulativeplot.png'))
    self.assertEqual(0.0, res)


#############################################################################
# Test Suite run
#############################################################################
if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PlotsTestCase)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
