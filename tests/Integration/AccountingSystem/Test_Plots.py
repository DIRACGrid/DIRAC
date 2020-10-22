""" It is used to test Plotting utilities used to create different plots.
"""

# pylint: disable=invalid-name,wrong-import-position

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import math
import operator

from functools import reduce
from PIL import Image

# sut
from DIRAC.Core.Utilities.Plotting.Plots import generateHistogram, generateStackedLinePlot, \
    generatePiePlot, generateCumulativePlot, generateQualityPlot, generateTimedStackedBarPlot, \
    generateNoDataPlot, generateErrorMessagePlot

plots_directory = os.path.join(os.path.dirname(__file__), 'plots')


def compare(file1Path, file2Path):
  """
  Function used to compare two plots
  returns 0.0 if both are identical

  :param str file1Path: Path to the file1.
  :param str file2Path: Path to the file2.

  :return: float value rms.
  """

  # Crops image to remove the "Generated on xxxx UTC" string
  image1 = Image.open(file1Path).crop((0, 0, 800, 570))
  image2 = Image.open(file2Path).crop((0, 0, 800, 570))

  h1 = image1.histogram()
  h2 = image2.histogram()
  rms = math.sqrt(reduce(operator.add,
                         map(lambda a, b: (a - b) ** 2, h1, h2)) / len(h1))
  return rms


filename = "plot.png"


def test_histogram():
  """
  test histogram
  """

  res = generateHistogram(filename, [2, 2, 3, 4, 5, 5], {})
  assert res['OK'] is True

  res = compare(filename, os.path.join(plots_directory, 'histogram1.png'))
  assert res == 0.0

  res = generateHistogram(filename,
                          [{'a': [1, 2, 3, 1, 2, 2, 4, 2]}, {'b': [2, 2, 2, 4, 4, 1, 1]}], {'plot_grid': '2:1'})
  assert res['OK'] is True

  res = compare(filename, os.path.join(plots_directory, 'histogram2.png'))
  assert res == 0.0

  res = generateHistogram(filename, [{'a': [1]}, {'b': [2, 3, 3, 5, 5]}], {})
  assert res['OK'] is True

  res = compare(filename, os.path.join(plots_directory, 'histogram3.png'))
  assert res == 0.0


def test_stackedlineplots():
  """
  test stacked line plot
  """

  res = generateStackedLinePlot(filename,
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

  assert res['OK'] is True

  res = compare(filename, os.path.join(plots_directory, 'stackedline.png'))
  assert res == 0.0


def test_piechartplot():
  """
  test pie chart plots
  """
  res = generatePiePlot(filename, {'a': 16.0, 'b': 56.0, 'c': 15, 'd': 20}, {})
  assert res['OK'] is True

  res = compare(filename, os.path.join(plots_directory, 'piechart.png'))
  assert res == 0.0


def test_cumulativeplot():
  """
  test cumulative stracked line plot
  """

  res = generateCumulativePlot(filename,
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

  assert res['OK'] is True

  res = compare(filename, os.path.join(plots_directory, 'cumulativeplot.png'))
  assert res == 0.0


def test_qualityplot():
  """
  Test quality plot
  """

  res = generateQualityPlot(filename, {
      'User': {
          1584543600: 37.5,
          1584547200: 37.5,
          1584619200: 33.33333333333333,
          1584601200: 36.53846153846153}}, {})
  assert res['OK'] is True

  res = compare(filename, os.path.join(plots_directory, 'qualityplot1.png'))
  assert res == 0.0

  res = generateQualityPlot(filename,
                            {'User': {1584543600: 37.5,
                                      1584547200: 37.5,
                                      1584619200: 33.33333333333333,
                                      1584601200: 36.53846153846153}},
                            {'endtime': 1584627764,
                             'span': 3600,
                             'starttime': 1584541364,
                             'title': 'Job CPU efficiency by JobType'})
  assert res['OK'] is True

  res = compare(filename, os.path.join(plots_directory, 'qualityplot2.png'))
  assert res == 0.0


def test_timestackedbarplot():
  """
  test timed stacked bar plot
  """
  res = generateTimedStackedBarPlot(filename,
                                    {'LCG.Cern.cern': {1584662400: 0.0,
                                                       1584691200: 0.0,
                                                       1584637200: 15.9593220339,
                                                       1584666000: 0.0,
                                                       1584694800: 0.0,
                                                       1584626400: 31.867945823900005,
                                                       1584669600: 0.0,
                                                       1584644400: 0.0,
                                                       1584615600: 0.0,
                                                       1584673200: 0.0,
                                                       1584633600: 14.0406779661,
                                                       1584676800: 0.0,
                                                       1584684000: 0.0,
                                                       1584651600: 0.0,
                                                       1584680400: 0.0,
                                                       1584612000: 0.0,
                                                       1584640800: 0.0,
                                                       1584655200: 0.0,
                                                       1584622800: 23.2293933044,
                                                       1584698400: 0.0,
                                                       1584630000: 9.5970654628,
                                                       1584658800: 0.0,
                                                       1584648000: 0.0,
                                                       1584687600: 12.0,
                                                       1584619200: 10.3055954089},
                                     'LCG.NCBJ.pl': {1584691200: 0.1,
                                                     1584662400: 2.0,
                                                     1584651600: 0.0,
                                                     1584637200: 0.0,
                                                     1584694800: 4.0,
                                                     1584626400: 0.0,
                                                     1584669600: 6.0,
                                                     1584655200: 0.0,
                                                     1584644400: 0.0,
                                                     1584615600: 0.0,
                                                     1584673200: 0.0,
                                                     1584633600: 9.0,
                                                     1584676800: 0.0,
                                                     1584698400: 0.0,
                                                     1584622800: 0.0,
                                                     1584680400: 0.0,
                                                     1584612000: 0.0,
                                                     1584640800: 0.0,
                                                     1584684000: 0.0,
                                                     1584666000: 0.0,
                                                     1584630000: 0.0,
                                                     1584658800: 0.0,
                                                     1584648000: 0.0,
                                                     1584687600: 0.0,
                                                     1584619200: 0.0}},
                                    {'ylabel': 'jobs / hour',
                                     'endtime': 1584700844,
                                     'span': 3600,
                                     'starttime': 1584614444,
                                     'title': 'Jobs by Site'})
  assert res['OK'] is True

  res = compare(filename, os.path.join(plots_directory, 'timedstackedbarplot.png'))
  assert res == 0.0


def test_nodataplot():
  """
  Test no data plot
  """

  res = generateNoDataPlot(filename, {}, {'title': 'Test plot'})
  assert res['OK'] is True
  res = compare(filename, os.path.join(plots_directory, 'nodata.png'))
  assert res == 0.0


def test_error():
  """
  Test error message plot
  """

  res = generateErrorMessagePlot("testing error message")
  assert res['OK'] is True
  with open(filename, 'wb') as out:
    out.write(res['Value'])

  res = compare(filename, os.path.join(plots_directory, 'error.png'))
  assert res == 0.0
