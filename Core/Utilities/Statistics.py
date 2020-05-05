##################################################################################################
# $HeadURL$
##################################################################################################

"""Collection of DIRAC useful statistics related modules.

.. warning::

   By default on Error they return None.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from math import sqrt     # Mathematical functions.


def getMean(numbers):
  """Returns the arithmetic mean of a numeric list.

  :param numbers: data sample
  :type numbers: python:list
  """
  if len(numbers):
    numbers = sorted([float(x) for x in numbers])
    return sum(numbers) / float(len(numbers))


def getMedian(numbers):
  """ Return the median of the list of numbers.

  :param numbers: data sample
  :type numbers: python:list
  """
  # Sort the list and take the middle element.
  nbNum = len(numbers)
  if not nbNum:
    return
  copy = sorted([float(x) for x in numbers])
  if nbNum & 1:         # There is an odd number of elements
    return copy[nbNum // 2]
  else:
    return 0.5 * (copy[nbNum // 2 - 1] + copy[nbNum // 2])


def getVariance(numbers, posMean='Empty'):
  """Determine the measure of the spread of the data set about the mean.
  Sample variance is determined by default; population variance can be
  determined by setting population attribute to True.

  :param numbers: data sample
  :type numbers: python:list
  :param mixed posMean: mean of a sample or 'Empty' str
  """
  if not len(numbers):
    return
  if posMean == 'Empty':
    mean = getMean(numbers)
  else:
    mean = posMean
  numbers = sorted([float(x) for x in numbers])

  # Subtract the mean from each data item and square the difference.
  # Sum all the squared deviations.
  return sum((float(item) - mean) ** 2.0 for item in numbers) / len(numbers)


def getStandardDeviation(numbers, variance='Empty', mean='Empty'):
  """Determine the measure of the dispersion of the data set based on the
  variance.

  :param numbers: data sample
  :type numbers: python:list
  :param mixed variance: variance or str 'Empty'
  :param mixed mean: mean or str 'Empty'
  """
  if not len(numbers):
    return
  # Take the square root of the variance.
  if variance == 'Empty':
    if mean == 'Empty':
      variance = getVariance(numbers)
    else:
      variance = getVariance(numbers, posMean=mean)
  return sqrt(variance)
