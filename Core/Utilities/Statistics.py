# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Statistics.py,v 1.1 2008/11/10 20:39:00 acsmith Exp $
__RCSID__ = "$Id: Statistics.py,v 1.1 2008/11/10 20:39:00 acsmith Exp $"

"""
   Collection of DIRAC useful statistics related modules
   by default on Error they return None
"""

from math import sqrt     # Mathematical functions.

def getMean(numbers):
   "Returns the arithmetic mean of a numeric list."
   return sum(numbers) / float(len(numbers))

def getMedian(numbers):
   "Return the median of the list of numbers."
   # Sort the list and take the middle element.
   n = len(numbers)
   copy = numbers[:] # So that "numbers" keeps its original order
   copy.sort()
   if n & 1:         # There is an odd number of elements
      return copy[n // 2]
   else:
      return (copy[n // 2 - 1] + copy[n // 2]) / 2

def getVariance(numbers,posMean='Empty'):
  """Determine the measure of the spread of the data set about the mean.
  Sample variance is determined by default; population variance can be
  determined by setting population attribute to True.
  """
  x = 0 # Summation variable.

  if posMean == 'Empty':
    mean = getMean(numbers)
  else:
    mean = posMean

  # Subtract the mean from each data item and square the difference.
  # Sum all the squared deviations.
  for item in numbers:
     x += (float(item) - mean)**2.0

  try:
     # Divide sum of squares by N-1 (sample variance).
     variance = x/(len(numbers))
  except:
     variance = 0
  return variance

def getStandardDeviation(numbers,variance='Empty',mean='Empty'):
  """Determine the measure of the dispersion of the data set based on the
  variance.
  """
  # Take the square root of the variance.
  if variance =='Empty':
    if mean == 'Empty':
      variance = getVariance(numbers)
    else:
      variance = getVariance(numbers,posMean=mean)
  return sqrt(variance)

