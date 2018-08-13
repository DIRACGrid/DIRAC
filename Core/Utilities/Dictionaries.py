""" Collecting utilities for dictionaries
"""

from itertools import islice


def breakDictionaryIntoChunks(aDict, chunkSize):
  """ This function is a generator of chunks of dictionaries of size chunkSize
  """
  iterDict = iter(aDict)
  for _ in xrange(0, len(aDict), chunkSize):
    yield {k: aDict[k] for k in islice(iterDict, chunkSize)}
