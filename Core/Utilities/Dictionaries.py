""" Collecting utilities for dictionaries
"""

from itertools import islice


def breakDictionaryIntoChunks(aDict, chunkSize):
  """ This function is a generator of chunks of dictionaries of size chunkSize
  """
  if chunkSize < 1:
    raise RuntimeError("chunkSize cannot be less than 1")
  iterDict = iter(aDict)
  for _ in xrange(0, len(aDict), chunkSize):
    yield {k: aDict[k] for k in islice(iterDict, chunkSize)}
