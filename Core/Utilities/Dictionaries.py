""" Collecting utilities for dictionaries
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from itertools import islice


def breakDictionaryIntoChunks(aDict, chunkSize):
  """ This function is a generator of chunks of dictionaries of size chunkSize

      :param dict aDict: the dictionary you want to chunk
      :param int chunkSize: the size of the chunks
      :return: a generator object, that generates chunks of the original dictionary in aDict
  """
  iterDict = iter(aDict)
  for _ in range(0, len(aDict), chunkSize):
    yield {k: aDict[k] for k in islice(iterDict, chunkSize)}
