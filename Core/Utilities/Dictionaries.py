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


def bytesKeysToStrings(aDict):
  """ Decode dictionary keys from bytes to str

      Primarily useful for supporting Python 3 in DEncode. When forceBytes=True
      is passed to a RPC call all dictionary keys will be bytes. This converts
      them to be more easy to use in Python 3.

      :param dict aDict: the dictionary with keys to decode
      :return: a copy of the dictionary with decoded keys
  """
  return {k.decode() if isinstance(k, bytes) else k: v for k, v in aDict.items()}
