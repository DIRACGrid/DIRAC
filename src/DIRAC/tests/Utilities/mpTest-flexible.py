#!/usr/bin/env python

# it expects to be called with the the number of processors allowed to use

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import sys
from multiprocessing import Pool, current_process

##############################################################################
# copy from List.py for convenience


def getChunk(aList, chunkSize):
  """Generator yielding chunk from a list of a size chunkSize.

  :param aList: list to be splitted
  :type aList: python:list
  :param int chunkSize: lenght of one chunk
  :raise: StopIteration

  Usage:

  >>> for chunk in getChunk( aList, chunkSize=10):
        process( chunk )

  """
  for i in range(0, len(aList), chunkSize):
    yield aList[i:i + chunkSize]


def breakListIntoChunks(aList, chunkSize):
  """This function takes a list as input and breaks it into list of size 'chunkSize'.
     It returns a list of lists.

  :param aList: list of elements
  :type aList: python:list
  :param int chunkSize: len of a single chunk
  :return: list of lists of length of chunkSize
  :raise: RuntimeError if numberOfFilesInChunk is less than 1
  """
  if chunkSize < 1:
    raise RuntimeError("chunkSize cannot be less than 1")
  if isinstance(aList, (set, dict, tuple)):
    aList = list(aList)
  return [chunk for chunk in getChunk(aList, chunkSize)]

##############################################################################


def do_sum(l):
  print(current_process().name)
  return sum(l)


def main(nProc):
  p = Pool(nProc)
  r = list(range(1, 20000000))
  chunkSize = int(20000000 / nProc)
  rc = breakListIntoChunks(r, chunkSize)
  r = p.map(do_sum, rc[:nProc])
  print(r, sum(r))


if __name__ == '__main__':
  nProc = sys.argv[1]
  main(int(nProc))
