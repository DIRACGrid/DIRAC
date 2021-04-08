"""Collection of DIRAC useful list related modules.
   By default on Error they return None.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import random
import sys


def uniqueElements(aList):
  """Utility to retrieve list of unique elements in a list (order is kept).

  :param aList: list of elements
  :type aList: python:list
  :return: list of unique elements
  """
  result = []
  seen = set()
  try:
    for i in aList:
      if i not in seen:
        result.append(i)
        seen.add(i)
    return result
  except Exception:
    return None


def appendUnique(aList, anObject):
  """ Append to list if object does not exist.

     :param aList: list of elements
     :type aList: python:list
     :param anObject: object you want to append
  """
  if anObject not in aList:
    aList.append(anObject)


def fromChar(inputString, sepChar=","):
  """Generates a list splitting a string by the required character(s)
     resulting string items are stripped and empty items are removed.

     :param string inputString: list serialised to string
     :param string sepChar: separator
     :return: list of strings or None if sepChar has a wrong type
  """
  # to prevent getting an empty String as argument
  if not (isinstance(inputString, six.string_types) and isinstance(sepChar, six.string_types) and sepChar):
    return None
  return [fieldString.strip() for fieldString in inputString.split(sepChar) if len(fieldString.strip()) > 0]


def randomize(aList):
  """Return a randomly sorted list.

     :param aList: list to permute
     :type aList: python:list
  """
  tmpList = list(aList)
  random.shuffle(tmpList)
  return tmpList


def pop(aList, popElement):
  """ Pop the first element equal to popElement from the list.

      :param aList: list
      :type aList: python:list
      :param popElement: element to pop
  """
  if popElement in aList:
    return aList.pop(aList.index(popElement))


def stringListToString(aList):
  """This function is used for making MySQL queries with a list of string elements.

    :param aList: list to be serialized to string for making queries
    :type aList: python:list
  """
  return ",".join("'%s'" % x for x in aList)


def intListToString(aList):
  """This function is used for making MySQL queries with a list of int elements.

  :param aList: list to be serialized to string for making queries
  :type aList: python:list
  """
  return ",".join(str(x) for x in aList)


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
  if isinstance(aList, (set, dict, tuple, {}.keys().__class__,
                        {}.items().__class__, {}.values().__class__)):
    aList = list(aList)
  return [chunk for chunk in getChunk(aList, chunkSize)]


def getIndexInList(anItem, aList):
  """ Return the index of the element x in the list l
      or sys.maxint if it does not exist

      :param anItem: element to look for
      :param list aList: list to look into

      :return: the index or sys.maxint
  """
  # try:
  if anItem in aList:
    return aList.index(anItem)
  else:
    return sys.maxsize
  # except ValueError:
  #   return sys.maxsize
