"""Collection of DIRAC useful list related modules.
   By default on Error they return None.
"""
import random
import sys
from typing import Any


def uniqueElements(aList: list) -> list:
    """Utility to retrieve list of unique elements in a list (order is kept)."""

    # Use dict.fromkeys instead of set ensure the order is preserved
    return list(dict.fromkeys(aList))


def appendUnique(aList: list, anObject: Any):
    """Append to list if object does not exist.

    :param aList: list of elements
    :param anObject: object you want to append
    """
    if anObject not in aList:
        aList.append(anObject)


def fromChar(inputString: str, sepChar: str = ","):
    """Generates a list splitting a string by the required character(s)
    resulting string items are stripped and empty items are removed.

    :param inputString: list serialised to string
    :param sepChar: separator
    :return: list of strings or None if sepChar has a wrong type
    """
    # to prevent getting an empty String as argument
    if not (isinstance(inputString, str) and isinstance(sepChar, str) and sepChar):
        return None
    return [fieldString.strip() for fieldString in inputString.split(sepChar) if len(fieldString.strip()) > 0]


def randomize(aList: list) -> list:
    """Return a randomly sorted list.

    :param aList: list to permute
    """
    tmpList = list(aList)
    random.shuffle(tmpList)
    return tmpList


def pop(aList, popElement):
    """Pop the first element equal to popElement from the list.

    :param aList: list
    :type aList: python:list
    :param popElement: element to pop
    """
    if popElement in aList:
        return aList.pop(aList.index(popElement))


def stringListToString(aList: list) -> str:
    """This function is used for making MySQL queries with a list of string elements.

    :param aList: list to be serialized to string for making queries
    """
    return ",".join("'%s'" % x for x in aList)


def intListToString(aList: list) -> str:
    """This function is used for making MySQL queries with a list of int elements.

    :param aList: list to be serialized to string for making queries
    """
    return ",".join(str(x) for x in aList)


def getChunk(aList: list, chunkSize: int):
    """Generator yielding chunk from a list of a size chunkSize.

    :param aList: list to be splitted
    :param chunkSize: lenght of one chunk
    :raise: StopIteration

    Usage:

    >>> for chunk in getChunk( aList, chunkSize=10):
          process( chunk )

    """
    chunkSize = int(chunkSize)
    for i in range(0, len(aList), chunkSize):
        yield aList[i : i + chunkSize]


def breakListIntoChunks(aList: list, chunkSize: int):
    """This function takes a list as input and breaks it into list of size 'chunkSize'.
       It returns a list of lists.

    :param aList: list of elements
    :param chunkSize: len of a single chunk
    :return: list of lists of length of chunkSize
    :raise: RuntimeError if numberOfFilesInChunk is less than 1
    """
    if chunkSize < 1:
        raise RuntimeError("chunkSize cannot be less than 1")
    if isinstance(aList, (set, dict, tuple, {}.keys().__class__, {}.items().__class__, {}.values().__class__)):
        aList = list(aList)
    return [chunk for chunk in getChunk(aList, chunkSize)]


def getIndexInList(anItem: Any, aList: list) -> int:
    """Return the index of the element x in the list l
    or sys.maxint if it does not exist

    :param anItem: element to look for
    :param aList: list to look into

    :return: the index or sys.maxint
    """
    # try:
    if anItem in aList:
        return aList.index(anItem)
    else:
        return sys.maxsize
