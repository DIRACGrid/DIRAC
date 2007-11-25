# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/List.py,v 1.11 2007/11/25 15:56:16 acsmith Exp $
__RCSID__ = "$Id: List.py,v 1.11 2007/11/25 15:56:16 acsmith Exp $"
"""
   Collection of DIRAC useful list related modules
   by default on Error they return None
"""

from types import StringTypes
import random,math
random.seed()

def uniqueElements( list ):
  """
     Utility to retrieve list of unique elements in a list (order is kept)
  """
  newList = []
  try:
    for i in list:
      if i not in newList:
        newList.append( i )
    return newList
  except:
    return None

def fromChar( inputString, sepChar = "," ):
  """
     Generates a list splitting a string by the required character(s)
     resulting string items are stripped and empty items are removed
  """
  if not ( type( inputString ) in StringTypes and
           type( sepChar ) in StringTypes and
           sepChar ): # to prevent getting an empty String as argument
    return None

  return [ fieldString.strip() for fieldString in inputString.split( sepChar ) if len( fieldString.strip() ) > 0 ]

def randomize( initialList ):
  """
  Return a randomly sorted list
  """
  #A index list is built so the initial list is left untouched
  indexList = range( len( initialList ) )
  randomList = []
  while len( indexList ) > 0:
    randomPos = random.randrange( len( indexList ) )
    randomList.append( initialList[ indexList[ randomPos ] ] )
    del( indexList[ randomPos ] )
  return randomList

def sortList(list,invert=False):
  """
    Return a sorted list of ints or list of strings
  """
  if not invert:
    list.sort(lambda x, y: cmp(x, y))
  else:
    list.sort(lambda x, y: cmp(y, x))
  return list

def stringListToString(list):
  """
    This method is used for making MySQL queries with a list of string elements.

  """
  str_list = []
  for item in list:
    str_list.append("'%s'" % item)
  str = ','.join(str_list)
  return str

def intListToString(list):
  """
      This method is used for making MySQL queries with a list of int elements.
  """
  str_list = []
  for item in list:
    str_list.append("%s" % item)
  str = ','.join(str_list)
  return str


def breakListIntoChunks(list,numberOfFilesInChunk):
  """ This method takes a list as input and breaks it into list of size 'chunkSize'
      It returns a list of lists.
  """
  listOfLists = []
  numberOfChunks = int(math.ceil(len(list)/float(numberOfFilesInChunk)))
  count = 0
  for i in range(numberOfChunks):
    listOfLists.append(list[count:count+numberOfFilesInChunk])
    count += numberOfFilesInChunk
  return listOfLists
