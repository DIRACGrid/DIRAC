###########################################################################################
# $HeadURL$
###########################################################################################
"""Collection of DIRAC useful list related modules.
   By default on Error they return None.
"""

__RCSID__ = "$Id$"

from types import StringTypes
import random
random.seed()

def uniqueElements( aList ):
  """Utility to retrieve list of unique elements in a list (order is kept).

  :param list aList: list of elements
  :return: list of unique elements
  """
  newList = []
  try:
    for i in aList:
      if i not in newList:
        newList.append( i )
    return newList
  except:
    return None

def appendUnique( aList, anObject ):
  """Append to list if object does not exist.

	:param list aList: list of elements
	:param anObject: object you want to append
  """
  if anObject not in aList:
    aList.append( anObject )

def fromChar( inputString, sepChar = "," ):
  """Generates a list splitting a string by the required character(s)
     resulting string items are stripped and empty items are removed.
     
     :param string inputString: list serialised to string
     :param string sepChar: separator 
     :return: list of strings or None if sepChar has a wrong type
  """
  if not ( type( inputString ) in StringTypes and
           type( sepChar ) in StringTypes and
           sepChar ): # to prevent getting an empty String as argument
    return None
  return [ fieldString.strip() for fieldString in inputString.split( sepChar ) if len( fieldString.strip() ) > 0 ]

def randomize( aList ):
  """Return a randomly sorted list.

	:param list aList: list to permute
  """
  tmpList = aList[:]
  random.shuffle( tmpList )
  return tmpList

def sortList( aList, invert = False ):
  """Return a sorted list of ints or list of strings

		:param list aList: list to sort
		:param boolean invert: flag to revert sorting order (default False = list sorted in ascending order)
  """
  return sorted( aList, reverse = invert )

def pop( aList, popElement ):
  """ Pop the first element equal to popElement from the list.

	:param list aList: list
	:param popElement: element to pop
  """
  if popElement in aList:
    return aList.pop( aList.index( popElement ) )

def stringListToString( aList ):
  """This method is used for making MySQL queries with a list of string elements.

    :param list aList: list to be serialized to string for making queries
  """
  return ",".join( ["'" + str( x ) + "'" for x in aList ] )

def intListToString( aList ):
  """This method is used for making MySQL queries with a list of int elements.

  :param list aList: list to be serialized to string for making queries
  """
  return ",".join( [str( x ) for x in aList ] )

def getChunk( aList, chunkSize ):
  """Generator yielding chunk from a list of a size chunkSize. 

  :param list aList: list to be splitted
  :param integer chunkSize: lenght of one chunk
  :raise: StopIteration 

  Usage:

  >>> for chunk in getChunk( aList, chunkSize=10):
        process( chunk )

  """
  for i in range( 0, len( aList ), chunkSize ):
    yield aList[i:i + chunkSize]

def breakListIntoChunks( aList, chunkSize ):
  """This method takes a list as input and breaks it into list of size 'chunkSize'. It returns a list of lists.
  
  :param list aList: list of elements
  :param integer chunkSize: len of a single chunk
  :return: list of lists of length of chunkSize
  :raise: RuntimeError if numberOfFilesInChunk is less than 1
  """
  if chunkSize < 1:
    raise RuntimeError( "chunkSize cannot be less than 1" )
  return [ chunk for chunk in getChunk( aList, chunkSize ) ]

def removeEmptyElements( aList ):
  """Remove empty elements from a list ( [''] ), preserve the order of the non-null elements.

	:param list aList: list of elements
  """
  return [x for x in aList if x]
