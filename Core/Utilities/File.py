# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/File.py,v 1.23 2008/06/20 17:38:39 acsmith Exp $
__RCSID__ = "$Id: File.py,v 1.23 2008/06/20 17:38:39 acsmith Exp $"

"""
   Collection of DIRAC useful file related modules
   by default on Error they return None
"""

import os
import md5
import random
import glob
import types

def makeGuid( fileName=None ):
  """
     Utility to create GUID's, if a filename it is provided the
     GUID will correspond to its hexadecimal md5 checksum
     the format is capitalized 8-4-4-4-12, otherwise a random seed it is
     used to create a GUID
  """
  myMd5 = md5.new()
  if fileName:
    try:
      fd = open( fileName, 'r' )
      data = fd.read( 1024*1024 )
      myMd5.update( data )
      fd.close()
    except:
      return None
  else:
    myMd5.update( str( random.getrandbits( 128 ) ) )

  md5HexString = myMd5.hexdigest()

  md5String = "%s-%s-%s-%s-%s" % ( md5HexString[0:8],
                                   md5HexString[8:12],
                                   md5HexString[12:16],
                                   md5HexString[16:20],
                                   md5HexString[20:32] )

  return md5String.upper()

def checkGuid(guid):
    """
       Checks whether a supplied GUID is of the correct format.
       The guid is a string of 36 characters long split into 5 parts of length 8-4-4-4-12.

       INPUT:     guid - string to be checked .
       OPERATION: Split the string on '-', checking each part is correct length.
       OUTPUT:    Returns 1 if the supplied string is a GUID.
                  Returns 0 otherwise.
    """
    guidSplit = guid.split('-')
    if len(guid) == 36 \
      and len(guidSplit[0]) == 8 \
        and len(guidSplit[1]) == 4 \
          and len(guidSplit[2]) == 4 \
            and len(guidSplit[3]) ==4 \
              and len(guidSplit[4]) == 12:
      return True
    else:
      return False

def getSize( fileName ):
  """
  Get size of a file
  """
  try:
    return os.stat( fileName )[6]
  except Exception, v:
    return -1

def getGlobbedTotalSize( files ):
  """
  Get total size of a list of files or a single file.
  Globs the parameter to allow regular expressions
  """
  totalSize = 0
  if type( files ) in ( types.ListType, types.TupleType ):
    for entry in files:
      size = getGlobbedTotalSize( entry )
      if size == -1:
        size = 0
      totalSize += size
  else:
    for path in glob.glob( files ):
      if os.path.isdir( path ):
        for content in os.listdir( path ):
          totalSize += getGlobbedTotalSize( os.path.join( path, content ) )
      if os.path.isfile( path ):
        size = getSize( path )
        if size == -1:
          size = 0
        totalSize += size
  return totalSize

def stringAdler(string):
  """ Calculate adler32 of the supplied string
  """
  try:
    intAdler = adler32(string)
    return hex(intAdler)[-8:]
  except Exception,x:
    return False

def fileAdler(fileName):
  """ Calculate alder32 of the supplied file
  """
  try:
    inputFile = open(fileName)
    inputFileSize = os.stat(fileName)[6]
    increment = 1024*1024
    fullBlocks = inputFileSize/increment
    remainder = inputFileSize%increment

    mbString = inputFile.read(increment)
    myAdler = adler32(mbString)
    for i in range(fullBlocks-1):
      mbString = inputFile.read(increment)
      myAdler = adler32(mbString,myAdler)
    mbString = inputFile.read(remainder)
    myAdler = adler32(mbString,myAdler)
    inputFile.close()
    return hex(myAdler)[-8:]
  except Exception,x:
    return False

if __name__=="__main__":
  import sys
  for p in sys.argv[1:]:
    print "%s : %s bytes" % ( p, getGlobbedTotalSize( p ) )
