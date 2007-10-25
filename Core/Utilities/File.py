# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/File.py,v 1.17 2007/10/25 15:39:11 acsmith Exp $
__RCSID__ = "$Id: File.py,v 1.17 2007/10/25 15:39:11 acsmith Exp $"

"""
   Collection of DIRAC useful file related modules
   by default on Error they return None
"""

import os
import md5
import random

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
  try:
    return os.lstat( fileName )[6]
  except Exception, v:
    return -1