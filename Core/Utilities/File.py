#####################################################################################
# $HeadURL$
#####################################################################################
"""Collection of DIRAC useful file related modules.

.. warning::
   By default on Error they return None.
"""

__RCSID__ = "$Id$"

import os
try:
  import hashlib as md5
except ImportError:
  import md5
import random
import glob
import types
import re

def makeGuid( fileName = None ):
  """Utility to create GUID. If a filename is provided the
     GUID will correspond to its content's hexadecimal md5 checksum.
     Otherwise a random seed is used to create the GUID.
     The format is capitalized 8-4-4-4-12.
     
     .. warning::
        Could return None in case of OSError or IOError.
     
     :param string fileName: name of file 
  """
  myMd5 = md5.md5()
  if fileName:
    try:
      fd = open( fileName, 'r' )
      data = fd.read( 10 * 1024 * 1024 )
      myMd5.update( data )
      fd.close()
    except:
      return None
  else:
    myMd5.update( str( random.getrandbits( 128 ) ) )
  md5HexString = myMd5.hexdigest().upper()
  return "-".join( [ md5HexString[0:8],
                    md5HexString[8:12],
                    md5HexString[12:16],
                    md5HexString[16:20],
                    md5HexString[20:32] ] )

def checkGuid( guid ):
  """Checks whether a supplied GUID is of the correct format.
     The guid is a string of 36 characters [0-9A-F] long split into 5 parts of length 8-4-4-4-12.

     .. warning::
        As we are using GUID produced by various services and some of them could not follow
        convention, this function is passing by a guid which can be made of lower case chars or even just 
        have 5 parts of proper length with whatever chars.

     :param string guid: string to be checked
     :return: True (False) if supplied string is (not) a valid GUID. 
  """
  reGUID = re.compile( "^[0-9A-F]{8}(-[0-9A-F]{4}){3}-[0-9A-F]{12}$" )
  if reGUID.match( guid.upper() ):
    return True
  else:
    guid = [ len( x ) for x in guid.split( "-" ) ]
    if ( guid == [ 8, 4, 4, 4, 12 ] ):
      return True
  return False

def getSize( fileName ):
  """Get size of a file.

  :param string fileName: name of file to be checked
  
  The os module claims only OSError can be thrown, 
  but just for curiosity it's catching all possible exceptions.

  .. warning:: 
     On any exception it returns -1.
  
  """
  try:
    return os.stat( fileName )[6]
  except Exception:
    return - 1

def getGlobbedTotalSize( files ):
  """Get total size of a list of files or a single file.
  Globs the parameter to allow regular expressions.

  :params list files: list or tuple of strings of files
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

def getGlobbedFiles( files ):
  """Get list of files or a single file.
  Globs the parameter to allow regular expressions.
  
  :params list files: list or tuple of strings of files
  """
  globbedFiles = []
  if type( files ) in ( types.ListType, types.TupleType ):
    for entry in files:
      globbedFiles += getGlobbedFiles( entry )
  else:
    for path in glob.glob( files ):
      if os.path.isdir( path ):
        for content in os.listdir( path ):
          globbedFiles += getGlobbedFiles( os.path.join( path, content ) )
      if os.path.isfile( path ):
        globbedFiles.append( path )
  return globbedFiles

def getCommonPath( files ):
  """Get the common path for all files in the file list.

  :param list files: list of strings with paths
  """
  def properSplit( dirPath ):
    """Splitting of path to drive and path parts for non-Unix file systems.

    :param string dirPath: path
    """
    nDrive, nPath = os.path.splitdrive( dirPath )
    return  [ nDrive ] + [ d for d in nPath.split( os.sep ) if d.strip() ]
  if not files:
    return ""
  commonPath = properSplit( files[0] )
  for fileName in files:
    if os.path.isdir( fileName ):
      dirPath = fileName
    else:
      dirPath = os.path.dirname( fileName )
    nPath = properSplit( dirPath )
    tPath = []
    for i in range( min( len( commonPath ), len( nPath ) ) ):
      if commonPath[ i ] != nPath[ i ]:
        break
      tPath .append( commonPath[ i ] )
    if not tPath:
      return ""
    commonPath = tPath
  return tPath[0] + os.sep + os.path.join( *tPath[1:] )

def getMD5ForFiles( fileList ):
  """Calculate md5 for the content of all the files.

  :param list fileList: list of paths
  """
  fileList.sort()
  hashMD5 = md5.md5()
  for filePath in fileList:
    if ( os.path.isdir( filePath ) ):
      continue
    fd = open( filePath, "rb" )
    buf = fd.read( 4096 )
    while buf:
      hashMD5.update( buf )
      buf = fd.read( 4096 )
    fd.close()
  return hashMD5.hexdigest()

if __name__ == "__main__":
  import sys
  for p in sys.argv[1:]:
    print "%s : %s bytes" % ( p, getGlobbedTotalSize( p ) )
