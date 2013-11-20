########################################################################
# $HeadURL$
########################################################################

""" DIRAC FileCatalog utilities
"""

__RCSID__ = "$Id$"

try:
  import hashlib 
  md5 = hashlib
except:
  import md5

import random, os, time
from types import StringTypes, ListType, DictType
from DIRAC import S_OK, S_ERROR

def checkArgumentFormat( path ):
  """ Bring the various possible form of arguments to FileCatalog methods to
      the standard dictionary form
  """
  
  def checkArgumentDict( path ):
    """ Check and process format of the arguments to FileCatalog methods """
    if type( path ) in StringTypes:
      urls = {path:True}
    elif type( path ) == ListType:
      urls = {}
      for url in path:
        urls[url] = True
    elif type( path ) == DictType:
      urls = path
    else:
      return S_ERROR( "checkArgumentDict: Supplied path is not of the correct format" )
    return S_OK( urls )
    
  result = checkArgumentDict( path )
  if not result['OK']:
    return result

  pathDict = result['Value']

  # Bring the lfn path to the normalized form
  urls = {}
  for url in pathDict:
    mUrl = url
    if url.startswith( 'lfn:' ):
      mUrl = url[4:]
    elif url.startswith( 'LFN:' ):
      mUrl = url[4:]  
    if mUrl.startswith('/grid'):  
      mUrl = mUrl[5:] 
    normpath = os.path.normpath( mUrl )
    urls[normpath] = pathDict[url]
  return S_OK( urls )

def generateGuid( checksum, checksumtype ):
  """ Generate a GUID based on the file checksum
  """

  if checksum:
    if checksumtype == "MD5":
      checksumString = checksum
    elif checksumtype == "Adler32":
      checksumString = str( checksum ).zfill( 32 )
    else:
      checksumString = ''
    if checksumString:
      guid = "%s-%s-%s-%s-%s" % ( checksumString[0:8],
                                  checksumString[8:12],
                                  checksumString[12:16],
                                  checksumString[16:20],
                                  checksumString[20:32] )
      guid = guid.upper()
      return guid

  # Failed to use the check sum, generate a new guid    
  myMd5 = md5.md5()
  myMd5.update( str( random.getrandbits( 128 ) ) )
  md5HexString = myMd5.hexdigest()
  guid = "%s-%s-%s-%s-%s" % ( md5HexString[0:8],
                              md5HexString[8:12],
                              md5HexString[12:16],
                              md5HexString[16:20],
                              md5HexString[20:32] )
  guid = guid.upper()
  return guid

def queryTime(f):
  """ Decorator to measure the function call time
  """
  def measureQueryTime(*args, **kwargs):
    start = time.time()
    result = f(*args, **kwargs)
    if not 'QueryTime' in result:
      result['QueryTime'] = time.time() - start
    return result
  return measureQueryTime
