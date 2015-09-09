########################################################################
# $HeadURL$
########################################################################

""" DIRAC FileCatalog utilities
"""

__RCSID__ = "$Id$"

import hashlib as md5

import random, os, time
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.List import intListToString

def checkArgumentFormat( path, generateMap = False ):
  """ Bring the various possible form of arguments to FileCatalog methods to
      the standard dictionary form
  """
  
  def checkArgumentDict( path ):
    """ Check and process format of the arguments to FileCatalog methods """
    if isinstance( path, basestring ):
      urls = {path:True}
    elif isinstance( path, list ):
      urls = {}
      for url in path:
        urls[url] = True
    elif isinstance( path, dict ):
      urls = path
    else:
      return S_ERROR( "checkArgumentDict: Supplied path is not of the correct format" )
    return S_OK( urls )
    
  if not path:
    return S_ERROR( 'Empty input: %s' % str( path ) )  
    
  result = checkArgumentDict( path )
  if not result['OK']:
    return result

  pathDict = result['Value']

  # Bring the lfn path to the normalized form
  urls = {}
  urlMap = {}
  for url in pathDict:
    # avoid empty path...
    if not url:
      continue
    mUrl = url
    if url.startswith( 'lfn:' ):
      mUrl = url[4:]
    elif url.startswith( 'LFN:' ):
      mUrl = url[4:]  
    if mUrl.startswith('/grid'):
      uList = mUrl.split('/')
      if len( uList ) >= 2 and uList[1] == 'grid':  
        mUrl = mUrl[5:] 
    normPath = os.path.normpath( mUrl )
    urls[normPath] = pathDict[url]
    if normPath != url:
      urlMap[normPath] = url
  if generateMap:
    return S_OK( ( urls, urlMap ) )
  else:
    return S_OK( urls )

def checkCatalogArguments( f ):
  """ Decorator to check arguments of FileCatalog calls in the clients
  """
  def processWithCheckingArguments(*args, **kwargs):

    checkFlag = kwargs.pop( 'NoLFNChecking', True )
    if checkFlag:
      argList = list( args )
      lfnArgument = argList[1]
      result = checkArgumentFormat( lfnArgument, generateMap = True )
      if not result['OK']:
        return result
      checkedLFNDict, lfnMap = result['Value']
      argList[1] = checkedLFNDict
      argTuple = tuple( argList )
    else:
      argTuple = args
    result = f(*argTuple, **kwargs)
    if not result['OK']:
      return result

    if not checkFlag:
      return result

    # Restore original paths
    argList[1] = lfnArgument
    failed = {}
    successful = {}
    for lfn in result['Value']['Failed']:
      failed[lfnMap.get( lfn, lfn )] = result['Value']['Failed'][lfn]
    for lfn in result['Value']['Successful']:
      successful[lfnMap.get( lfn, lfn )] = result['Value']['Successful'][lfn]

    result['Value'].update( { "Successful": successful, "Failed": failed } )
    return result

  return processWithCheckingArguments

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

def getIDSelectString( ids ):
  """
  :param ids: input IDs - can be single int, list or tuple or a SELECT string
  :return: Select string
  """
  if isinstance( ids, basestring ) and ids.lower().startswith( 'select' ):
    idString = ids
  elif isinstance( ids, ( int, long ) ):
    idString = '%d' % ids
  elif isinstance( ids, ( tuple, list) ):
    idString = intListToString( ids )
  else:
    return S_ERROR( 'Illegal fileID' )

  return S_OK( idString )

