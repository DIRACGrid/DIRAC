import os
import stat
import tempfile
import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import g_X509ChainType, X509Chain
from DIRAC.Core.Security.Locations import getProxyLocation

def writeToProxyFile( proxyContents, fileName = False):
  """
  Write an proxy string to file
  arguments:
    - proxyContents : string object to dump to file
    - fileName : filename to dump to
  """
  if not fileName:
    try:
      fd, proxyLocation = tempfile.mkstemp()
      os.close(fd)
    except IOError:
      return S_ERROR('Failed to create temporary file')
    fileName = proxyLocation
  try:
    fd = open( fileName, 'w' )
    fd.write( proxyContents )
    fd.close()
  except Exception, e:
    return S_ERROR( "Cannot write to file %s :%s" % ( fileName, str(e) ) )
  try:
    os.chmod( fileName, stat.S_IRUSR | stat.S_IWUSR )
  except Exception, e:
    return S_ERROR( "Cannot set permissions to file %s :%s" % ( filePath, str(e) ) )
  return S_OK( fileName )

def writeChainToProxyFile( proxyChain, fileName ):
  """
  Write an X509Chain to file
  arguments:
    - proxyChain : X509Chain object to dump to file
    - fileName : filename to dump to
  """
  retVal = proxyChain.dumpAllToString()
  if not retVal[ 'OK' ]:
    return retVal
  return writeToProxyFile( retVal[ 'Value' ], fileName )

def writeChainToTemporaryFile( proxyChain ):
  """
  Write a proxy chain to a temporary file
  return S_OK( string with name of file )/ S_ERROR
  """
  try:
    fd, proxyLocation = tempfile.mkstemp()
    os.close(fd)
  except IOError:
    return S_ERROR('Failed to create temporary file')
  retVal = writeChainToProxyFile( proxyChain, proxyLocation )
  if not retVal[ 'OK' ]:
    try:
      os.unlink( proxyLocation )
    except:
      pass
    return retVal
  return S_OK( proxyLocation )

def deleteMultiProxy( multiProxyDict ):
  """
  Delete a file from a multiProxyArgument if needed
  """
  if multiProxyDict[ 'tempFile' ]:
    try:
      os.unlink( multiProxyDict[ 'file' ] )
    except:
      pass

def multiProxyArgument( proxy = False ):
  """
  Load a proxy:
    proxyChain param can be:
      : Default -> use current proxy
      : string -> upload file specified as proxy
      : X509Chain -> use chain
    returns:
      S_OK( { 'file' : <string with file location>,
              'chain' : X509Chain object,
              'tempFile' : <True if file is temporal>
            }
      S_ERROR
  """
  tempFile = False
  #Set env
  if type( proxy ) == g_X509ChainType:
    tempFile = True
    retVal = writeChainToTemporaryFile( proxy )
    if not retVal[ 'OK' ]:
      return retVal
    proxyLoc = retVal[ 'Value' ]
  else:
    if not proxy:
      proxyLoc = getProxyLocation()
      if not proxyLoc:
        return S_ERROR( "Can't find proxy" )
    if type( proxy ) == types.StringType:
      proxyLoc = proxy
    #Load proxy
    proxy = X509Chain()
    retVal = proxy.loadProxyFromFile( proxyLoc)
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't load proxy at %s" % proxyLoc )
  return S_OK( { 'file' : proxyLoc,
                 'chain' : proxy,
                 'tempFile' : tempFile } )
