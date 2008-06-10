import os
import stat
import tempfile
from DIRAC import S_OK, S_ERROR

def writeToProxyFile( proxyContents, fileName ):
  """
  Write an proxy string to file
  arguments:
    - proxyContents : string object to dump to file
    - fileName : filename to dump to
  """
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
  return S_OK()

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
  retVal = writeChainToProxyFile( proxyLocation )
  if not retVal[ 'OK' ]:
    try:
      os.unlink( proxyLocation )
    except:
      pass
    return retVal
  return S_OK( proxyLocation )

