########################################################################
# $HeadURL $
# File: StorageElementProxyHandler.py
########################################################################
""" 
:mod: StorageElementProxyHandler
 
.. module: StorageElementProxyHandler
:synopsis: This is a service which represents a DISET proxy to the Storage Element component.

This is used to get and put files from a remote storage.
"""

__RCSID__ = "$Id$"

import os
import shutil
import SocketServer
import threading
import socket
import random
from types import DictType, ListType, StringType, StringTypes
## from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Utilities.Subprocess import pythonCall
from DIRAC.Core.Utilities.Os import getDiskSpace
from DIRAC.Core.Utilities.DictCache import DictCache    
from DIRAC.DataManagementSystem.private.HttpStorageAccessHandler import HttpStorageAccessHandler

## globals
BASE_PATH = ""
HTTP_FLAG = False
HTTP_PORT = 9180
HTTP_PATH = ""

def purgeCacheDirectory( path ):
  """ del recursively :path: """
  shutil.rmtree( path )
  
gRegister = DictCache(purgeCacheDirectory)
    
def initializeStorageElementProxyHandler( serviceInfo ):
  """ handler initialisation """

  global BASE_PATH, HTTP_FLAG, HTTP_PORT, HTTP_PATH
  cfgPath = serviceInfo['serviceSectionPath']

  BASE_PATH = gConfig.getValue( "%s/BasePath" % cfgPath, BASE_PATH )
  if not BASE_PATH:
    gLogger.error( 'Failed to get the base path' )
    return S_ERROR( 'Failed to get the base path' )
  
  gLogger.info('The base path obtained is %s. Checking its existence...' % BASE_PATH)
  if not os.path.exists(BASE_PATH):
    gLogger.info('%s did not exist. Creating....' % BASE_PATH)
    os.makedirs(BASE_PATH)

  HTTP_FLAG = gConfig.getValue( "%s/HttpAccess" % cfgPath, False )
  if HTTP_FLAG:
    HTTP_PATH = '%s/httpCache' % BASE_PATH
    HTTP_PATH = gConfig.getValue( "%s/HttpCache" % cfgPath, HTTP_PATH )
    if not os.path.exists( HTTP_PATH ):
      gLogger.info('Creating HTTP cache directory %s' % (HTTP_PATH) )
      os.makedirs( HTTP_PATH )
    HTTP_PORT = gConfig.getValue( "%s/HttpPort" % cfgPath, 9180 )
    gLogger.info('Creating HTTP server thread, port:%d, path:%s' % ( HTTP_PORT, HTTP_PATH ) )
    httpThread = HttpThread( HTTP_PORT, HTTP_PATH )

  return S_OK()

class ThreadedSocketServer( SocketServer.ThreadingMixIn, SocketServer.TCPServer ):
  """ bag dummy class to hold ThreadingMixIn and TCPServer """
  pass

class HttpThread( threading.Thread ):
  """ 
  .. class:: HttpThread

  Single daemon thread runing HttpStorageAccessHandler.
  """
  def __init__( self, port, path ):
    """ c'tor """
    self.port = port
    self.path = path
    threading.Thread.__init__( self )
    self.setDaemon( 1 )
    self.start()
  
  def run( self ):
    """ thread run """
    global gRegister, BASE_PATH
    os.chdir( self.path )
    handler = HttpStorageAccessHandler
    handler.register = gRegister
    handler.basePath = self.path
    httpd = ThreadedSocketServer( ("", self.port), handler )
    httpd.serve_forever()

class StorageElementProxyHandler(RequestHandler):
  """ 
  .. class:: StorageElementProxyHandler
  """

  types_callProxyMethod = [ StringType, StringType, ListType, DictType ]
  def export_callProxyMethod( self, se, name, args, kargs ):
    """ A generic method to call methods of the Storage Element.
    """
    res = pythonCall( 200, self.__proxyWrapper, se, name, args, kargs )
    if res['OK']:
      return res['Value']   
    return res

  def __proxyWrapper( self, se, name, args, kargs ):
    """ The wrapper will obtain the client proxy and set it up in the environment.

        The required functionality is then executed and returned to the client.
    """
    res = self.__prepareSecurityDetails()
    if not res['OK']:
      return res
    storageElement = StorageElement( se )
    method = getattr( storageElement, name ) if hasattr( storageElement, name ) else None
    if not method:
      return S_ERROR( "Method '%s' isn't implemented!" % name )
    if not callable( getattr( storageElement, name ) ):
      return S_ERROR( "Attribute '%s' isn't a method!" % name )
    return method( *args, **kargs )

  types_uploadFile = [ StringType, StringType ]
  def export_uploadFile( self, se, pfn ):
    """ This method uploads a file present in the local cache to the specified storage element
    """
    res = pythonCall( 300, self.__uploadFile, se, pfn )
    if res['OK']:
      return res['Value']
    return res

  def __uploadFile(self, se, pfn):
    """ proxied upload file """
    res = self.__prepareSecurityDetails()
    if not res['OK']:
      return res

    # Put file to the SE
    try:
      storageElement = StorageElement(se)
    except AttributeError, x:
      errStr = "__uploadFile: Exception while instantiating the Storage Element."
      gLogger.exception( errStr, se, str(x) )
      return S_ERROR(errStr)
    putFileDir = "%s/putFile" % BASE_PATH
    localFileName = "%s/%s" % ( putFileDir, os.path.basename(pfn) )
    res = storageElement.putFile( { pfn : localFileName }, True )
    if not res['OK']:
      gLogger.error("prepareFile: Failed to put local file to storage.", res['Message'] )
    # Clear the local cache
    try:
      shutil.rmtree(putFileDir)
      gLogger.debug("Cleared existing putFile cache")
    except Exception, x:
      gLogger.exception("Failed to remove destination dir.", putFileDir, x )
    return res

  types_prepareFile = [ StringType, StringType ]
  def export_prepareFile(self, se, pfn):
    """ This method simply gets the file to the local storage area
    """
    res = pythonCall( 300, self.__prepareFile, se, pfn )
    if res['OK']:
      return res['Value']
    return res
    
  def __prepareFile( self, se, pfn ):
    """ proxied prepare file """
    res = self.__prepareSecurityDetails()
    if not res['OK']:
      return res
  
    # Clear the local cache
    getFileDir = "%s/getFile" % BASE_PATH
    if os.path.exists(getFileDir):
      try:
        shutil.rmtree(getFileDir)
        gLogger.debug("Cleared existing getFile cache")
      except Exception, x:
        gLogger.exception("Failed to remove destination directory.", getFileDir, x )
    os.mkdir(getFileDir)        
   
    # Get the file to the cache 
    try:
      storageElement = StorageElement(se)
    except AttributeError, x:
      errStr = "prepareFile: Exception while instantiating the Storage Element."
      gLogger.exception( errStr, se, str(x) )
      return S_ERROR(errStr)
    res = storageElement.getFile( pfn, localPath = "%s/getFile" % BASE_PATH, singleFile = True )
    if not res['OK']:
      gLogger.error( "prepareFile: Failed to get local copy of file.", res['Message'] )
      return res
    return S_OK()

  types_prepareFileForHTTP = [ list(StringTypes)+[ListType] ]
  def export_prepareFileForHTTP(self, lfn):
    """ This method simply gets the file to the local storage area using LFN
    """

    # Do clean-up, should be a separate regular thread
    gRegister.purgeExpired()

    key = str( random.getrandbits( 128 ) )
    result = pythonCall( 300, self.__prepareFileForHTTP, lfn, key )
    if result['OK']:
      result = result['Value']
      if result['OK']:
        if HTTP_FLAG:
          host = socket.getfqdn()
          url = 'http://%s:%d/%s' % ( host, HTTP_PORT, key )
          gRegister.add( key, 1800, result['CachePath'] )
          result['HttpKey'] = key
          result['HttpURL'] = url
        return result
      return result
    return result
    
  def __prepareFileForHTTP( self, lfn, key ):
    """ proxied preapre file for HTTP """
    global HTTP_PATH
    
    res = self.__prepareSecurityDetails()
    if not res['OK']:
      return res
  
    # Clear the local cache
    getFileDir = "%s/%s" % ( HTTP_PATH, key )
    os.makedirs(getFileDir)
   
    # Get the file to the cache 
    from DIRAC.DataManagementSystem.Client.DataManager import DataManager
    dataMgr = DataManager()
    result = dataMgr.getFile( lfn, destinationDir = getFileDir )
    result['CachePath'] = getFileDir
    return result  
    
  ############################################################
  #
  # This is the method to setup the proxy and configure the environment with the client credential
  #

  def __prepareSecurityDetails(self):
    """ Obtains the connection details for the client
    """
    try:
      credDict = self.getRemoteCredentials()
      clientDN = credDict['DN']
      clientUsername = credDict['username']
      clientGroup = credDict['group']
      gLogger.debug( "Getting proxy for %s@%s (%s)" % ( clientUsername, clientGroup, clientDN ) )
      res = gProxyManager.downloadVOMSProxy( clientDN, clientGroup ) 
      if not res['OK']:
        return res
      chain = res['Value']
      proxyBase = "%s/proxies" % BASE_PATH
      if not os.path.exists(proxyBase):
        os.makedirs(proxyBase)
      proxyLocation = "%s/proxies/%s-%s" % ( BASE_PATH, clientUsername, clientGroup )
      gLogger.debug("Obtained proxy chain, dumping to %s." % proxyLocation)
      res = gProxyManager.dumpProxyToFile( chain, proxyLocation )
      if not res['OK']:
        return res
      gLogger.debug("Updating environment.")
      os.environ['X509_USER_PROXY'] = res['Value']
      return res
    except Exception, error:
      exStr = "__getConnectionDetails: Failed to get client connection details."
      gLogger.exception( exStr, '', error )
      return S_ERROR(exStr)

  ############################################################
  #
  # These are the methods that are for actual file transfer
  #

  def transfer_toClient( self, fileID, token, fileHelper ):
    """ Method to send files to clients.
        fileID is the local file name in the SE.
        token is used for access rights confirmation.
    """
    file_path = "%s/%s" % ( BASE_PATH, fileID )
    result = fileHelper.getFileDescriptor( file_path, 'r' )
    if not result['OK']:
      result = fileHelper.sendEOF()
      # check if the file does not really exist
      if not os.path.exists(file_path):
        return S_ERROR('File %s does not exist' % os.path.basename(file_path))
      else:
        return S_ERROR('Failed to get file descriptor')

    fileDescriptor = result['Value']
    result = fileHelper.FDToNetwork(fileDescriptor)
    if not result['OK']:
      return S_ERROR('Failed to get file %s' % fileID )
    return result

  def transfer_fromClient( self, fileID, token, fileSize, fileHelper ):
    """ Method to receive file from clients.
        fileID is the local file name in the SE.
        fileSize can be Xbytes or -1 if unknown.
        token is used for access rights confirmation.
    """
    if not self.__checkForDiskSpace( BASE_PATH, fileSize ):
      return S_ERROR('Not enough disk space')

    file_path = "%s/%s" % ( BASE_PATH, fileID )
    if not os.path.exists( os.path.dirname( file_path ) ):
      os.makedirs( os.path.dirname( file_path ) )
    result = fileHelper.getFileDescriptor( file_path, 'w' )
    if not result['OK']:
      return S_ERROR('Failed to get file descriptor')

    fileDescriptor = result['Value']
    result = fileHelper.networkToFD(fileDescriptor)
    if not result['OK']:
      return S_ERROR('Failed to put file %s' % fileID )
    return result

  @staticmethod
  def __checkForDiskSpace( dpath, size ):
    """ Check if the directory dpath can accomodate 'size' volume of data
    """
    dsize = (getDiskSpace(dpath)-1)*1024*1024
    maxStorageSizeBytes = 1024*1024*1024
    return ( min(dsize, maxStorageSizeBytes) > size )

