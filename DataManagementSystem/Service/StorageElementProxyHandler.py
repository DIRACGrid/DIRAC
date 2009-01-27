"""
This is a service which represents a DISET proxy to the Storage Element component.

This is used to get and put files from a remote storage.
"""
from types import *
import os,shutil
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Utilities.Subprocess import pythonCall
from DIRAC.Core.Utilities.Os import getDiskSpace, getDirectorySize
base_path = ''

def initializeStorageElementProxyHandler(serviceInfo):
  global base_path
  cfgPath = serviceInfo['serviceSectionPath']
  res = gConfig.getOption( "%s/BasePath" % cfgPath )
  if res['OK']:
    base_path =  res['Value']
    gLogger.info('The base path obtained is %s. Checking its existence...' % base_path)
    if not os.path.exists(base_path):
      gLogger.info('%s did not exist. Creating....' % base_path)
      os.makedirs(base_path)
  else:
    gLogger.error('Failed to get the base path')
    return S_ERROR('Failed to get the base path')
  return S_OK()

class StorageElementProxyHandler(RequestHandler):

  types_callProxyMethod = [StringType,StringType,ListType,DictType]
  def export_callProxyMethod(self, se, name, args, kargs):
    """ A generic method to call methods of the Storage Element.
    """
    res = pythonCall(0,self.__proxyWrapper,se,name,args,kargs)
    if res['OK']:
      return res['Value']   
    else:
      return res

  def __proxyWrapper(self,se,name,args,kargs):
    """ The wrapper will obtain the client proxy and set it up in the environment.

        The required functionality is then executed and returned to the client.
    """
    res = self.__prepareSecurityDetails()
    if not res['OK']:
      return res
    try:
      storageElement = StorageElement(se)
      method = getattr(storageElement,name)
    except AttributeError, x:
      exStr = "Exception: no such method."
      gLogger.exception(exStr,name,x)
      return S_ERROR(error)
    result = method(*args,**kargs)
    return result

  types_uploadFile = [StringType,StringType]
  def export_uploadFile(self,se,pfn):
    """ This method uploads a file present in the local cache to the specified storage element
    """
    res = pythonCall(0,self.__uploadFile,se,pfn)
    if res['OK']:
      return res['Value']
    else:
      return res

  def __uploadFile(self, se, pfn):
    res = self.__prepareSecurityDetails()
    if not res['OK']:
      return res

    # Put file to the SE
    try:
      storageElement = StorageElement(se)
    except AttributeError, x:
      errStr = "__uploadFile: Exception while instantiating the Storage Element."
      gLogger.exception(errStr,se,str(x))
      return S_ERROR(errStr)
    putFileDir = "%s/putFile" % base_path
    localFileName = "%s/%s" % (putFileDir,os.path.basename(pfn))
    res = storageElement.putFile({pfn:localFileName},True)
    if not res['OK']:
      gLogger.error("prepareFile: Failed to put local file to storage.",res['Message'])
    # Clear the local cache
    try:
      shutil.rmtree(putFileDir)
      gLogger.debug("Cleared existing putFile cache")
    except Exception, x:
      gLogger.exception("Failed to remove source dir.",getFileDir,x)
    return res

  types_prepareFile = [StringType,StringType]
  def export_prepareFile(self, se, pfn):
    """ This method simply gets the file to the local storage area
    """
    res = pythonCall(0,self.__prepareFile,se,pfn)
    if res['OK']:
      return res['Value']
    else:
      return res

  def __prepareFile(self,se,pfn):
    res = self.__prepareSecurityDetails()
    if not res['OK']:
      return res
  
    # Clear the local cache
    getFileDir = "%s/getFile" % base_path
    if os.path.exists(getFileDir):
      try:
        shutil.rmtree(getFileDir)
        gLogger.debug("Cleared existing getFile cache")
      except Exception, x:
        gLogger.exception("Failed to remove destination directory.",getFileDir,x)
   
    # Get the file to the cache 
    try:
      storageElement = StorageElement(se)
    except AttributeError, x:
      errStr = "prepareFile: Exception while instantiating the Storage Element."
      gLogger.exception(errStr,se,str(x))
      return S_ERROR(errStr)
    res = storageElement.getFile(pfn,"%s/getFile" % base_path,True)
    if not res['OK']:
      gLogger.error("prepareFile: Failed to get local copy of file.",res['Message'])
      return res
    return S_OK()

  ############################################################
  #
  # This is the method to setup the proxy and configure the environment with the client credential
  #

  def __prepareSecurityDetails(self):
    """ Obtains the connection details for the client
    """
    try:
      clientDN = self._clientTransport.peerCredentials['DN']
      clientUsername = self._clientTransport.peerCredentials['username']
      clientGroup = self._clientTransport.peerCredentials['group']
      gLogger.debug( "Getting proxy for %s@%s (%s)" % (clientUsername,clientGroup,clientDN) )
      res = gProxyManager.downloadVOMSProxy(clientDN, clientGroup)
      if not res['OK']:
        return res
      chain = res['Value']
      proxyBase = "%s/proxies" % base_path
      if not os.path.exists(proxyBase):
        os.makedirs(proxyBase)
      proxyLocation = "%s/proxies/%s-%s" % (base_path,clientUsername,clientGroup)
      gLogger.debug("Obtained proxy chain, dumping to %s." % proxyLocation)
      res = gProxyManager.dumpProxyToFile(chain,proxyLocation)
      if not res['OK']:
        return res
      gLogger.debug("Updating environment.")
      os.environ['X509_USER_PROXY'] = res['Value']
      return res
    except Exception,x:
      exStr = "__getConnectionDetails: Failed to get client connection details."
      gLogger.exception(exStr,'',x)
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
    file_path = "%s/%s" % (base_path,fileID)
    result = fileHelper.getFileDescriptor(file_path,'r')
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
      return S_ERROR('Failed to get file '+fileID)
    else:
      return result

  def transfer_fromClient( self, fileID, token, fileSize, fileHelper ):
    """ Method to receive file from clients.
        fileID is the local file name in the SE.
        fileSize can be Xbytes or -1 if unknown.
        token is used for access rights confirmation.
    """
    if not self.__checkForDiskSpace(base_path,fileSize):
      return S_ERROR('Not enough disk space')

    file_path = "%s/%s" % (base_path,fileID)
    if not os.path.exists(os.path.dirname(file_path)):
      os.makedirs(os.path.dirname(file_path))
    result = fileHelper.getFileDescriptor(file_path,'w')
    if not result['OK']:
      return S_ERROR('Failed to get file descriptor')

    fileDescriptor = result['Value']
    result = fileHelper.networkToFD(fileDescriptor)
    if not result['OK']:
      return S_ERROR('Failed to put file '+fileID)
    else:
      return result

  def __checkForDiskSpace(self,dpath,size):
    """ Check if the directory dpath can accomodate 'size' volume of data
    """
    dsize = (getDiskSpace(dpath)-1)*1024*1024
    maxStorageSizeBytes = 1024*1024*1024
    return ( min(dsize,maxStorageSizeBytes) > size )
