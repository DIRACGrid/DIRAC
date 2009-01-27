"""
This is a service which represents a DISET proxy to the Storage Element component.

This is used to get and put files from a remote storage.
"""
from types import *
import os
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.DataManagementSystem.Service.StorageElementHandler import StorageElementHandler
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

class StorageElementProxyHandler(StorageElementHandler):

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
 
  def __prepareFile(self,pfn,se):
    res = self.__prepareSecurityDetails()
    if not res['OK']:
      return res
    try:
      storageElement = StorageElement(se)
    except AttributeError, x:
      errStr = "prepareFile: Exception while instantiating the Storage Element."
      gLogger.exception(errStr,se,str(x))
      return S_ERROR(errStr)

    res = storageElement.getFile(pfn,"%s/getFile" % base_path,True)
    print res
    if not res['OK']:
      gLogger.error("prepareFile: Failed to get local copy of file.",res['Message'])
      return res
    return S_OK()

  types_prepareFile = [StringType,StringType]
  def export_prepareFile(self, pfn, se):
    """ This method simply gets the file to the local storage area
    """
    res = self.__prepareFile(pfn,se)
    return res
