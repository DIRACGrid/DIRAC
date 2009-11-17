"""
This is a service which represents a DISET proxy to the LCG File Catalog
"""
__RCSID__ = "$Id$"

from DIRAC                                          import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                import RequestHandler
from DIRAC.Resources.Catalog.FileCatalog            import FileCatalog
from DIRAC.Core.Utilities.Shifter                   import setupShifterProxyInEnv
from types import *

base_path = ''

def initializeLcgFileCatalogProxyHandler(serviceInfo):
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

class LcgFileCatalogProxyHandler(RequestHandler):

  types_callProxyMethod = [StringType,TupleType,DictionaryType]
  def export_callProxyMethod(self, name, args, kargs):
    """ A generic method to call methods of the Storage Element.
    """
    res = pythonCall(0,self.__proxyWrapper,name,args,kargs)
    if res['OK']:
      return res['Value']
    else:
      return res

  def __proxyWrapper(self,name,args,kargs):
    """ The wrapper will obtain the client proxy and set it up in the environment.
    
        The required functionality is then executed and returned to the client.
    """
    res = self.__prepareSecurityDetails()
    if not res['OK']:
      return res
    try:
      fileCatalog = FileCatalog(['LcgFileCatalogCombined'])
      method = getattr(fileCatalog,name)
    except AttributeError, x:
      errStr = "LcgFileCatalogProxyHandler.__proxyWrapper: No method named %s" % name
      gLogger.exception(errStr,name,x)
      return S_ERROR(error)
    try:
      result = method(*args,**kargs)
      return result
    except Exception,x:
      errStr = "LcgFileCatalogProxyHandler.__proxyWrapper: Exception while performing %s" % name"
      gLogger.exception(errStr,name,x)
      return S_ERROR(errStr)

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
