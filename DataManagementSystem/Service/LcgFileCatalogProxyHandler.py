########################################################################
# $HeadURL $
# File: LcgFileCatalogProxyHandler.py
########################################################################

""" :mod: LcgFileCatalogProxyHandler 
    ================================
 
    .. module: LcgFileCatalogProxyHandler
    :synopsis: This is a service which represents a DISET proxy to the LCG File Catalog    
"""
## imports
import os
from types import StringType, DictType, TupleType
## from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.Subprocess import pythonCall
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

__RCSID__ = "$Id$"

def initializeLcgFileCatalogProxyHandler( _serviceInfo ):
  """ service initalisation """
  return S_OK()

class LcgFileCatalogProxyHandler( RequestHandler ):
  """
  .. class:: LcgFileCatalogProxyHandler
  """

  types_callProxyMethod = [ StringType, TupleType, DictType ]
  def export_callProxyMethod( self, name, args, kargs ):
    """ A generic method to call methods of the Storage Element.
    """
    res = pythonCall( 120, self.__proxyWrapper, name, args, kargs )
    if res['OK']:
      return res['Value']
    else:
      return res

  def __proxyWrapper( self, name, args, kwargs ):
    """ The wrapper will obtain the client proxy and set it up in the environment.
        The required functionality is then executed and returned to the client.

    :param self: self reference
    :param str name: fcn name
    :param tuple args: fcn args
    :param dict kwargs: fcn keyword args 
    """
    res = self.__prepareSecurityDetails()
    if not res['OK']:
      return res
    try:
      fileCatalog = FileCatalog( ['LcgFileCatalogCombined'] )
      method = getattr( fileCatalog, name )
    except AttributeError, error:
      errStr = "LcgFileCatalogProxyHandler.__proxyWrapper: No method named %s" % name
      gLogger.exception( errStr, name, error )
      return S_ERROR( errStr )
    try:
      result = method( *args, **kwargs )
      return result
    except Exception, error:
      errStr = "LcgFileCatalogProxyHandler.__proxyWrapper: Exception while performing %s" % name
      gLogger.exception( errStr, name, error )
      return S_ERROR( errStr )

  def __prepareSecurityDetails( self ):
    """ Obtains the connection details for the client """
    try:
      credDict = self.getRemoteCredentials()
      clientDN = credDict[ 'DN' ]
      clientUsername = credDict['username']
      clientGroup = credDict['group']
      gLogger.debug( "Getting proxy for %s@%s (%s)" % ( clientUsername, clientGroup, clientDN ) )
      res = gProxyManager.downloadVOMSProxy( clientDN, clientGroup )
      if not res['OK']:
        return res
      chain = res['Value']
      proxyBase = "/tmp/proxies"
      if not os.path.exists( proxyBase ):
        os.makedirs( proxyBase )
      proxyLocation = "%s/%s-%s" % ( proxyBase, clientUsername, clientGroup )
      gLogger.debug( "Obtained proxy chain, dumping to %s." % proxyLocation )
      res = gProxyManager.dumpProxyToFile( chain, proxyLocation )
      if not res['OK']:
        return res
      gLogger.debug( "Updating environment." )
      os.environ['X509_USER_PROXY'] = res['Value']
      return res
    except Exception, error:
      exStr = "__getConnectionDetails: Failed to get client connection details."
      gLogger.exception( exStr, '', error )
      return S_ERROR( exStr )
