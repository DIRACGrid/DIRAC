
import types
import os
import DIRAC
import DIRAC.Core.Security.Locations
from DIRAC.Core.Security.X509Chain import X509Chain,g_X509ChainType
from DIRAC.Core.Security import Locations
from DIRAC import gConfig, S_OK, S_ERROR

class BaseSecurity:

  def __init__( self,
                maxProxyHoursLifeTime = False,
                server = False,
                serverCert = False,
                serverKey = False,
                voName = False,
                timeout = False ):
    if timeout:
      self._secCmdTimeout = timeout
    else:
      self._secCmdTimeout = 0
    if not maxProxyHoursLifeTime:
      self.__maxProxyLifeTime = 7 * 24
    else:
      self.__maxProxyLifeTime = maxProxyHoursLifeTime
    if not server:
      self._secServer = "myproxy.cern.ch"
    else:
      self._secServer = server
    if not voName:
      self._secVO = gConfig.getValue( "/DIRAC/VirtualOrganization", "" )
    else:
      self._secVO = voName
    ckLoc = Locations.getHostCertificateAndKeyLocation()
    if serverCert:
      self._secCertLoc = serverCert
    else:
      if ckLoc:
        self._secCertLoc = ckLoc[0]
      else:
        self._secCertLoc = "%s/etc/grid-security/servercert.pem" % DIRAC.rootPath
    if serverKey:
      self._secKeyLoc = serverKey
    else:
      if ckLoc:
        self._secKeyLoc = ckLoc[1]
      else:
        self._secKeyLoc = "%s/etc/grid-security/serverkey.pem" % DIRAC.rootPath

  def _getExternalCmdEnvironment(self):
    cmdEnv = {}
    for key in ( 'PATH', 'LD_LIBRARY_PATH', 'X509_USER_KEY', 'X509_USER_CERT' ):
      if key in os.environ:
        cmdEnv[ key ] = os.environ[ key ]
    return cmdEnv

  def _unlinkFiles( files ):
    if type( files ) in ( types.ListType, types.TupleType ):
      for file in files:
        self._unlinkFiles( file )
    else:
      try:
        os.unlink( files )
      except:
        pass

  def _loadProxy( self, proxy = False ):
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
      retVal = File.writeChainToTemporaryFile( proxy )
      if not retVal[ 'OK' ]:
        return retVal
      proxyLoc = retVal[ 'Value' ]
    else:
      if not proxy:
        proxyLoc = Locations.getProxyLocation()
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
