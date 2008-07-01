
import types
import os
import tempfile
import DIRAC
from DIRAC.Core.Security.X509Chain import X509Chain,g_X509ChainType
from DIRAC.Core.Security import Locations, File
from DIRAC import gConfig, S_OK, S_ERROR

class BaseSecurity:

  def __init__( self,
                maxProxyLifeTime = False,
                server = False,
                serverCert = False,
                serverKey = False,
                voName = False,
                timeout = False ):
    if timeout:
      self._secCmdTimeout = timeout
    else:
      self._secCmdTimeout = 30
    if not maxProxyLifeTime:
      self.__maxProxyLifeTime = 604800 # 1week
    else:
      self.__maxProxyLifeTime = maxProxyLifeTime
    if not server:
      self._secServer = gConfig.getValue( "/DIRAC/VOPolicy/MyProxyServer", "myproxy.cern.ch" )
    else:
      self._secServer = server
    if not voName:
      self._secVO = gConfig.getValue( "/DIRAC/VirtualOrganization", "unknown" )
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

  def _getExternalCmdEnvironment( self, noX509 = False ):
    cmdEnv = {}
    keys = ['PATH', 'LD_LIBRARY_PATH']
    if not noX509:
      keys.extend( [ 'X509_USER_KEY', 'X509_USER_CERT' ] )
    for key in keys:
      if key in os.environ:
        cmdEnv[ key ] = os.environ[ key ]
    return cmdEnv

  def _unlinkFiles( self, files ):
    if type( files ) in ( types.ListType, types.TupleType ):
      for file in files:
        self._unlinkFiles( file )
    else:
      try:
        os.unlink( files )
      except:
        pass

  def _generateTemporalFile(self):
    try:
      fd, filename = tempfile.mkstemp()
      os.close(fd)
    except IOError:
      return S_ERROR('Failed to create temporary file')
    return S_OK( filename )

  def _getUsername( self, proxyChain ):
    retVal = proxyChain.getCredentials()
    if not retVal[ 'OK' ]:
      return retVal
    credDict = retVal[ 'Value' ]
    if not credDict[ 'isProxy' ]:
      return S_ERROR( "chain does not contain a proxy" )
    if not credDict[ 'validDN' ]:
      return S_ERROR( "DN %s is not known in dirac" % credDict[ 'subject' ] )
    if not credDict[ 'validGroup' ]:
      return S_ERROR( "Group %s is invalid for DN %s" % ( credDict[ 'group' ], credDict[ 'subject' ] ) )
    mpUsername = "%s:%s" % ( credDict[ 'group' ], credDict[ 'username' ] )
    return S_OK( mpUsername )
