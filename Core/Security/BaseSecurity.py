
import types
import os
import DIRAC
from DIRAC.Core.Security import Locations

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
    if not maxProxyDaysLifeTime:
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
    cmdEnv["PATH"] = os.environ['PATH']
    cmdEnv["LD_LIBRARY_PATH"] = os.environ['LD_LIBRARY_PATH']
    cmdEnv["X509_USER_KEY"]   = self._secKeyLoc
    cmdEnv["X509_USER_CERT"]  = self._secCertLoc
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