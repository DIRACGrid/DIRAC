""" Base class for MyProxy and VOMS
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import tempfile

import DIRAC
from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Security import Locations


class BaseSecurity(object):

  def __init__(self,
               server=False,
               serverCert=False,
               serverKey=False,
               timeout=False):
    if timeout:
      self._secCmdTimeout = timeout
    else:
      self._secCmdTimeout = 30
    if not server:
      self._secServer = gConfig.getValue("/DIRAC/VOPolicy/MyProxyServer", "myproxy.cern.ch")
    else:
      self._secServer = server
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
    self._secRunningFromTrustedHost = gConfig.getValue(
        "/DIRAC/VOPolicy/MyProxyTrustedHost",
        "True").lower() in (
        "y",
        "yes",
        "true")
    self._secMaxProxyHours = gConfig.getValue("/DIRAC/VOPolicy/MyProxyMaxDelegationTime", 168)

  def getMyProxyServer(self):
    return self._secServer

  def getServiceDN(self):
    chain = X509Chain()
    retVal = chain.loadChainFromFile(self._secCertLoc)
    if not retVal['OK']:
      return retVal
    return chain.getCertInChain(0)['Value'].getSubjectDN()

  def _getExternalCmdEnvironment(self):
    return dict(os.environ)

  def _unlinkFiles(self, files):
    if isinstance(files, (list, tuple)):
      for fileName in files:
        self._unlinkFiles(fileName)
    else:
      try:
        os.unlink(files)
      except Exception:
        pass

  def _generateTemporalFile(self):
    try:
      fd, filename = tempfile.mkstemp()
      os.close(fd)
    except IOError:
      return S_ERROR(DErrno.ECTMPF)
    return S_OK(filename)

  def _getUsername(self, proxyChain):
    retVal = proxyChain.getCredentials()
    if not retVal['OK']:
      return retVal
    credDict = retVal['Value']
    if not credDict['isProxy']:
      return S_ERROR(DErrno.EX509, "chain does not contain a proxy")
    if not credDict['validDN']:
      return S_ERROR(DErrno.EDISET, "DN %s is not known in dirac" % credDict['subject'])
    if not credDict['validGroup']:
      return S_ERROR(DErrno.EDISET, "Group %s is invalid for DN %s" % (credDict['group'], credDict['subject']))
    mpUsername = "%s:%s" % (credDict['group'], credDict['username'])
    return S_OK(mpUsername)
