########################################################################
# File :    ProxyUpload.py
# Author :  Adrian Casajus
########################################################################

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import sys
from prompt_toolkit import prompt

import DIRAC
from DIRAC import gLogger, S_ERROR
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager


class ProxyUpload(DIRACScript):

  def initParameters(self):
    self.rfc = False
    self.keyLoc = False
    self.certLoc = False
    self.proxyLoc = False
    self.onTheFly = False
    self.userPasswd = ""
    self.stdinPasswd = False
    self.proxyLifeTime = 2592000
    self.proxyUploadSwitches = [
        ("v:", "valid=", "Valid HH:MM for the proxy. By default is one month", self.setProxyLifeTime),
        ("C:", "Cert=", "File to use as user certificate", self.setCertLocation),
        ("K:", "Key=", "File to use as user key", self.setKeyLocation),
        ("P:", "Proxy=", "File to use as proxy", self.setProxyLocation),
        ("f", "onthefly", "Generate a proxy on the fly", self.setOnTheFly),
        ("p", "pwstdin", "Get passwd from stdin", self.setStdinPasswd),
        ("i", "version", "Print version", self.showVersion)
    ]

  def __str__(self):
    data = []
    for k in ('proxyLifeTime', 'certLoc', 'keyLoc', 'proxyLoc',
              'onTheFly', 'stdinPasswd', 'userPasswd'):
      if k == 'userPasswd':
        data.append("userPasswd = *****")
      else:
        data.append("%s=%s" % (k, getattr(self, k)))
    msg = "<UploadCLIParams %s>" % " ".join(data)
    return msg

  def setProxyLifeTime(self, arg):
    try:
      fields = [f.strip() for f in arg.split(":")]
      self.proxyLifeTime = int(fields[0]) * 3600 + int(fields[1]) * 60
    except ValueError:
      gLogger.notice("Can't parse %s time! Is it a HH:MM?" % arg)
      return DIRAC.S_ERROR("Can't parse time argument")
    return DIRAC.S_OK()

  def setProxyRemainingSecs(self, arg):
    self.proxyLifeTime = int(arg)
    return DIRAC.S_OK()

  def getProxyLifeTime(self):
    hours = int(self.proxyLifeTime / 3600)
    mins = int(self.proxyLifeTime / 60 - hours * 60)
    return "%s:%s" % (hours, mins)

  def getProxyRemainingSecs(self):
    return self.proxyLifeTime

  def setCertLocation(self, arg):
    self.certLoc = arg
    return DIRAC.S_OK()

  def setKeyLocation(self, arg):
    self.keyLoc = arg
    return DIRAC.S_OK()

  def setProxyLocation(self, arg):
    self.proxyLoc = arg
    return DIRAC.S_OK()

  def setOnTheFly(self, arg):
    self.onTheFly = True
    return DIRAC.S_OK()

  def setStdinPasswd(self, arg):
    self.stdinPasswd = True
    return DIRAC.S_OK()

  def showVersion(self, arg):
    gLogger.notice("Version:")
    gLogger.notice(" ", __RCSID__)
    sys.exit(0)
    return DIRAC.S_OK()

  def uploadProxy(self):
    DIRAC.gLogger.info("Loading user proxy")
    proxyLoc = self.proxyLoc
    if not proxyLoc:
      proxyLoc = Locations.getDefaultProxyLocation()
    if not proxyLoc:
      return S_ERROR("Can't find any proxy")

    if self.onTheFly:
      DIRAC.gLogger.info("Uploading proxy on-the-fly")
      certLoc = self.certLoc
      keyLoc = self.keyLoc
      if not certLoc or not keyLoc:
        cakLoc = Locations.getCertificateAndKeyLocation()
        if not cakLoc:
          return S_ERROR("Can't find user certificate and key")
        if not certLoc:
          certLoc = cakLoc[0]
        if not keyLoc:
          keyLoc = cakLoc[1]

      DIRAC.gLogger.info("Cert file %s" % certLoc)
      DIRAC.gLogger.info("Key file  %s" % keyLoc)

      testChain = X509Chain()
      retVal = testChain.loadKeyFromFile(keyLoc, password=self.userPasswd)
      if not retVal['OK']:
        if self.stdinPasswd:
          userPasswd = sys.stdin.readline().strip("\n")
        else:
          try:
            userPasswd = prompt(u"Enter Certificate password: ", is_password=True)
          except KeyboardInterrupt:
            return S_ERROR("Caught KeyboardInterrupt, exiting...")
        self.userPasswd = userPasswd

      DIRAC.gLogger.info("Loading cert and key")
      chain = X509Chain()
      # Load user cert and key
      retVal = chain.loadChainFromFile(certLoc)
      if not retVal['OK']:
        return S_ERROR("Can't load %s" % certLoc)
      retVal = chain.loadKeyFromFile(keyLoc, password=self.userPasswd)
      if not retVal['OK']:
        return S_ERROR("Can't load %s" % keyLoc)
      DIRAC.gLogger.info("User credentials loaded")
      restrictLifeTime = self.proxyLifeTime

    else:
      proxyChain = X509Chain()
      retVal = proxyChain.loadProxyFromFile(proxyLoc)
      if not retVal['OK']:
        return S_ERROR("Can't load proxy file %s: %s" % (self.proxyLoc, retVal['Message']))

      chain = proxyChain
      restrictLifeTime = 0

    DIRAC.gLogger.info(" Uploading...")
    return gProxyManager.uploadProxy(proxy=chain, restrictLifeTime=restrictLifeTime, rfcIfPossible=self.rfc)
