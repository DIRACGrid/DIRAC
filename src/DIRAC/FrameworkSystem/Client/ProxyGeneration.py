########################################################################
# File :   ProxyGeneration.py
# Author : Adrian Casajus
########################################################################
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

__RCSID__ = "$Id$"

import sys
from prompt_toolkit import prompt

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Utilities.NTP import getClockDeviation
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


class ProxyGeneration(DIRACScript):

  def initParameters(self):
    self.rfc = True
    self.keyLoc = False
    self.strict = False
    self.summary = False
    self.certLoc = False
    self.proxyLoc = False
    self.checkClock = True
    self.diracGroup = False
    self.userPasswd = ""
    self.stdinPasswd = False
    self.checkWithCS = True
    self.limitedProxy = False
    self.proxyLifeTime = 86400
    self.proxyStrength = 1024
    self.embedDefaultGroup = True

    self.proxyGenerationSwitches = [
        ("v:", "valid=", "Valid HH:MM for the proxy. By default is 24 hours", self.setProxyLifeTime),
        ("g:", "group=", "DIRAC Group to embed in the proxy", self.setDIRACGroup),
        ("b:", "strength=", "Set the proxy strength in bytes", self.setProxyStrength),
        ("l", "limited", "Generate a limited proxy", self.setProxyLimited),
        ("t", "strict", "Fail on each error. Treat warnings as errors.", self.setStrict),
        ("S", "summary", "Enable summary output when generating proxy", self.setSummary),
        ("C:", "Cert=", "File to use as user certificate", self.setCertLocation),
        ("K:", "Key=", "File to use as user key", self.setKeyLocation),
        ("u:", "out=", "File to write as proxy", self.setProxyLocation),
        ("x", "nocs", "Disable CS check", self.setDisableCSCheck),
        ("p", "pwstdin", "Get passwd from stdin", self.setStdinPasswd),
        ("i", "version", "Print version", self.showVersion),
        ("j", "noclockcheck", "Disable checking if time is ok", self.disableClockCheck),
        ("r", "rfc", "Create an RFC proxy, true by default, deprecated flag", self.setRFC),
        ("L", "legacy", "Create a legacy non-RFC proxy", self.setNoRFC)
    ]

  def setProxyLifeTime(self, arg):
    """ Set proxy lifetime

        :param str arg: arguments

        :return: S_OK()/S_ERROR()
    """
    try:
      fields = [f.strip() for f in arg.split(":")]
      self.proxyLifeTime = int(fields[0]) * 3600 + int(fields[1]) * 60
    except Exception:
      gLogger.error("Can't parse time! Is it a HH:MM?", arg)
      return S_ERROR("Can't parse time argument")
    return S_OK()

  def setRFC(self, _arg):
    """ Set RFC

        :param _arg: unuse

        :return: S_OK()
    """
    self.rfc = True
    return S_OK()

  def setNoRFC(self, _arg):
    """ Unset RFC

        :param _arg: unuse

        :return: S_OK()
    """
    self.rfc = False
    return S_OK()

  def setProxyRemainingSecs(self, arg):
    """ Set proxy lifetime

        :param int arg: lifetime in seconds

        :return: S_OK()
    """
    self.proxyLifeTime = int(arg)
    return S_OK()

  def getProxyLifeTime(self):
    """ Get proxy lifetime

        :return: str
    """
    hours = int(self.proxyLifeTime / 3600)
    mins = int(self.proxyLifeTime / 60 - hours * 60)
    return "%s:%s" % (hours, mins)

  def getProxyRemainingSecs(self):
    """ Get proxy livetime

        :return: int
    """
    return self.proxyLifeTime

  def setDIRACGroup(self, arg):
    """ Set DIRAC group

        :param str arg: arguments

        :return: S_OK()
    """
    self.diracGroup = arg
    return S_OK()

  def getDIRACGroup(self):
    """ Get DIRAC group

        :return: str
    """
    return self.diracGroup

  def setProxyStrength(self, arg):
    """ Get proxy strength

        :return: S_OK()
    """
    try:
      self.proxyStrength = int(arg)
    except Exception:
      gLogger.error("Can't parse bits! Is it a number?", '%s' % arg)
      return S_ERROR("Can't parse strength argument")
    return S_OK()

  def setProxyLimited(self, _arg):
    """ Set proxy limited

        :param _arg: unuse

        :return: str
    """
    self.limitedProxy = True
    return S_OK()

  def setSummary(self, _arg):
    """ Set proxy limited

        :param _arg: unuse

        :return: str
    """
    gLogger.info("Enabling summary output")
    self.summary = True
    return S_OK()

  def setCertLocation(self, arg):
    """ Set certificate path

        :param str arg: certificate path

        :return: S_OK()
    """
    self.certLoc = arg
    return S_OK()

  def setKeyLocation(self, arg):
    """ Set key path

        :param str arg: key path

        :return: S_OK()
    """
    self.keyLoc = arg
    return S_OK()

  def setProxyLocation(self, arg):
    """ Set proxy path

        :param str arg: proxy path

        :return: S_OK()
    """
    self.proxyLoc = arg
    return S_OK()

  def setDisableCSCheck(self, _arg):
    """ Disable CS check

        :param _arg: unuse

        :return: S_OK()
    """
    self.checkWithCS = False
    return S_OK()

  def setStdinPasswd(self, _arg):
    """ Set stdin passwd

        :param _arg: unuse

        :return: S_OK()
    """
    self.stdinPasswd = True
    return S_OK()

  def setStrict(self, _arg):
    """ Set strict

        :param _arg: unuse

        :return: S_OK()
    """
    self.strict = True
    return S_OK()

  def showVersion(self, _arg):
    """ Show version

        :param _arg: unuse

        :return: S_OK()
    """
    gLogger.always("Version: %s" % __RCSID__)
    sys.exit(0)
    return S_OK()

  def disableClockCheck(self, _arg):
    """ Disable clock check

        :param _arg: unuse

        :return: S_OK()
    """
    self.checkClock = False
    return S_OK()

  def generateProxy(self):
    """ Generate proxy

        :return: S_OK()/S_ERROR()
    """
    if self.checkClock:
      result = getClockDeviation()
      if result['OK']:
        deviation = result['Value']
        if deviation > 600:
          gLogger.error("Your host clock seems to be off by more than TEN MINUTES! Thats really bad.")
          gLogger.error("We're cowardly refusing to generate a proxy. Please fix your system time")
          sys.exit(1)
        elif deviation > 180:
          gLogger.error("Your host clock seems to be off by more than THREE minutes! Thats bad.")
          gLogger.notice("We'll generate the proxy but please fix your system time")
        elif deviation > 60:
          gLogger.error("Your host clock seems to be off by more than a minute! Thats not good.")
          gLogger.notice("We'll generate the proxy but please fix your system time")

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
    self.certLoc = certLoc
    self.keyLoc = keyLoc

    # Load password
    testChain = X509Chain()
    retVal = testChain.loadChainFromFile(self.certLoc)
    if not retVal['OK']:
      return S_ERROR("Cannot load certificate %s: %s" % (self.certLoc, retVal['Message']))
    timeLeft = int(testChain.getRemainingSecs()['Value'] / 86400)
    if timeLeft < 30:
      gLogger.notice("\nYour certificate will expire in %d days. Please renew it!\n" % timeLeft)

    # First try reading the key from the file
    retVal = testChain.loadKeyFromFile(self.keyLoc, password=self.userPasswd)  # XXX why so commented?
    if not retVal['OK']:
      if self.stdinPasswd:
        userPasswd = sys.stdin.readline().strip("\n")
      else:
        try:
          userPasswd = prompt(u"Enter Certificate password: ", is_password=True)
        except KeyboardInterrupt:
          return S_ERROR("Caught KeyboardInterrupt, exiting...")
      self.userPasswd = userPasswd

    # Find location
    proxyLoc = self.proxyLoc
    if not proxyLoc:
      proxyLoc = Locations.getDefaultProxyLocation()

    chain = X509Chain()
    # Load user cert and key
    retVal = chain.loadChainFromFile(certLoc)
    if not retVal['OK']:
      gLogger.warn(retVal['Message'])
      return S_ERROR("Can't load %s" % certLoc)
    retVal = chain.loadKeyFromFile(keyLoc, password=self.userPasswd)
    if not retVal['OK']:
      gLogger.warn(retVal['Message'])
      if 'bad decrypt' in retVal['Message'] or 'bad pass phrase' in retVal['Message']:
        return S_ERROR("Bad passphrase")
      return S_ERROR("Can't load %s" % keyLoc)

    if self.checkWithCS:
      retVal = chain.generateProxyToFile(proxyLoc,
                                         self.proxyLifeTime,
                                         strength=self.proxyStrength,
                                         limited=self.limitedProxy,
                                         rfc=self.rfc)

      gLogger.info("Contacting CS...")
      retVal = self.enableCS()
      if not retVal['OK']:
        gLogger.warn(retVal['Message'])
        if 'Unauthorized query' in retVal['Message']:
          # add hint for users
          return S_ERROR("Can't contact DIRAC CS: %s (User possibly not registered with dirac server) "
                         % retVal['Message'])
        return S_ERROR("Can't contact DIRAC CS: %s" % retVal['Message'])
      userDN = chain.getCertInChain(-1)['Value'].getSubjectDN()['Value']

      if not self.diracGroup:
        result = Registry.findDefaultGroupForDN(userDN)
        if not result['OK']:
          gLogger.warn("Could not get a default group for DN %s: %s" % (userDN, result['Message']))
        else:
          self.diracGroup = result['Value']
          gLogger.info("Default discovered group is %s" % self.diracGroup)
      gLogger.info("Checking DN %s" % userDN)
      retVal = Registry.getUsernameForDN(userDN)
      if not retVal['OK']:
        gLogger.warn(retVal['Message'])
        return S_ERROR("DN %s is not registered" % userDN)
      username = retVal['Value']
      gLogger.info("Username is %s" % username)
      retVal = Registry.getGroupsForUser(username)
      if not retVal['OK']:
        gLogger.warn(retVal['Message'])
        return S_ERROR("User %s has no groups defined" % username)
      groups = retVal['Value']
      if self.diracGroup not in groups:
        return S_ERROR("Requested group %s is not valid for DN %s" % (self.diracGroup, userDN))
      gLogger.info("Creating proxy for %s@%s (%s)" % (username, self.diracGroup, userDN))
    if self.summary:
      h = int(self.proxyLifeTime / 3600)
      m = int(self.proxyLifeTime / 60) - h * 60
      gLogger.notice("Proxy lifetime will be %02d:%02d" % (h, m))
      gLogger.notice("User cert is %s" % certLoc)
      gLogger.notice("User key  is %s" % keyLoc)
      gLogger.notice("Proxy will be written to %s" % proxyLoc)
      if self.diracGroup:
        gLogger.notice("DIRAC Group will be set to %s" % self.diracGroup)
      else:
        gLogger.notice("No DIRAC Group will be set")
      gLogger.notice("Proxy strength will be %s" % self.proxyStrength)
      if self.limitedProxy:
        gLogger.notice("Proxy will be limited")
    retVal = chain.generateProxyToFile(proxyLoc,
                                       self.proxyLifeTime,
                                       self.diracGroup,
                                       strength=self.proxyStrength,
                                       limited=self.limitedProxy,
                                       rfc=self.rfc)
    if not retVal['OK']:
      gLogger.warn(retVal['Message'])
      return S_ERROR("Couldn't generate proxy: %s" % retVal['Message'])
    return S_OK(proxyLoc)
