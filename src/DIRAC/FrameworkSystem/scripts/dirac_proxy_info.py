#!/usr/bin/env python
########################################################################
# File :    dirac-proxy-info.py
# Author :  Adrian Casajus
########################################################################
"""
Print information about the current proxy.

Example:
  $ dirac-proxy-info
  subject      : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar/CN=proxy/CN=proxy
  issuer       : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar/CN=proxy
  identity     : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
  timeleft     : 23:53:55
  DIRAC group  : dirac_user
  path         : /tmp/x509up_u40885
  username     : vhamar
  VOMS         : True
  VOMS fqan    : ['/formation']
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import sys

from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Utilities.ReturnValues import S_OK


class Params(object):

  proxyLoc = False
  vomsEnabled = True
  csEnabled = True
  steps = False
  checkValid = False
  checkClock = True
  uploadedInfo = False

  def showVersion(self, arg):
    print("Version:")
    print(" ", __RCSID__)
    sys.exit(0)
    return S_OK()

  def setProxyLocation(self, arg):
    self.proxyLoc = arg
    return S_OK()

  def disableVOMS(self, arg):
    self.vomsEnabled = False
    return S_OK()

  def disableCS(self, arg):
    self.csEnabled = False
    return S_OK()

  def showSteps(self, arg):
    self.steps = True
    return S_OK()

  def validityCheck(self, arg):
    self.checkValid = True
    return S_OK()

  def disableClockCheck(self, arg):
    self.checkClock = False
    return S_OK()

  def setManagerInfo(self, arg):
    self.uploadedInfo = True
    return S_OK()


@DIRACScript()
def main():
  params = Params()

  from DIRAC.Core.Base import Script
  Script.registerSwitch("f:", "file=", "File to use as user key", params.setProxyLocation)
  Script.registerSwitch("i", "version", "Print version", params.showVersion)
  Script.registerSwitch("n", "novoms", "Disable VOMS", params.disableVOMS)
  Script.registerSwitch("v", "checkvalid", "Return error if the proxy is invalid", params.validityCheck)
  Script.registerSwitch("x", "nocs", "Disable CS", params.disableCS)
  Script.registerSwitch("e", "steps", "Show steps info", params.showSteps)
  Script.registerSwitch("j", "noclockcheck", "Disable checking if time is ok", params.disableClockCheck)
  Script.registerSwitch("m", "uploadedinfo", "Show uploaded proxies info", params.setManagerInfo)

  Script.disableCS()
  Script.parseCommandLine()

  from DIRAC.Core.Utilities.NTP import getClockDeviation
  from DIRAC import gLogger
  from DIRAC.Core.Security.ProxyInfo import getProxyInfo, getProxyStepsInfo
  from DIRAC.Core.Security.ProxyInfo import formatProxyInfoAsString, formatProxyStepsInfoAsString
  from DIRAC.Core.Security import VOMS
  from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
  from DIRAC.ConfigurationSystem.Client.Helpers import Registry

  if params.csEnabled:
    retVal = Script.enableCS()
    if not retVal['OK']:
      print("Cannot contact CS to get user list")

  if params.checkClock:
    result = getClockDeviation()
    if result['OK']:
      deviation = result['Value']
      if deviation > 600:
        gLogger.error("Your host clock seems to be off by more than TEN MINUTES! Thats really bad.")
      elif deviation > 180:
        gLogger.error("Your host clock seems to be off by more than THREE minutes! Thats bad.")
      elif deviation > 60:
        gLogger.error("Your host clock seems to be off by more than a minute! Thats not good.")

  result = getProxyInfo(params.proxyLoc, not params.vomsEnabled)
  if not result['OK']:
    gLogger.error(result['Message'])
    sys.exit(1)
  infoDict = result['Value']
  gLogger.notice(formatProxyInfoAsString(infoDict))
  if not infoDict['isProxy']:
    gLogger.error('==============================\n!!! The proxy is not valid !!!')

  if params.steps:
    gLogger.notice("== Steps extended info ==")
    chain = infoDict['chain']
    stepInfo = getProxyStepsInfo(chain)['Value']
    gLogger.notice(formatProxyStepsInfoAsString(stepInfo))

  def invalidProxy(msg):
    gLogger.error("Invalid proxy:", msg)
    sys.exit(1)

  if params.uploadedInfo:
    result = gProxyManager.getUserProxiesInfo()
    if not result['OK']:
      gLogger.error("Could not retrieve the uploaded proxies info", result['Message'])
    else:
      uploadedInfo = result['Value']
      if not uploadedInfo:
        gLogger.notice("== No proxies uploaded ==")
      if uploadedInfo:
        gLogger.notice("== Proxies uploaded ==")
        maxDNLen = 0
        maxGroupLen = 0
        for userDN in uploadedInfo:
          maxDNLen = max(maxDNLen, len(userDN))
          for group in uploadedInfo[userDN]:
            maxGroupLen = max(maxGroupLen, len(group))
        gLogger.notice(" %s | %s | Until (GMT)" % ("DN".ljust(maxDNLen), "Group".ljust(maxGroupLen)))
        for userDN in uploadedInfo:
          for group in uploadedInfo[userDN]:
            gLogger.notice(" %s | %s | %s" % (userDN.ljust(maxDNLen),
                                              group.ljust(maxGroupLen),
                                              uploadedInfo[userDN][group].strftime("%Y/%m/%d %H:%M")))

  if params.checkValid:
    if infoDict['secondsLeft'] == 0:
      invalidProxy("Proxy is expired")
    if params.csEnabled and not infoDict['validGroup']:
      invalidProxy("Group %s is not valid" % infoDict['group'])
    if 'hasVOMS' in infoDict and infoDict['hasVOMS']:
      requiredVOMS = Registry.getVOMSAttributeForGroup(infoDict['group'])
      if 'VOMS' not in infoDict or not infoDict['VOMS']:
        invalidProxy("Unable to retrieve VOMS extension")
      if len(infoDict['VOMS']) > 1:
        invalidProxy("More than one voms attribute found")
      if requiredVOMS not in infoDict['VOMS']:
        invalidProxy("Unexpected VOMS extension %s. Extension expected for DIRAC group is %s" % (
            infoDict['VOMS'][0],
            requiredVOMS))
      result = VOMS.VOMS().getVOMSProxyInfo(infoDict['chain'], 'actimeleft')
      if not result['OK']:
        invalidProxy("Cannot determine life time of VOMS attributes: %s" % result['Message'])
      if int(result['Value'].strip()) == 0:
        invalidProxy("VOMS attributes are expired")

  sys.exit(0)


if __name__ == "__main__":
  main()
