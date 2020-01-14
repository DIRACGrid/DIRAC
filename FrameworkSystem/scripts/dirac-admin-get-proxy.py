#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-proxy
# Author :  Stuart Paterson
########################################################################
""" Retrieve a delegated proxy for the given user and group
"""
from __future__ import print_function
import os
import DIRAC

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

__RCSID__ = "$Id$"


class Params(object):

  limited = False
  proxyPath = False
  proxyLifeTime = 86400
  enableVOMS = False

  def setLimited(self, args):
    """ Set limited

        :param bool args: is limited

        :return: S_OK()/S_ERROR()
    """
    self.limited = True
    return S_OK()

  def setProxyLocation(self, args):
    """ Set proxy location

        :param str args: proxy path

        :return: S_OK()/S_ERROR()
    """
    self.proxyPath = args
    return S_OK()

  def setProxyLifeTime(self, arg):
    """ Set proxy lifetime

        :param int arg: lifetime in a seconds

        :return: S_OK()/S_ERROR()
    """
    try:
      fields = [f.strip() for f in arg.split(":")]
      self.proxyLifeTime = int(fields[0]) * 3600 + int(fields[1]) * 60
    except BaseException:
      gLogger.notice("Can't parse %s time! Is it a HH:MM?" % arg)
      return S_ERROR("Can't parse time argument")
    return S_OK()

  def automaticVOMS(self, arg):
    """ Enable VOMS

        :param bool arg: enable VOMS

        :return: S_OK()/S_ERROR()
    """
    self.enableVOMS = True
    return S_OK()

  def registerCLISwitches(self):
    """ Register CLI switches
    """
    Script.registerSwitch("v:", "valid=", "Valid HH:MM for the proxy. By default is 24 hours", self.setProxyLifeTime)
    Script.registerSwitch("l", "limited", "Get a limited proxy", self.setLimited)
    Script.registerSwitch("u:", "out=", "File to write as proxy", self.setProxyLocation)
    Script.registerSwitch("a", "voms", "Get proxy with VOMS extension mapped to the DIRAC group", self.automaticVOMS)

params = Params()
params.registerCLISwitches()

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... user group' % Script.scriptName,
                                  'Arguments:',
                                  '  user:     DIRAC user name',
                                  '  group:    DIRAC group name']))

Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) != 2:
  Script.showHelp()

userGroup = str(args[1])

# First argument is user name
if str(args[0]).find("/"):
  userName = str(args[0])
  result = Registry.getDNForUsernameInGroup(userName, userGroup)
  if not result['OK']:
    gLogger.notice("Cannot discover DN for %s@%s" % (userName, userGroup))
    DIRAC.exit(2)
  userDN = result['Value']
# Or DN
else:
  userDN = str(args[0])
  result = Registry.getUsernameForDN(userDN)
  if not result['OK']:
    gLogger.notice("DN '%s' is not registered in DIRAC" % userDN)
    DIRAC.exit(2)
  userName = result['Value']

if not params.proxyPath:
  if not userName:
    result = Registry.getUsernameForDN(userDN)
    if not result['OK']:
      gLogger.notice("DN '%s' is not registered in DIRAC" % userDN)
      DIRAC.exit(2)
    userName = result['Value']
  params.proxyPath = "%s/proxy.%s.%s" % (os.getcwd(), userName, userGroup)

if params.enableVOMS:
  result = gProxyManager.downloadVOMSProxy(userName, userGroup, limited=params.limited,
                                           requiredTimeLeft=params.proxyLifeTime)
else:
  result = gProxyManager.downloadProxy(userName, userGroup, limited=params.limited,
                                       requiredTimeLeft=params.proxyLifeTime)
if not result['OK']:
  gLogger.notice('Proxy file cannot be retrieved: %s' % result['Message'])
  DIRAC.exit(2)
chain = result['Value']
result = chain.dumpAllToFile(params.proxyPath)
if not result['OK']:
  gLogger.notice('Proxy file cannot be written to %s: %s' % (params.proxyPath, result['Message']))
  DIRAC.exit(2)
gLogger.notice("Proxy downloaded to %s" % params.proxyPath)
DIRAC.exit(0)
