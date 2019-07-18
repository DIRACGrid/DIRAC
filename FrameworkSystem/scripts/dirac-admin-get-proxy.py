#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-proxy
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve a delegated proxy for the given user and group
"""
from __future__ import print_function
import os
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

__RCSID__ = "$Id$"


class Params(object):

  limited = False
  proxyPath = False
  proxyLifeTime = 86400
  enableVOMS = False
  vomsAttr = False

  def setLimited(self, args):
    self.limited = True
    return DIRAC.S_OK()

  def setProxyLocation(self, args):
    self.proxyPath = args
    return DIRAC.S_OK()

  def setProxyLifeTime(self, arg):
    try:
      fields = [f.strip() for f in arg.split(":")]
      self.proxyLifeTime = int(fields[0]) * 3600 + int(fields[1]) * 60
    except BaseException:
      print("Can't parse %s time! Is it a HH:MM?" % arg)
      return DIRAC.S_ERROR("Can't parse time argument")
    return DIRAC.S_OK()

  def automaticVOMS(self, arg):
    self.enableVOMS = True
    return DIRAC.S_OK()

  def setVOMSAttr(self, arg):
    self.enableVOMS = True
    self.vomsAttr = arg
    return DIRAC.S_OK()

  def registerCLISwitches(self):
    Script.registerSwitch("v:", "valid=", "Valid HH:MM for the proxy. By default is 24 hours", self.setProxyLifeTime)
    Script.registerSwitch("l", "limited", "Get a limited proxy", self.setLimited)
    Script.registerSwitch("u:", "out=", "File to write as proxy", self.setProxyLocation)
    Script.registerSwitch("a", "voms", "Get proxy with VOMS extension mapped to the DIRAC group", self.automaticVOMS)
    Script.registerSwitch("m:", "vomsAttr=", "VOMS attribute to require", self.setVOMSAttr)


params = Params()
params.registerCLISwitches()

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... <DN|user> group' % Script.scriptName,
                                  'Arguments:',
                                  '  DN:       DN of the user',
                                  '  user:     DIRAC user name (will fail if there is more than 1 DN registered)',
                                  '  group:    DIRAC group name']))

Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) != 2:
  Script.showHelp()

userGroup = str(args[1])
userDN = str(args[0])
userName = False
if userDN.find("/") != 0:
  userName = userDN
  retVal = Registry.getDNForUsername(userName)
  if not retVal['OK']:
    print("Cannot discover DN for username %s\n\t%s" % (userName, retVal['Message']))
    DIRAC.exit(2)
  DNList = retVal['Value']
  if len(DNList) > 1:
    print("Username %s has more than one DN registered" % userName)
    ind = 0
    for dn in DNList:
      print("%d %s" % (ind, dn))
      ind += 1
    inp = raw_input("Which DN do you want to download? [default 0] ")
    if not inp:
      inp = 0
    else:
      inp = int(inp)
    userDN = DNList[inp]
  else:
    userDN = DNList[0]

if not params.proxyPath:
  if not userName:
    result = Registry.getUsernameForDN(userDN)
    if not result['OK']:
      print("DN '%s' is not registered in DIRAC" % userDN)
      DIRAC.exit(2)
    userName = result['Value']
  params.proxyPath = "%s/proxy.%s.%s" % (os.getcwd(), userName, userGroup)

if params.enableVOMS:
  result = gProxyManager.downloadVOMSProxy(userDN, userGroup, limited=params.limited,
                                           requiredTimeLeft=params.proxyLifeTime,
                                           requiredVOMSAttribute=params.vomsAttr)
else:
  result = gProxyManager.downloadProxy(userDN, userGroup, limited=params.limited,
                                       requiredTimeLeft=params.proxyLifeTime)
if not result['OK']:
  print('Proxy file cannot be retrieved: %s' % result['Message'])
  DIRAC.exit(2)
chain = result['Value']
result = chain.dumpAllToFile(params.proxyPath)
if not result['OK']:
  print('Proxy file cannot be written to %s: %s' % (params.proxyPath, result['Message']))
  DIRAC.exit(2)
print("Proxy downloaded to %s" % params.proxyPath)
DIRAC.exit(0)
