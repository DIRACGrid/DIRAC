#!/usr/bin/env python
########################################################################
# File :    dirac-proxy-init.py
# Author :  Adrian Casajus
########################################################################
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import sys

import DIRAC

from DIRAC import gLogger, S_OK
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient
from DIRAC.Core.Security import Properties
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

__RCSID__ = "$Id:"

userName = False


def setUser(arg):
  """ Set user

      :param basestring arg: user name

      :return: S_OK()
  """
  global userName
  userName = arg
  return S_OK()


Script.registerSwitch("u:", "user=", "User to query (by default oneself)", setUser)

Script.parseCommandLine()

result = getProxyInfo()
if not result['OK']:
  gLogger.notice("Do you have a valid proxy?")
  gLogger.notice(result['Message'])
  sys.exit(1)
proxyProps = result['Value']

userName = userName or proxyProps.get('username')
if not userName:
  gLogger.notice("Your proxy don`t have username extension")
  sys.exit(1)

if userName in Registry.getAllUsers():
  if Properties.PROXY_MANAGEMENT not in proxyProps['groupProperties']:
    if userName != proxyProps['username'] and userName != proxyProps['issuer']:
      gLogger.notice("You can only query info about yourself!")
      sys.exit(1)
  result = Registry.getDNForUsername(userName)
  if not result['OK']:
    gLogger.notice("Oops %s" % result['Message'])
  dnList = result['Value']
  if not dnList:
    gLogger.notice("User %s has no DN defined!" % userName)
    sys.exit(1)
  userDNs = dnList
else:
  userDNs = [userName]


gLogger.notice("Checking for DNs %s" % " | ".join(userDNs))
pmc = ProxyManagerClient()
result = pmc.getDBContents({'UserDN': userDNs})
if not result['OK']:
  gLogger.notice("Could not retrieve the proxy list: %s" % result['Value'])
  sys.exit(1)

data = result['Value']
colLengths = []
for pN in data['ParameterNames']:
  colLengths.append(len(pN))
for row in data['Records']:
  for i in range(len(row)):
    colLengths[i] = max(colLengths[i], len(str(row[i])))

lines = [""]
for i in range(len(data['ParameterNames'])):
  pN = data['ParameterNames'][i]
  lines[0] += "| %s " % pN.ljust(colLengths[i])
lines[0] += "|"
tL = len(lines[0])
lines.insert(0, "-" * tL)
lines.append("-" * tL)
for row in data['Records']:
  nL = ""
  for i in range(len(row)):
    nL += "| %s " % str(row[i]).ljust(colLengths[i])
  nL += "|"
  lines.append(nL)
  lines.append("-" * tL)

gLogger.notice("\n".join(lines))
