#!/usr/bin/env python

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Utilities import Time

__RCSID__ = "$Id$"


class Params(object):

  limited = False
  proxyPath = False
  proxyLifeTime = 3600

  def setProxyLifeTime(self, arg):
    try:
      fields = [f.strip() for f in arg.split(":")]
      self.proxyLifeTime = int(fields[0]) * 3600 + int(fields[1]) * 60
    except BaseException:
      print("Can't parse %s time! Is it a HH:MM?" % arg)
      return DIRAC.S_ERROR("Can't parse time argument")
    return DIRAC.S_OK()

  def registerCLISwitches(self):
    Script.registerSwitch("v:", "valid=", "Required HH:MM for the users", self.setProxyLifeTime)


params = Params()
params.registerCLISwitches()

Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()
result = gProxyManager.getDBContents()
if not result['OK']:
  print("Can't retrieve list of users: %s" % result['Message'])
  DIRAC.exit(1)

keys = result['Value']['ParameterNames']
records = result['Value']['Records']
dataDict = {}
now = Time.dateTime()
for record in records:
  expirationDate = record[3]
  dt = expirationDate - now
  secsLeft = dt.days * 86400 + dt.seconds
  if secsLeft > params.proxyLifeTime:
    userName, userDN, userGroup, _, persistent = record
    if userName not in dataDict:
      dataDict[userName] = []
    dataDict[userName].append((userDN, userGroup, expirationDate, persistent))


for userName in dataDict:
  print("* %s" % userName)
  for iP in range(len(dataDict[userName])):
    data = dataDict[userName][iP]
    print(" DN         : %s" % data[0])
    print(" group      : %s" % data[1])
    print(" not after  : %s" % Time.toString(data[2]))
    print(" persistent : %s" % data[3])
    if iP < len(dataDict[userName]) - 1:
      print(" -")


DIRAC.exit(0)
