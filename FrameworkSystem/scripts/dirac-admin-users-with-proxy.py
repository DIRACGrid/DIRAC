#!/usr/bin/env python

from __future__ import print_function
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
result = gProxyManager.getUploadedProxiesDetails()
if not result['OK']:
  print("Can't retrieve list of users: %s" % result['Message'])
  DIRAC.exit(1)

dataDict = {}
for infoDict in result['Value']['Dictionaries']:
  user = infoDict['user']
  del infoDict['user']
  dt = infoDict['expirationtime'] - Time.dateTime()
  secsLeft = dt.days * 86400 + dt.seconds
  if secsLeft > params.proxyLifeTime:
    infoDict['expirationtime'] = Time.toString(infoDict['expirationtime'])
    if user not in dataDict:
      dataDict[user] = []
    dataDict[user].append(infoDict)

ln = len(max(result['Value']['Dictionaries'][0].keys() if result['Value']['Dictionaries'] else ['']))

for user, userDicts in dataDict:
  print("* %s" % user)
  for userDict in userDicts:
    for k, v in userDict:
      print(" %s : %s" % (k + (' ' * (len(k) - ln))), v)
  print(" -")

DIRAC.exit(0)
