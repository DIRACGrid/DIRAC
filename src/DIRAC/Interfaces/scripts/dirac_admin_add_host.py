#!/usr/bin/env python
"""
Add or Modify a Host info in DIRAC

Usage:
  dirac-admin-add-host [options] ... Property=<Value> ...

Arguments:
  Property=<Value>: Other properties to be added to the Host like (Responsible=XXX)

Example:
  $ dirac-admin-add-host -H dirac.i2np3.fr -D /O=GRID-FR/C=FR/O=CNRS/OU=CC-IN2P3/CN=dirac.in2p3.fr
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

hostName = None
hostDN = None
hostProperties = []


def setHostName(arg):
  global hostName
  if hostName or not arg:
    Script.showHelp(exitCode=1)
  hostName = arg


def setHostDN(arg):
  global hostDN
  if hostDN or not arg:
    Script.showHelp(exitCode=1)
  hostDN = arg


def addProperty(arg):
  global hostProperties
  if not arg:
    Script.showHelp(exitCode=1)
  if arg not in hostProperties:
    hostProperties.append(arg)


@DIRACScript()
def main():
  global hostName
  global hostDN
  global hostProperties
  Script.registerSwitch('H:', 'HostName:', 'Name of the Host (Mandatory)', setHostName)
  Script.registerSwitch('D:', 'HostDN:', 'DN of the Host Certificate (Mandatory)', setHostDN)
  Script.registerSwitch(
      'P:',
      'Property:',
      'Property to be added to the Host (Allow Multiple instances or None)',
      addProperty)

  Script.parseCommandLine(ignoreErrors=True)

  if hostName is None or hostDN is None:
    Script.showHelp(exitCode=1)

  args = Script.getPositionalArgs()

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  errorList = []

  hostProps = {'DN': hostDN}
  if hostProperties:
    hostProps['Properties'] = ', '.join(hostProperties)

  for prop in args:
    pl = prop.split("=")
    if len(pl) < 2:
      errorList.append(("in arguments", "Property %s has to include a '=' to separate name from value" % prop))
      exitCode = 255
    else:
      pName = pl[0]
      pValue = "=".join(pl[1:])
      Script.gLogger.info("Setting property %s to %s" % (pName, pValue))
      hostProps[pName] = pValue

  if not diracAdmin.csModifyHost(hostName, hostProps, createIfNonExistant=True)['OK']:
    errorList.append(("add host", "Cannot register host %s" % hostName))
    exitCode = 255
  else:
    result = diracAdmin.csCommitChanges()
    if not result['OK']:
      errorList.append(("commit", result['Message']))
      exitCode = 255

  if exitCode == 0:
    from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient
    cmc = ComponentMonitoringClient()
    ret = cmc.hostExists(dict(HostName=hostName))
    if not ret['OK']:
      Script.gLogger.error('Cannot check if host is registered in ComponentMonitoring', ret['Message'])
    elif ret['Value']:
      Script.gLogger.info('Host already registered in ComponentMonitoring')
    else:
      ret = cmc.addHost(dict(HostName=hostName, CPU='TO_COME'))
      if not ret['OK']:
        Script.gLogger.error('Failed to add Host to ComponentMonitoring', ret['Message'])

  for error in errorList:
    Script.gLogger.error("%s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
