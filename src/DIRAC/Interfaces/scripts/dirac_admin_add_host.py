#!/usr/bin/env python
"""
Add or Modify a Host info in DIRAC

Example:
  $ dirac-admin-add-host -H dirac.i2np3.fr -D /O=GRID-FR/C=FR/O=CNRS/OU=CC-IN2P3/CN=dirac.in2p3.fr
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


class AddHost(DIRACScript):

  def initParameters(self):
    self.hostName = None
    self.hostDN = None
    self.hostProperties = []
    self.switches = [
        ('H:', 'HostName:', 'Name of the Host (Mandatory)', self.setHostName),
        ('D:', 'HostDN:', 'DN of the Host Certificate (Mandatory)', self.setHostDN),
        ('P:', 'Property:',
         'Property to be added to the Host (Allow Multiple instances or None)',
         self.addProperty)
    ]

  def setHostName(self, arg):
    if self.hostName or not arg:
      self.showHelp(exitCode=1)
    self.hostName = arg

  def setHostDN(self, arg):
    if self.hostDN or not arg:
      self.showHelp(exitCode=1)
    self.hostDN = arg

  def addProperty(self, arg):
    if not arg:
      self.showHelp(exitCode=1)
    if arg not in self.hostProperties:
      self.hostProperties.append(arg)


@DIRACScript()
def main(self):
  self.registerSwitches(self.switches)
  self.registerArgument(["Property=<Value>: Other properties to be added to the Host like (Responsible=XXX)"],
                        mandatory=False)
  _, args = self.parseCommandLine(ignoreErrors=True)

  if self.hostName is None or self.hostDN is None:
    self.showHelp(exitCode=1)

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  errorList = []

  hostProps = {'DN': self.hostDN}
  if self.hostProperties:
    hostProps['Properties'] = ', '.join(self.hostProperties)

  for prop in args:
    pl = prop.split("=")
    if len(pl) < 2:
      errorList.append(("in arguments", "Property %s has to include a '=' to separate name from value" % prop))
      exitCode = 255
    else:
      pName = pl[0]
      pValue = "=".join(pl[1:])
      self.gLogger.info("Setting property %s to %s" % (pName, pValue))
      hostProps[pName] = pValue

  if not diracAdmin.csModifyHost(self.hostName, hostProps, createIfNonExistant=True)['OK']:
    errorList.append(("add host", "Cannot register host %s" % self.hostName))
    exitCode = 255
  else:
    result = diracAdmin.csCommitChanges()
    if not result['OK']:
      errorList.append(("commit", result['Message']))
      exitCode = 255

  if exitCode == 0:
    from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient
    cmc = ComponentMonitoringClient()
    ret = cmc.hostExists(dict(HostName=self.hostName))
    if not ret['OK']:
      self.gLogger.error('Cannot check if host is registered in ComponentMonitoring', ret['Message'])
    elif ret['Value']:
      self.gLogger.info('Host already registered in ComponentMonitoring')
    else:
      ret = cmc.addHost(dict(HostName=self.hostName, CPU='TO_COME'))
      if not ret['OK']:
        self.gLogger.error('Failed to add Host to ComponentMonitoring', ret['Message'])

  for error in errorList:
    self.gLogger.error("%s: %s" % error)

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
