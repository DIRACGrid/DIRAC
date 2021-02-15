#!/usr/bin/env python
"""
Enable using one or more Storage Elements

Usage:
  dirac-admin-allow-se SE1 [SE2 ...]

Example:
  $ dirac-admin-allow-se M3PEC-disk
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  read = False
  write = False
  check = False
  remove = False
  site = ''
  mute = False

  Script.registerSwitch("r", "AllowRead", "     Allow only reading from the storage element")
  Script.registerSwitch("w", "AllowWrite", "     Allow only writing to the storage element")
  Script.registerSwitch("k", "AllowCheck", "     Allow only check access to the storage element")
  Script.registerSwitch("v", "AllowRemove", "    Allow only remove access to the storage element")
  Script.registerSwitch("a", "All", "    Allow all access to the storage element")
  Script.registerSwitch("m", "Mute", "     Do not send email")
  Script.registerSwitch("S:", "Site=", "     Allow all SEs associated to site")

  Script.parseCommandLine(ignoreErrors=True)

  ses = Script.getPositionalArgs()
  for switch in Script.getUnprocessedSwitches():
    if switch[0].lower() in ("r", "allowread"):
      read = True
    if switch[0].lower() in ("w", "allowwrite"):
      write = True
    if switch[0].lower() in ("k", "allowcheck"):
      check = True
    if switch[0].lower() in ("v", "allowremove"):
      remove = True
    if switch[0].lower() in ("a", "all"):
      read = True
      write = True
      check = True
      remove = True
    if switch[0].lower() in ("m", "mute"):
      mute = True
    if switch[0].lower() in ("s", "site"):
      site = switch[1]

  # imports
  from DIRAC import gConfig, gLogger
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
  from DIRAC.Core.Security.ProxyInfo import getProxyInfo
  from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup
  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

  if not (read or write or check or remove):
    # No switch was specified, means we need all of them
    gLogger.notice("No option given, all accesses will be allowed if they were not")
    read = True
    write = True
    check = True
    remove = True

  ses = resolveSEGroup(ses)
  diracAdmin = DiracAdmin()
  errorList = []
  setup = gConfig.getValue('/DIRAC/Setup', '')
  if not setup:
    print('ERROR: Could not contact Configuration Service')
    DIRAC.exit(2)

  res = getProxyInfo()
  if not res['OK']:
    gLogger.error('Failed to get proxy information', res['Message'])
    DIRAC.exit(2)

  userName = res['Value'].get('username')
  if not userName:
    gLogger.error('Failed to get username for proxy')
    DIRAC.exit(2)

  if site:
    res = getSites()
    if not res['OK']:
      gLogger.error(res['Message'])
      DIRAC.exit(-1)
    if site not in res['Value']:
      gLogger.error('The provided site (%s) is not known.' % site)
      DIRAC.exit(-1)
    ses.extend(res['Value']['SE'].replace(' ', '').split(','))
  if not ses:
    gLogger.error('There were no SEs provided')
    DIRAC.exit()

  STATUS_TYPES = ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"]
  ALLOWED_STATUSES = ["Unknown", "InActive", "Banned", "Probing", "Degraded"]

  statusAllowedDict = {}
  for statusType in STATUS_TYPES:
    statusAllowedDict[statusType] = []

  statusFlagDict = {}
  statusFlagDict['ReadAccess'] = read
  statusFlagDict['WriteAccess'] = write
  statusFlagDict['CheckAccess'] = check
  statusFlagDict['RemoveAccess'] = remove

  resourceStatus = ResourceStatus()

  res = resourceStatus.getElementStatus(ses, "StorageElement")
  if not res['OK']:
    gLogger.error('Storage Element %s does not exist' % ses)
    DIRAC.exit(-1)

  reason = 'Forced with dirac-admin-allow-se by %s' % userName

  for se, seOptions in res['Value'].items():
    # InActive is used on the CS model, Banned is the equivalent in RSS
    for statusType in STATUS_TYPES:
      if statusFlagDict[statusType]:
        if seOptions.get(statusType) == "Active":
          gLogger.notice('%s status of %s is already Active' % (statusType, se))
          continue
        if statusType in seOptions:
          if not seOptions[statusType] in ALLOWED_STATUSES:
            gLogger.notice('%s option for %s is %s, instead of %s' %
                           (statusType, se, seOptions['ReadAccess'], ALLOWED_STATUSES))
            gLogger.notice('Try specifying the command switches')
          else:
            resR = resourceStatus.setElementStatus(se, "StorageElement", statusType, 'Active', reason, userName)
            if not resR['OK']:
              gLogger.fatal("Failed to update %s %s to Active, exit -" % (se, statusType), resR['Message'])
              DIRAC.exit(-1)
            else:
              gLogger.notice("Successfully updated %s %s to Active" % (se, statusType))
              statusAllowedDict[statusType].append(se)

  totalAllowed = 0
  totalAllowedSEs = []
  for statusType in STATUS_TYPES:
    totalAllowed += len(statusAllowedDict[statusType])
    totalAllowedSEs += statusAllowedDict[statusType]
  totalAllowedSEs = list(set(totalAllowedSEs))

  if not totalAllowed:
    gLogger.info("No storage elements were allowed")
    DIRAC.exit(-1)

  if mute:
    gLogger.notice('Email is muted by script switch')
    DIRAC.exit(0)

  subject = '%s storage elements allowed for use' % len(totalAllowedSEs)
  addressPath = 'EMail/Production'
  address = Operations().getValue(addressPath, '')

  body = ''
  if read:
    body = "%s\n\nThe following storage elements were allowed for reading:" % body
    for se in statusAllowedDict['ReadAccess']:
      body = "%s\n%s" % (body, se)
  if write:
    body = "%s\n\nThe following storage elements were allowed for writing:" % body
    for se in statusAllowedDict['WriteAccess']:
      body = "%s\n%s" % (body, se)
  if check:
    body = "%s\n\nThe following storage elements were allowed for checking:" % body
    for se in statusAllowedDict['CheckAccess']:
      body = "%s\n%s" % (body, se)
  if remove:
    body = "%s\n\nThe following storage elements were allowed for removing:" % body
    for se in statusAllowedDict['RemoveAccess']:
      body = "%s\n%s" % (body, se)

  if not address:
    gLogger.notice("'%s' not defined in Operations, can not send Mail\n" % addressPath, body)
    DIRAC.exit(0)

  res = diracAdmin.sendMail(address, subject, body)
  gLogger.notice('Notifying %s' % address)
  if res['OK']:
    gLogger.notice(res['Value'])
  else:
    gLogger.notice(res['Message'])

  DIRAC.exit(0)


if __name__ == "__main__":
  main()
