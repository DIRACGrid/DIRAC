#!/usr/bin/env python

"""
Ban one or more Storage Elements for usage

Usage:
  dirac-admin-ban-se SE1 [SE2 ...]

Example:
  $ dirac-admin-ban-se M3PEC-disk
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
  read = True
  write = True
  check = True
  remove = True
  site = ''
  mute = False

  Script.registerSwitch("r", "BanRead", "     Ban only reading from the storage element")
  Script.registerSwitch("w", "BanWrite", "     Ban writing to the storage element")
  Script.registerSwitch("k", "BanCheck", "     Ban check access to the storage element")
  Script.registerSwitch("v", "BanRemove", "    Ban remove access to the storage element")
  Script.registerSwitch("a", "All", "    Ban all access to the storage element")
  Script.registerSwitch("m", "Mute", "     Do not send email")
  Script.registerSwitch(
      "S:",
      "Site=",
      "     Ban all SEs associate to site (note that if writing is allowed, check is always allowed)")
  Script.parseCommandLine(ignoreErrors=True)

  ses = Script.getPositionalArgs()
  for switch in Script.getUnprocessedSwitches():
    if switch[0].lower() in ("r", "banread"):
      write = False
      check = False
      remove = False
    if switch[0].lower() in ("w", "banwrite"):
      read = False
      check = False
      remove = False
    if switch[0].lower() in ("k", "bancheck"):
      read = False
      write = False
      remove = False
    if switch[0].lower() in ("v", "banremove"):
      read = False
      write = False
      check = False
    if switch[0].lower() in ("a", "all"):
      pass
    if switch[0].lower() in ("m", "mute"):
      mute = True
    if switch[0].lower() in ("s", "site"):
      site = switch[1]

  # from DIRAC.ConfigurationSystem.Client.CSAPI           import CSAPI
  from DIRAC import gConfig, gLogger
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
  from DIRAC.Core.Security.ProxyInfo import getProxyInfo
  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
  from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup

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
    DIRAC.exit(-1)

  readBanned = []
  writeBanned = []
  checkBanned = []
  removeBanned = []

  resourceStatus = ResourceStatus()

  res = resourceStatus.getElementStatus(ses, "StorageElement")
  if not res['OK']:
    gLogger.error("Storage Element %s does not exist" % ses)
    DIRAC.exit(-1)

  reason = 'Forced with dirac-admin-ban-se by %s' % userName

  for se, seOptions in res['Value'].items():

    resW = resC = resR = {'OK': False}

    # Eventually, we will get rid of the notion of InActive, as we always write Banned.
    if read and 'ReadAccess' in seOptions:

      if not seOptions['ReadAccess'] in ['Active', 'Degraded', 'Probing']:
        gLogger.notice('Read option for %s is %s, instead of %s' %
                       (se, seOptions['ReadAccess'], ['Active', 'Degraded', 'Probing']))
        gLogger.notice('Try specifying the command switches')
      else:

        resR = resourceStatus.setElementStatus(se, 'StorageElement', 'ReadAccess', 'Banned', reason, userName)
        # res = csAPI.setOption( "%s/%s/ReadAccess" % ( storageCFGBase, se ), "InActive" )
        if not resR['OK']:
          gLogger.error('Failed to update %s read access to Banned' % se)
        else:
          gLogger.notice('Successfully updated %s read access to Banned' % se)
          readBanned.append(se)

    # Eventually, we will get rid of the notion of InActive, as we always write Banned.
    if write and 'WriteAccess' in seOptions:

      if not seOptions['WriteAccess'] in ['Active', 'Degraded', 'Probing']:
        gLogger.notice('Write option for %s is %s, instead of %s' %
                       (se, seOptions['WriteAccess'], ['Active', 'Degraded', 'Probing']))
        gLogger.notice('Try specifying the command switches')
      else:

        resW = resourceStatus.setElementStatus(se, 'StorageElement', 'WriteAccess', 'Banned', reason, userName)
        # res = csAPI.setOption( "%s/%s/WriteAccess" % ( storageCFGBase, se ), "InActive" )
        if not resW['OK']:
          gLogger.error("Failed to update %s write access to Banned" % se)
        else:
          gLogger.notice("Successfully updated %s write access to Banned" % se)
          writeBanned.append(se)

    # Eventually, we will get rid of the notion of InActive, as we always write Banned.
    if check and 'CheckAccess' in seOptions:

      if not seOptions['CheckAccess'] in ['Active', 'Degraded', 'Probing']:
        gLogger.notice('Check option for %s is %s, instead of %s' %
                       (se, seOptions['CheckAccess'], ['Active', 'Degraded', 'Probing']))
        gLogger.notice('Try specifying the command switches')
      else:

        resC = resourceStatus.setElementStatus(se, 'StorageElement', 'CheckAccess', 'Banned', reason, userName)
        # res = csAPI.setOption( "%s/%s/CheckAccess" % ( storageCFGBase, se ), "InActive" )
        if not resC['OK']:
          gLogger.error("Failed to update %s check access to Banned" % se)
        else:
          gLogger.notice("Successfully updated %s check access to Banned" % se)
          checkBanned.append(se)

    # Eventually, we will get rid of the notion of InActive, as we always write Banned.
    if remove and 'RemoveAccess' in seOptions:

      if not seOptions['RemoveAccess'] in ['Active', 'Degraded', 'Probing']:
        gLogger.notice('Remove option for %s is %s, instead of %s' %
                       (se, seOptions['RemoveAccess'], ['Active', 'Degraded', 'Probing']))
        gLogger.notice('Try specifying the command switches')
      else:

        resC = resourceStatus.setElementStatus(se, 'StorageElement', 'RemoveAccess', 'Banned', reason, userName)
        # res = csAPI.setOption( "%s/%s/CheckAccess" % ( storageCFGBase, se ), "InActive" )
        if not resC['OK']:
          gLogger.error("Failed to update %s remove access to Banned" % se)
        else:
          gLogger.notice("Successfully updated %s remove access to Banned" % se)
          removeBanned.append(se)

    if not(resR['OK'] or resW['OK'] or resC['OK']):
      DIRAC.exit(-1)

  if not (writeBanned or readBanned or checkBanned or removeBanned):
    gLogger.notice("No storage elements were banned")
    DIRAC.exit(-1)

  if mute:
    gLogger.notice('Email is muted by script switch')
    DIRAC.exit(0)

  subject = '%s storage elements banned for use' % len(writeBanned + readBanned + checkBanned + removeBanned)
  addressPath = 'EMail/Production'
  address = Operations().getValue(addressPath, '')

  body = ''
  if read:
    body = "%s\n\nThe following storage elements were banned for reading:" % body
    for se in readBanned:
      body = "%s\n%s" % (body, se)
  if write:
    body = "%s\n\nThe following storage elements were banned for writing:" % body
    for se in writeBanned:
      body = "%s\n%s" % (body, se)
  if check:
    body = "%s\n\nThe following storage elements were banned for check access:" % body
    for se in checkBanned:
      body = "%s\n%s" % (body, se)
  if remove:
    body = "%s\n\nThe following storage elements were banned for remove access:" % body
    for se in removeBanned:
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
