#!/usr/bin/env python
########################################################################
# File :    dirac-admin-allow-site
# Author :  Stuart Paterson
########################################################################
"""
Add Site to Active mask for current Setup

Usage:
  dirac-admin-allow-site [options] ... Site Comment

Arguments:
  Site:     Name of the Site
  Comment:  Reason of the action

Example:
  $ dirac-admin-allow-site LCG.IN2P3.fr "FRANCE"
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import time

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.registerSwitch("E:", "email=", "Boolean True/False (True by default)")
  Script.parseCommandLine(ignoreErrors=True)

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  from DIRAC import exit as DIRACExit, gConfig, gLogger

  def getBoolean(value):
    if value.lower() == 'true':
      return True
    elif value.lower() == 'false':
      return False
    else:
      Script.showHelp()

  email = True
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == "email":
      email = getBoolean(switch[1])

  args = Script.getPositionalArgs()

  if len(args) < 2:
    Script.showHelp()

  diracAdmin = DiracAdmin()
  exitCode = 0
  errorList = []
  setup = gConfig.getValue('/DIRAC/Setup', '')
  if not setup:
    print('ERROR: Could not contact Configuration Service')
    exitCode = 2
    DIRACExit(exitCode)

  # result = promptUser(
  #     'All the elements that are associated with this site will be active, '
  #     'are you sure about this action?'
  # )
  # if not result['OK'] or result['Value'] is 'n':
  #  print 'Script stopped'
  #  DIRACExit( 0 )

  site = args[0]
  comment = args[1]
  result = diracAdmin.allowSite(site, comment, printOutput=True)
  if not result['OK']:
    errorList.append((site, result['Message']))
    exitCode = 2
  else:
    if email:
      userName = diracAdmin._getCurrentUser()
      if not userName['OK']:
        print('ERROR: Could not obtain current username from proxy')
        exitCode = 2
        DIRACExit(exitCode)
      userName = userName['Value']
      subject = '%s is added in site mask for %s setup' % (site, setup)
      body = 'Site %s is added to the site mask for %s setup by %s on %s.\n\n' % (site, setup, userName, time.asctime())
      body += 'Comment:\n%s' % comment
      addressPath = 'EMail/Production'
      address = Operations().getValue(addressPath, '')
      if not address:
        gLogger.notice("'%s' not defined in Operations, can not send Mail\n" % addressPath, body)
      else:
        result = diracAdmin.sendMail(address, subject, body)
    else:
      print('Automatic email disabled by flag.')

  for error in errorList:
    print("ERROR %s: %s" % error)

  DIRACExit(exitCode)


if __name__ == "__main__":
  main()
