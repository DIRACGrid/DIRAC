#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-banned-sites
# Author :  Stuart Paterson
########################################################################

from __future__ import print_function
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

from DIRAC import exit as DIRACExit
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
diracAdmin = DiracAdmin()

result = diracAdmin.getBannedSites()
if result['OK']:
  bannedSites = result['Value']
else:
  print(result['Message'])
  DIRACExit(2)

for site in bannedSites:
  result = diracAdmin.getSiteMaskLogging(site)
  if result['OK']:
    for siteLog in result['Value']:
      print('%-30s %s %s %s' % (site, siteLog[0], siteLog[1], siteLog[2]))
  else:
    print('%-30s %s' % (site, result['Message']))

DIRACExit(0)
