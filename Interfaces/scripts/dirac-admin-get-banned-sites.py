#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-admin-get-banned-sites.py,v 1.1 2008/10/16 09:21:28 paterson Exp $
# File :   dirac-admin-get-banned-sites
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-admin-get-banned-sites.py,v 1.1 2008/10/16 09:21:28 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

diracAdmin = DiracAdmin()

result = diracAdmin.getBannedSites(printOutput=False)
if result['OK']:
  banned_sites = result['Value']
else:
  print result['Message']
  DIRAC.exit(2)

for site in banned_sites:
  result = diracAdmin.getSiteMaskLogging( site)
  if result['OK']:
    sites = result['Value']
    print '%-30s %s %s %s' % (site, sites[site][-1][1], sites[site][-1][2], sites[site][-1][3])
  else:
    print '%-30s %s' % (site, result['Message'])

DIRAC.exit(0)

