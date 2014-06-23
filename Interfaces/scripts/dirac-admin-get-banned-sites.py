#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-get-banned-sites
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC import exit as DIRACExit
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()

result = diracAdmin.getBannedSites( printOutput = False )
if result['OK']:
  banned_sites = result['Value']
else:
  print result['Message']
  DIRACExit( 2 )

for site in banned_sites:
  result = diracAdmin.getSiteMaskLogging( site )
  if result['OK']:
    sites = result['Value']
    print '%-30s %s %s %s' % ( site, sites[site][-1][1], sites[site][-1][2], sites[site][-1][3] )
  else:
    print '%-30s %s' % ( site, result['Message'] )

DIRACExit( 0 )

