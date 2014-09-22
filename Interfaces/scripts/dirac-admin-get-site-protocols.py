#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-get-site-protocols
# Author :  Stuart Paterson
########################################################################
"""
  Check the defined protocols for all SEs of a given site
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... PilotID ...' % Script.scriptName ] ) )
Script.registerSwitch( "", "Site=", "Site for which protocols are to be checked (mandatory)" )
Script.parseCommandLine( ignoreErrors = True )

site = None
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "site":
    site = switch[1]

from DIRAC import exit as DIRACExit
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

if not site:
  Script.showHelp()

diracAdmin = DiracAdmin()
exitCode = 0
result = diracAdmin.getSiteProtocols( site, printOutput = True )
if not result['OK']:
  print 'ERROR: %s' % result['Message']
  exitCode = 2

DIRACExit( exitCode )
