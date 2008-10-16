#! /usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-admin-ce-info.py,v 1.1 2008/10/16 09:21:27 paterson Exp $
# File :   dirac-admin-site-info
# Author : Vladimir Romanovsky
########################################################################
__RCSID__   = "$Id: dirac-admin-ce-info.py,v 1.1 2008/10/16 09:21:27 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s < ce name> [< ce name>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

diracAdmin = DiracAdmin()
exitCode = 0
errorList = []

result = DIRAC.gConfig.getSections('/Resources/Sites/LCG')
if not result['OK']:
  print 'Could not get DIRAC site list'
  DIRAC.exit(2)

sites = result['Value']

for site in sites:
  result = diracAdmin.getCSDict('/Resources/Sites/LCG/%s'%site)
  if result['OK']:
    ces = result['Value'].get('CE',[])
    for ce in args:
      if ce in ces:
        print '%s: %s'%(ce,site)
	
DIRAC.exit(exitCode)
