#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-admin-ban-site.py,v 1.2 2009/05/20 12:03:22 acsmith Exp $
# File :   dirac-admin-ban-site
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-admin-ban-site.py,v 1.2 2009/05/20 12:03:22 acsmith Exp $"
__VERSION__ = "$Revision: 1.2 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "email=", "Boolean True/False (True by default)" )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
from DIRAC import gConfig

import time,string

def getBoolean(value):
  if value.lower()=='true':
    return True
  elif value.lower()=='false':
    return False
  else:
    print 'ERROR: expected boolean'
    DIRAC.exit(2)

email = True
for switch in Script.getUnprocessedSwitches():
  if switch[0]=="email":
    email=getBoolean(switch[1])

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <DIRAC site name> <COMMENT>" [--email=<True/False>]' %(Script.scriptName)
  print 'Note: emails should only be disabled for bulk operations.'
  DIRAC.exit(2)

if len(args) < 1:
  usage()

diracAdmin = DiracAdmin()
exitCode = 0
errorList = []
address = gConfig.getValue('/Operations/EMail/Production','')
setup = gConfig.getValue('/DIRAC/Setup','')
if not address or not setup:
  print 'ERROR: Could not contact Configuration Service'
  exitCode = 2
  DIRAC.exit(exitCode)

userName = diracAdmin._getCurrentUser()
if not userName['OK']:
  print 'ERROR: Could not obtain current username from proxy'
  exitCode = 2
  DIRAC.exit(exitCode)
userName = userName['Value']

site = args[0]
comment = args[1]
result = diracAdmin.banSiteFromMask(site,comment,printOutput=True)
if not result['OK']:
  errorList.append( (site, result['Message']) )
  exitCode = 2
else:
  subject = '%s is banned for %s setup' %(site,setup)
  body = 'Site %s is removed from site mask for %s setup by %s on %s.\n\n' %(site,setup,userName,time.asctime())
  body += 'Comment:\n%s' %comment
  if email:
    result = diracAdmin.sendMail(address,subject,body)
  else:
    print 'Automatic email disabled by flag.'

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)
