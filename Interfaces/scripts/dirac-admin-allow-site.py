#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-admin-allow-site.py,v 1.1 2008/10/16 09:21:27 paterson Exp $
# File :   dirac-admin-allow-site
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-admin-allow-site.py,v 1.1 2008/10/16 09:21:27 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "comment=", "To add a comment string when allowing sites" )
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

comment = None
email = True
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower()=="comment":
    comment = switch[1]
  elif switch[0]=="email":
    email=getBoolean(switch[1])

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <DIRAC site name> [<DIRAC site name>] --comment="<COMMENT>"' %(Script.scriptName)
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

if not comment:
  comment = 'No comment supplied.'

for site in args:
  result = diracAdmin.addSiteInMask(site,comment,printOutput=True)
  if not result['OK']:
    errorList.append( (site, result['Message']) )
    exitCode = 2
  else:
    subject = '%s is added in site mask for %s setup' %(site,setup)
    body = 'Site %s is added to the site mask for %s setup by %s on %s.\n\n' %(site,setup,userName,time.asctime())
    body += 'Comment:\n%s' %comment
    if email:
      result = diracAdmin.sendMail(address,subject,body)
    else:
      print 'Automatic email disabled by flag.'

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)
