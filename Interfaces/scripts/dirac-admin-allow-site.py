#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-allow-site
# Author :  Stuart Paterson
########################################################################
"""
  Add Site to Active mask for current Setup
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.registerSwitch( "E:", "email=", "Boolean True/False (True by default)" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Site Comment' % Script.scriptName,
                                     'Arguments:',
                                     '  Site:     Name of the Site',
                                     '  Comment:  Reason of the action' ] ) )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.DiracAdmin                     import DiracAdmin
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC                                               import exit as DIRACExit, gConfig, gLogger

import time

def getBoolean( value ):
  if value.lower() == 'true':
    return True
  elif value.lower() == 'false':
    return False
  else:
    Script.showHelp()

email = True
for switch in Script.getUnprocessedSwitches():
  if switch[0] == "email":
    email = getBoolean( switch[1] )

args = Script.getPositionalArgs()

if len( args ) < 2:
  Script.showHelp()

diracAdmin = DiracAdmin()
exitCode = 0
errorList = []
setup = gConfig.getValue( '/DIRAC/Setup', '' )
if not setup:
  print 'ERROR: Could not contact Configuration Service'
  exitCode = 2
  DIRACExit( exitCode )

site = args[0]
comment = args[1]
result = diracAdmin.addSiteInMask( site, comment, printOutput = True )
if not result['OK']:
  errorList.append( ( site, result['Message'] ) )
  exitCode = 2
else:
  if email:
    userName = diracAdmin._getCurrentUser()
    if not userName['OK']:
      print 'ERROR: Could not obtain current username from proxy'
      exitCode = 2
      DIRACExit( exitCode )
    userName = userName['Value']
    subject = '%s is added in site mask for %s setup' % ( site, setup )
    body = 'Site %s is added to the site mask for %s setup by %s on %s.\n\n' % ( site, setup, userName, time.asctime() )
    body += 'Comment:\n%s' % comment
    addressPath = 'EMail/Production'
    address = Operations().getValue( addressPath, '' )
    if not address:
      gLogger.notice( "'%s' not defined in Operations, can not send Mail\n" % addressPath, body )
    else:
      result = diracAdmin.sendMail( address, subject, body )
  else:
    print 'Automatic email disabled by flag.'

for error in errorList:
  print "ERROR %s: %s" % error

DIRACExit( exitCode )
