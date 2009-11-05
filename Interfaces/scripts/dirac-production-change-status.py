#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-production-change-status
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.3 $"

import string

from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "f", "Force", "Optional: specify this flag to disable checks" )
Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()

disableChecks=False
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ('f','force'):
    disableChecks=True

from DIRAC.Interfaces.API.DiracProduction                    import DiracProduction
diracProd = DiracProduction()

def usage():
  print 'Usage: %s <Command> <Production ID> |<Production ID>' %(Script.scriptName)
  commands = diracProd.getProductionCommands()['Value']
  print "\nCommands include: %s" %(string.join(commands.keys(),', '))
  print '\nDescription:\n'
  for n,v in commands.items():
    print '%s:' %n
    for i,j in v.items():
      print '     %s = %s' %(i,j)
  print '\nOptional flag: -f --Force to disable checks'

  DIRAC.exit(2)

if len(args) < 2:
  usage()

exitCode = 0
errorList = []
command = args[0]

for prodID in args[1:]:

  result = diracProd.production(prodID,command,printOutput=True,disableCheck=disableChecks)
  if result.has_key('Message'):
    errorList.append( (prodID, result['Message']) )
    exitCode = 2
  elif not result:
    errorList.append( (prodID, 'Null result for getProduction() call' ) )
    exitCode = 2
  else:
    exitCode = 0

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)