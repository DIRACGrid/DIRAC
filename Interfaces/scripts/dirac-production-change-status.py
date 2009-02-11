#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-production-change-status.py,v 1.2 2009/02/11 10:56:06 paterson Exp $
# File :   dirac-production-change-status
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-production-change-status.py,v 1.2 2009/02/11 10:56:06 paterson Exp $"
__VERSION__ = "$Revision: 1.2 $"

import string

from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracProduction                    import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

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

  DIRAC.exit(2)

if len(args) < 2:
  usage()

exitCode = 0
errorList = []
command = args[0]

for prodID in args[1:]:

  result = diracProd.production(prodID,command,printOutput=True,disableCheck=False)
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