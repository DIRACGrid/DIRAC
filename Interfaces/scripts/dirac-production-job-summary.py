#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-production-job-summary.py,v 1.1 2008/10/16 09:28:33 paterson Exp $
# File :   dirac-production-job-summary
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-production-job-summary.py,v 1.1 2008/10/16 09:28:33 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracProduction                    import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Production ID> [<DIRAC Status>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

diracProd = DiracProduction()
prodID = args[0]

stat = None
if len(args) == 2:
  stat = args[1]

result = diracProd.getProductionJobSummary(prodID,status=stat,printOutput=True)
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Getting production job summary failed with message:\n%s' %(result['Message'])
  DIRAC.exit(2)
else:
  print 'Null result for getProductionJobSummary() call'
  DIRAC.exit(2)