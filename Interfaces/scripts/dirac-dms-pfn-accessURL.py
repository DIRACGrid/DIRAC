#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-dms-pfn-accessURL.py,v 1.1 2008/10/16 09:28:33 paterson Exp $
# File :   dirac-dms-pfn-accessURL
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-dms-pfn-accessURL.py,v 1.1 2008/10/16 09:28:33 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                         import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <PFN> <SE>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 2:
  usage()

if len(args) > 2:
  print 'Only one PFN SE pair will be considered'

dirac = Dirac()
exitCode = 0
errorList = []

result = dirac.getPhysicalFileAccessURL(args[0],args[1],printOutput=True)
if not result['OK']:
  errorList.append( (args[0], result['Message']) )
  exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)