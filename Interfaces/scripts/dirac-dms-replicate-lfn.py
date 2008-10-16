#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-dms-replicate-lfn.py,v 1.1 2008/10/16 09:28:33 paterson Exp $
# File :   dirac-dms-replicate-lfn
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-dms-replicate-lfn.py,v 1.1 2008/10/16 09:28:33 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                       import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <LFN> <DESTINATION SE> [<SOURCE SE> <LOCAL CACHE>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 2 or len(args) > 4 or len(args)==3:
  usage()

sourceSE=''
localCache=''
if len(args)>2:
  sourceSE=args[2]
  localCache=args[3]

dirac = Dirac()
exitCode = 0
result = dirac.replicateFile(args[0],args[1],sourceSE,localCache,printOutput=True)
if not result['OK']:
  print 'ERROR %s' %(result['Message'])
  exitCode = 2

DIRAC.exit(exitCode)