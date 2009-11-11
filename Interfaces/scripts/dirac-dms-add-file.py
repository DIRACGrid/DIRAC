#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-dms-add-file
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                       import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <LFN> <FILE PATH> <DIRAC SE> [<GUID>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 3 or len(args) > 4:
  usage()

guid = None
if len(args)>3:
  guid = args[3]

dirac = Dirac()
exitCode = 0
result = dirac.addFile(args[0],args[1],args[2],guid,printOutput=True)
if not result['OK']:
  print 'ERROR %s' %(result['Message'])
  exitCode = 2

DIRAC.exit(exitCode)