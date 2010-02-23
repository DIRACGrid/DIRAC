#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-admin-lfn-replicas
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                         import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <LFN> [<LFN>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

dirac = Dirac()
exitCode = 0

result = dirac.getReplicas(args,printOutput=True)
if not result['OK']:
  exitCode = 2
  print "ERROR:", result['Message']

DIRAC.exit(exitCode)