#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-dms-replicate.py $
# File :    dirac-dms-replicate-lfn
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id: dirac-dms-replicate-lfn.py 18161 2009-11-11 12:07:09Z acasajus $"
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                       import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <LFN> <DESTINATION SE> [<SOURCE SE>]' % ( Script.scriptName )
  DIRAC.exit( 2 )

if len( args ) < 2 or len( args ) > 3:
  usage()

sourceSE = ''
localCache = ''
if len( args ) > 2:
  sourceSE = args[2]
  localCache = args[3]

dirac = Dirac()
exitCode = 0
result = dirac.replicate( args[0], args[1], sourceSE, printOutput = True )
if not result['OK']:
  print 'ERROR %s' % ( result['Message'] )
  exitCode = 2

DIRAC.exit( exitCode )
