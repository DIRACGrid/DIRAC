#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-lfn-logging-ingo
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve logging information for a given LFN
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... LFN ...' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

from DIRAC.Interfaces.API.Dirac                              import Dirac
dirac = Dirac()
exitCode = 0
errorList = []

for lfn in args:

  result = dirac.dataLoggingInfo( lfn, printOutput = True )
  if not result['OK']:
    errorList.append( ( lfn, result['Message'] ) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
