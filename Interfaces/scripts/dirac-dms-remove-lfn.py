#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-remove-lfn
# Author :  Stuart Paterson
########################################################################
"""
  Remove LFN and *all* associated replicas from Storage Elements and File Catalogs.
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

from DIRAC.Interfaces.API.Dirac                       import Dirac
dirac = Dirac()
exitCode = 0
for lfn in args:
  result = dirac.removeFile( lfn, printOutput = True )
  if not result['OK']:
    print 'ERROR %s: %s' % ( lfn, result['Message'] )
    exitCode = 2

DIRAC.exit( exitCode )
