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
                                     '  LFN:      Logical File Name or file containing LFNs' ] ) )
Script.parseCommandLine( ignoreErrors = True )
lfns = Script.getPositionalArgs()

if len( lfns ) < 1:
  Script.showHelp()

from DIRAC.Interfaces.API.Dirac                       import Dirac
dirac = Dirac()
exitCode = 0

if len( lfns ) == 1:
  try:
    f = open( lfns[0], 'r' )
    lfns = f.read().splitlines()
    f.close()
  except:
    pass

for lfn in lfns:
  result = dirac.removeFile( lfn, printOutput = True )
  if not result['OK']:
    print 'ERROR %s: %s' % ( lfn, result['Message'] )
    exitCode = 2

DIRAC.exit( exitCode )
