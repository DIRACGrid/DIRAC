#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-lfn-accessURL
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve an access URL for an LFN replica given a valid DIRAC SE.
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... LFN SE' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name or file containing LFNs',
                                     '  SE:       Valid DIRAC SE' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 2:
  Script.showHelp()

if len( args ) > 2:
  print 'Only one LFN SE pair will be considered'

from DIRAC.Interfaces.API.Dirac                         import Dirac
dirac = Dirac()
exitCode = 0

lfn = args[0]
seName = args[1]

try:
  f = open( lfn, 'r' )
  lfns = f.read().splitlines()
  f.close()
except:
  lfns = [lfn]

for lfn in lfns:
  result = dirac.getAccessURL( lfn, seName, printOutput = True )
  if not result['OK']:
    print 'ERROR: ', result['Message']
    exitCode = 2

DIRAC.exit( exitCode )
