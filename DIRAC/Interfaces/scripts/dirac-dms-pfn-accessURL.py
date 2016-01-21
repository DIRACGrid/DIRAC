#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-pfn-accessURL
# Author  : Stuart Paterson
########################################################################
"""
  Retrieve an access URL for a PFN given a valid DIRAC SE
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... PFN SE' % Script.scriptName,
                                     'Arguments:',
                                     '  PFN:      Physical File Name or file containing PFNs',
                                     '  SE:       Valid DIRAC SE' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 2:
  Script.showHelp()

if len( args ) > 2:
  print 'Only one PFN SE pair will be considered'

from DIRAC.Interfaces.API.Dirac                         import Dirac
dirac = Dirac()
exitCode = 0

pfn = args[0]
seName = args[1]
try:
  f = open( pfn, 'r' )
  pfns = f.read().splitlines()
  f.close()
except:
  pfns = [pfn]

for pfn in pfns:
  result = dirac.getPhysicalFileAccessURL( pfn, seName, printOutput = True )
  if not result['OK']:
    print 'ERROR: ', result['Message']
    exitCode = 2

DIRAC.exit( exitCode )
