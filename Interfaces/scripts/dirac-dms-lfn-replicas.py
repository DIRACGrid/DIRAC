#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-lfn-replicas
# Author :  Stuart Paterson
########################################################################
"""
  Obtain replica information from file catalogue client.
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
lfns = Script.getPositionalArgs()

if len( lfns ) < 1:
  Script.showHelp()

from DIRAC.Interfaces.API.Dirac                         import Dirac
dirac = Dirac()
exitCode = 0

result = dirac.getReplicas( lfns, printOutput = True )
if not result['OK']:
  print 'ERROR: ', result['Message']
  exitCode = 2

DIRAC.exit( exitCode )
