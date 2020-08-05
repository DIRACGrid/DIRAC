#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-get-file
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve a single file or list of files from Grid storage to the current directory.
"""
from __future__ import print_function
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

result = dirac.getFile( lfns, printOutput = True )
if not result['OK']:
  print('ERROR %s' % (result['Message']))
  exitCode = 2

DIRAC.exit( exitCode )
