#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-add-file
# Author :  Stuart Paterson
########################################################################
"""
  Upload a file to the grid storage and register it in the File Catalog
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... LFN Path SE [GUID]' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name',
                                     '  Path:     Local path to the file',
                                     '  SE:       DIRAC Storage Element',
                                     '  GUID:     GUID to use in the registration (optional)' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

import DIRAC

if len( args ) < 3 or len( args ) > 4:
  Script.showHelp()

guid = None
if len( args ) > 3:
  guid = args[3]

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()
exitCode = 0
result = dirac.addFile( args[0], args[1], args[2], guid, printOutput = True )
if not result['OK']:
  print 'ERROR %s' % ( result['Message'] )
  exitCode = 2

DIRAC.exit( exitCode )
