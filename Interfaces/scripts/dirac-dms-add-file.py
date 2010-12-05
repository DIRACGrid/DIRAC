#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-add-file
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Upload a file to the grid storage and register it in the File Catalog

Usage:
   %s <LFN> <FILE PATH> <DIRAC SE> [<GUID>]
""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

import DIRAC
from DIRAC.Interfaces.API.Dirac import Dirac    

if len( args ) < 3 or len( args ) > 4:
  Script.showHelp()
  DIRAC.exit( 2 )

guid = None
if len( args ) > 3:
  guid = args[3]

dirac = Dirac()
exitCode = 0
result = dirac.addFile( args[0], args[1], args[2], guid, printOutput = True )
if not result['OK']:
  print 'ERROR %s' % ( result['Message'] )
  exitCode = 2

DIRAC.exit( exitCode )
