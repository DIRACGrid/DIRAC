#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-get-files
# Author :  Alexander Richards
########################################################################
"""
  Retrieve multiple wildcarded files from Grid storage.
"""
__RCSID__ = "$Id$"
import os
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s dir [dest] [wildcard] [days]' % Script.scriptName,
                                     'Arguments:',
                                     '  dir:      directory from which to get files',
                                     '  dest:     location to copy into [default: $PWD]',
                                     '  wildcard: optional pattern to match files against [default: *]',
                                     '  days:     optional consider files older than days [default: 0]'] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
dest = os.getcwd()
wildcard='*'
days=0
if len( args ) < 1:
  Script.showHelp()
if len(args) > 1:
  dest = args[1]
if len(args) > 2:
  wildcard = args[2]
if len(args) > 3:
  days = int(args[3])
from DIRAC.Interfaces.API.Dirac                       import Dirac
dirac = Dirac()
exitCode = 0

result = dirac.getFilesFromDirectory( args[0], destdir=dest, wildcard=wildcard, days=days )
if not result['OK']:
  print 'ERROR %s' % ( result['Message'] )
  exitCode = 2

print "Returned the following:", result['Value']
DIRAC.exit( exitCode )
