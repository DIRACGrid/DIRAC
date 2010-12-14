#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-admin-get-pilot-output.py $
# File :    dirac-admin-get-pilot-output
# Author :  Ricardo Graciani
########################################################################
"""
  Retrieve available info about the given pilot
"""
__RCSID__ = "$Id: dirac-admin-get-pilot-output.py 18161 2009-11-11 12:07:09Z acasajus $"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... PilotID ...' % Script.scriptName,
                                     'Arguments:',
                                     '  PilotID:  Grid ID of the pilot' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

diracAdmin = DiracAdmin()
exitCode = 0
errorList = []

for gridID in args:

  result = diracAdmin.getPilotInfo( gridID )
  if not result['OK']:
    errorList.append( ( gridID, result['Message'] ) )
    exitCode = 2
  else:
    print diracAdmin.pPrint.pformat( result['Value'] )


for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
