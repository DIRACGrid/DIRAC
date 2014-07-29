#!/usr/bin/env python
# $HeadURL$
"""
 Perform initial sanity checks on WN, installs and configures DIRAC and runs
 Job Agent to execute pending workload on WMS.
 It requires dirac-install script to be sitting in the same directory.
"""
__RCSID__ = "$Id$"

import os
import getopt
import sys
from types import ListType

from pilotTools import Logger, pythonPathCheck, PilotParams, getCommands

pilotParams = PilotParams()
pilotParams.pilotRootPath = os.getcwd()
pilotParams.pilotScript = os.path.realpath( sys.argv[0] )
pilotParams.pilotScriptName = os.path.basename( pilotParams.pilotScript )

log = Logger( 'Pilot' )
if pilotParams.debugFlag:
  log.setDebug()
commands = getCommands( pilotParams )  
if type( commands ) != ListType:
  log.error( commands )
  sys.exit( -1 )

log.debug( 'PARAMETER [%s]' % ', '.join( map( str, pilotParams.optList ) ) )
log.info( "Executing commands: %s" % str( pilotParams.commands ) )
if pilotParams.commandExtensions:
  log.info( "Requested command extensions: %s" % str( params.commandExtensions ) )

def main():
  pythonPathCheck()
  for command in commands:
    command.execute()

if __name__ == "__main__":
    main()
