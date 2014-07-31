#!/usr/bin/env python

#################################################
# $Id$
#################################################

""" The dirac-pilot-2.py script is a steering script to execute a series of
 pilot commands. The commands are provided in the pilot input sandbox in
 the pilotCommands.py module or in any <EXTENSION>Commands.py module.
 The pilot script defines two switches in order to choose a set of commands
 for the pilot:
 
 -E, --commandExtensions value
    where the value is a comma separated list of extension names. Modules
    with names <EXTENSION>Commands.py will be searched for the commands in
    the order defined in the value. By default no extensions are given
 -X, --commands value
    where value is a comma separated list of pilot commands. By default
    the list is InstallDIRAC,ConfigureDIRAC,LaunchAgent      
  
 The pilot script performs initial sanity checks on WN, installs and configures 
 DIRAC and runs the Job Agent to execute pending workloads in the DIRAC WMS.
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
  log.info( "Requested command extensions: %s" % str( pilotParams.commandExtensions ) )

if __name__ == "__main__":
  pythonPathCheck()
  for command in commands:
    command.execute()
