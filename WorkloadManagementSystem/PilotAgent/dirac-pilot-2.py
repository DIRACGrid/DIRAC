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

from pilotTools import Logger, pythonPathCheck, PilotParams
from pilotCommands import InstallDIRAC, ConfigureDIRAC, LaunchAgent

pilotParams = PilotParams()
pilotParams.pilotRootPath = os.getcwd()
pilotParams.pilotScript = os.path.realpath( sys.argv[0] )
pilotParams.pilotScriptName = os.path.basename( pilotParams.pilotScript )

log = Logger( 'Pilot' )
for o, _v in pilotParams.optList:
  if o == '-d' or o == '--debug':
    log.setDebug()

log.debug( 'PARAMETER [%s]' % ', '.join( map( str, pilotParams.optList ) ) )

def main():
  pythonPathCheck()

  # FIXME: Here there should be a command discovery mechanism a la ObjectLoader
  for com in ['InstallDIRAC','ConfigureDIRAC','LaunchAgent']:
    command = globals()[com]( pilotParams )
    command.execute()

if __name__ == "__main__":
    main()
