#!/usr/bin/env python

"""
Uninstallation of a DIRAC component
"""

__RCSID__ = "$Id$"

import socket
from DIRAC.ConfigurationSystem.Client.Helpers import getCSExtensions
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.PromptUser import promptUser
from DIRAC import exit as DIRACexit
import DIRAC.Core.Utilities.InstallTools as InstallTools

InstallTools.exitOnError = True

force = False
def setForce( opVal ):
  global force
  force = True
  return S_OK()

Script.registerSwitch( "f", "force", "Forces the removal of the logs", setForce )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... System Component|System/Component' % Script.scriptName,
                                    'Arguments:',
                                    '  System:  Name of the DIRAC system (ie: WorkloadManagement)',
                                    '  Component: Name of the DIRAC component (ie: Matcher)'] ) )

Script.parseCommandLine()
args = Script.getPositionalArgs()

if len( args ) == 1:
  args = args[0].split( '/' )

if len( args ) < 2:
  Script.showHelp()
  DIRACexit( 1 )

system = args[0]
component = args[1]

monitoringClient = ComponentMonitoringClient()
result = monitoringClient.getInstallations( { 'Instance': component, 'UnInstallationTime': None },
                                                  { 'System': system },
                                                  { 'HostName': socket.getfqdn() }, True )
if not result[ 'OK' ]:
  gLogger.error( result[ 'Message' ] )
  DIRACexit( 1 )
if len( result[ 'Value' ] ) < 1:
  gLogger.error( 'Given component does not exist' )
  DIRACexit( 1 )
if len( result[ 'Value' ] ) > 1:
  gLogger.error( 'Too many components match' )
  DIRACexit( 1 )

removeLogs = False
if force:
  removeLogs = True
else:
  if result[ 'Value' ][0][ 'Component' ][ 'Type' ] in InstallTools.COMPONENT_TYPES:
    result = promptUser( 'Remove logs?', [ 'y', 'n' ], 'n' )
    if result[ 'OK' ]:
      removeLogs = result[ 'Value' ] == 'y'
    else:
      gLogger.error( result[ 'Message' ] )
      DIRACexit( 1 )

result = InstallTools.uninstallComponent( system, component, removeLogs )
if not result['OK']:
  gLogger.error( result[ 'Message' ] )
  DIRACexit( 1 )

result = MonitoringUtilities.monitorUninstallation( system, component )
if not result['OK']:
  gLogger.error( result[ 'Message' ] )
  DIRACexit( 1 )
gLogger.notice( 'Successfully uninstalled component %s/%s' % ( system, component ) )
DIRACexit()
