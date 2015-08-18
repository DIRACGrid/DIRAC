#!/usr/bin/env python

"""
Do the initial installation and configuration of a DIRAC component
"""
__RCSID__ = "$Id$"

from DIRAC.ConfigurationSystem.Client.Helpers import getCSExtensions
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base import Script
from DIRAC import exit as DIRACexit
from DIRAC.ResourceStatusSystem.Utilities import Utils
InstallTools = getattr( Utils.voimport( 'DIRAC.Core.Utilities.InstallTools' ), 'InstallTools' )

InstallTools.exitOnError = True

overwrite = False
def setOverwrite( opVal ):
  global overwrite
  overwrite = True
  return S_OK()

module = ''
specialOptions = {}
def setModule( optVal ):
  global specialOptions, module
  specialOptions['Module'] = optVal
  module = optVal
  return S_OK()

def setSpecialOption( optVal ):
  global specialOptions
  option, value = optVal.split( '=' )
  specialOptions[option] = value
  return S_OK()

Script.registerSwitch( "w", "overwrite", "Overwrite the configuration in the global CS", setOverwrite )
Script.registerSwitch( "m:", "module=", "Python module name for the component code", setModule )
Script.registerSwitch( "p:", "parameter=", "Special component option ", setSpecialOption )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... CompType ( System Component|System/Component )' % Script.scriptName,
                                    'Arguments:',
                                    '  CompType: Type of the component to be installed',
                                    '  System:  Name of the DIRAC system (ie: WorkloadManagement)',
                                    '  Service: Name of the DIRAC component (ie: Matcher)'] ) )

Script.parseCommandLine()
args = Script.getPositionalArgs()

if len( args ) == 2:
  args = [ args[0] ] + args[1].split( '/' )

if len( args ) != 3:
  Script.showHelp()
  DIRACexit( 1 )

cType = args[0]
system = args[1]
component = args[2]

if module:
  result = InstallTools.addDefaultOptionsToCS( gConfig, cType, system, module,
                                               getCSExtensions(),
                                               overwrite = overwrite )
  result = InstallTools.addDefaultOptionsToCS( gConfig, cType, system, component,
                                               getCSExtensions(),
                                               specialOptions = specialOptions,
                                               overwrite = overwrite,
                                               addDefaultOptions = False )
else:
  result = InstallTools.addDefaultOptionsToCS( gConfig, cType, system, component,
                                               getCSExtensions(),
                                               specialOptions = specialOptions,
                                               overwrite = overwrite )
if not result[ 'OK' ]:
  gLogger.error( result[ 'Message' ] )
  DIRACexit( 1 )
else:
  result = InstallTools.installComponent( cType, system, component, getCSExtensions(), module )
  if not result[ 'OK' ]:
    gLogger.error( result[ 'Message' ] )
    DIRACexit( 1 )
  else:
    gLogger.notice( 'Successfully installed component %s in %s system, now setting it up' % ( component, system ) )
    result = InstallTools.setupComponent( cType, system, component, getCSExtensions(), module )
    if not result[ 'OK' ]:
      gLogger.error( result[ 'Message' ] )
      DIRACexit( 1 )
    if component == 'ComponentMonitoring':
        result = MonitoringUtilities.monitorInstallation( 'DB', system, 'InstalledComponentsDB' )
        if not result['OK']:
          gLogger.error( result[ 'Message' ] )
          DIRACexit( 1 )
    result = MonitoringUtilities.monitorInstallation( cType, system, component, module )
    if not result['OK']:
      gLogger.error( result[ 'Message' ] )
      DIRACexit( 1 )
    gLogger.notice( 'Successfully completed the installation of %s/%s' % ( system, component ) )
    DIRACexit()
