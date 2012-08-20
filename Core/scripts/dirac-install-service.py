#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-install-service
# Author :  Ricardo Graciani
########################################################################
"""
Do the initial installation and configuration of a DIRAC service
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Utilities import InstallTools
from DIRAC.ConfigurationSystem.Client.Helpers import getCSExtensions
#
from DIRAC import gConfig, S_OK, S_ERROR
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script

overwrite = False
def setOverwrite( opVal ):
  global overwrite
  overwrite = True
  return S_OK()

module = ''
specialOptions = {}
def setModule( optVal ):
  global specialOptions,module
  specialOptions['Module'] = optVal
  module = optVal
  return S_OK()

def setSpecialOption( optVal ):
  global specialOptions
  option,value = optVal.split('=')
  specialOptions[option] = value
  return S_OK()

Script.registerSwitch( "w", "overwrite", "Overwrite the configuration in the global CS", setOverwrite )
Script.registerSwitch( "m:", "module=", "Python module name for the service code", setModule )
Script.registerSwitch( "p:", "parameter=", "Special service option ", setSpecialOption )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... System Service|System/Service' % Script.scriptName,
                                    'Arguments:',
                                    '  System:  Name of the DIRAC system (ie: WorkloadManagement)',
                                    '  Service: Name of the DIRAC service (ie: Matcher)'] ) )

Script.parseCommandLine()
args = Script.getPositionalArgs()

if len( args ) == 1:
  args = args[0].split( '/' )

if len( args ) != 2:
  Script.showHelp()
  exit( -1 )
#
system = args[0]
service = args[1]

if module:
  result = InstallTools.addDefaultOptionsToCS( gConfig, 'service', system, module,
                                               getCSExtensions(),
                                               overwrite = overwrite )
  result = InstallTools.addDefaultOptionsToCS( gConfig, 'service', system, service,
                                               getCSExtensions(),
                                               specialOptions=specialOptions,
                                               overwrite = overwrite,
                                               addDefaultOptions = False )
else:
  result = InstallTools.addDefaultOptionsToCS( gConfig, 'service', system, service,
                                               getCSExtensions(),
                                               specialOptions=specialOptions,
                                               overwrite = overwrite )
if not result['OK']:
  print "ERROR:", result['Message']
else:
  result = InstallTools.installComponent( 'service', system, service, getCSExtensions(), module )
  if not result['OK']:
    print "ERROR:", result['Message']
  else:
    print "Successfully installed service %s in %s system" % ( service, system )
