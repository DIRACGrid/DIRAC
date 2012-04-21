#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-install-agent
# Author :  Ricardo Graciani
########################################################################
"""
Do the initial installation and configuration of a DIRAC agent
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Utilities import InstallTools
from DIRAC.ConfigurationSystem.Client.Helpers import getCSExtensions
#
from DIRAC import gConfig
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script

overwrite = False
def setOverwrite( opVal ):
  global overwrite
  overwrite = True
  return S_OK()

module = ''
def setModule( optVal ):
  global module
  module = optVal
  return S_OK()

specialOptions = {}
def setSpecialOption( optVal ):
  global specialOptions
  option,value = optVal.split('=')
  specialOptions[option] = value
  return S_OK()

Script.registerSwitch( "w", "overwrite", "Overwrite the configuration in the global CS", setOverwrite )
Script.registerSwitch( "m", "module", "Python module name for the agent code", setModule )
Script.registerSwitch( "p", "parameter", "Special agent option ", setSpecialOption )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... System Agent|System/Agent' % Script.scriptName,
                                    'Arguments:',
                                    '  System:  Name of the DIRAC system (ie: WorkloadManagement)',
                                    '  Agent:   Name of the DIRAC agent (ie: JobCleaningAgent)'] ) )
Script.parseCommandLine()
args = Script.getPositionalArgs()
if len( args ) == 1:
  args = args[0].split( '/' )

if len( args ) != 2:
  Script.showHelp()
  exit( -1 )
#
system = args[0]
agent = args[1]

result = InstallTools.addDefaultOptionsToCS( gConfig, 'agent', system, agent,
                                             getCSExtensions(), overwrite = overwrite )
if not result['OK']:
  print "ERROR:", result['Message']
else:
  result = InstallTools.installComponent( 'agent', system, agent, getCSExtensions() )
  if not result['OK']:
    print "ERROR:", result['Message']
  else:
    print "Successfully installed agent %s in %s system" % ( agent, system )
