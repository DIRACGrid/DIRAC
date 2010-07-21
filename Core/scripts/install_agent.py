#!/usr/bin/env python
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/scripts/dirac-install.py $
"""
Do the initial installation and configuration of a DIRAC agent
"""
__RCSID__ = "$Id: dirac-install.py 26844 2010-07-16 08:44:22Z rgracian $"
#
from DIRAC.Core.Utilities import InstallTools
#
from DIRAC import gConfig
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

def usage():
  Script.showHelp( '\n'.join( ['Arguments:',
                               '  <system> Name of the system (mandatory)',
                               '  <agent> Name of the Agent (mandatory)'] ) )
  exit( -1 )

if len( args ) != 2:
  usage()
  exit( -1 )
#
system = args[0]
agent = args[1]
extensions = gConfig.getValue( '/DIRAC/Extensions', [] )

InstallTools.addDefaultOptionsToCS( gConfig, 'agent', system, agent, extensions, True )

InstallTools.installComponent( 'agent', system, agent, extensions )
