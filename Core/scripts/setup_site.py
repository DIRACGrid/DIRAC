#!/usr/bin/env python
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/scripts/dirac-install.py $
"""
Do the initial installation and configuration of a new DIRAC site
"""
__RCSID__ = "$Id: dirac-install.py 26844 2010-07-16 08:44:22Z rgracian $"
#
from DIRAC.Core.Utilities import InstallTools
#
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.setUsageMessage( '\n'.join( ['Usage:',
                                    '%s [option] ... [cfgfile]' % Script.scriptName,
                                    'Arguments:',
                                    ' [<cfgfile>]: DIRAC Cfg with description of the configuration (optional)'] ) )
Script.parseCommandLine()
args = Script.getExtraCLICFGFiles()

def usage():
  Script.showHelp()
  exit( -1 )

if len( args ) > 1:
  usage()
  exit( -1 )
#
cfg = None
if len( args ):
  cfg = args[0]

InstallTools.setupSite( Script.localCfg, cfg )
