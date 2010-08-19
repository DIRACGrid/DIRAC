#!/usr/bin/env python
# $HeadURL$
"""
Do the initial installation and configuration of the DIRAC MySQL server
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.setUsageMessage( '\n'.join( ['Start DIRAC MySQL server',
                                    'Usage:',
                                    '%s [option|cfgfile] ...' % Script.scriptName,
                                     ] ) )
Script.parseCommandLine()
#
from DIRAC.Core.Utilities import InstallTools
#
InstallTools.exitOnError = True
#
print InstallTools.startMySQL()['Value'][1]
