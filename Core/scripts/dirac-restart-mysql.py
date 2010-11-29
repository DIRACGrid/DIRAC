#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-restart-mysql
# Author : Ricardo Graciani
########################################################################
"""
  Restart DIRAC MySQL server
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName,
                                      ] ) )
Script.parseCommandLine()
#
from DIRAC.Core.Utilities import InstallTools
#
InstallTools.exitOnError = True
#
print InstallTools.stopMySQL()['Value'][1]
print InstallTools.startMySQL()['Value'][1]
