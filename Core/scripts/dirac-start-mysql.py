#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-start-mysql
# Author :  Ricardo Graciani
########################################################################
"""
Start DIRAC MySQL server
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
print InstallTools.startMySQL()['Value'][1]
