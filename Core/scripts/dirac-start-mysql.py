#!/usr/bin/env python
########################################################################
# File :    dirac-start-mysql
# Author :  Ricardo Graciani
########################################################################
"""
Start DIRAC MySQL server
"""
from __future__ import print_function
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
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
#
gComponentInstaller.exitOnError = True
#
print(gComponentInstaller.startMySQL()['Value'][1])
