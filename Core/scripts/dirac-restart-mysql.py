#!/usr/bin/env python
########################################################################
# File :   dirac-restart-mysql
# Author : Ricardo Graciani
########################################################################
"""
  Restart DIRAC MySQL server
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
print(gComponentInstaller.stopMySQL()['Value'][1])
print(gComponentInstaller.startMySQL()['Value'][1])
