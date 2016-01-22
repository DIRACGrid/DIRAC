#!/usr/bin/env python
########################################################################
# File :    dirac-install-web-portal
# Author :  Ricardo Graciani
########################################################################
"""
Do the initial installation of a DIRAC Web portal
"""
__RCSID__ = "$Id$"
#
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
from DIRAC import S_OK
#
gComponentInstaller.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName,
                                    'Arguments:',] ) )

old = False
def setOld( opVal ):
  global old
  old = True
  return S_OK()

Script.registerSwitch( "O", "--old", "install old Pylons based portal", setOld )

Script.parseCommandLine()

if old:
  result = gComponentInstaller.setupPortal()
else:
  result = gComponentInstaller.setupNewPortal()
