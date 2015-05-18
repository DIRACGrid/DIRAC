#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-install-web-portal
# Author :  Ricardo Graciani
########################################################################
"""
Do the initial installation of a DIRAC Web portal
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Utilities import InstallTools
from DIRAC import S_OK
#
InstallTools.exitOnError = True
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
  result = InstallTools.setupPortal()
else:
  result = InstallTools.setupNewPortal()  
