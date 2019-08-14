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
from DIRAC.Core.Base import Script
Script.disableCS()
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ...' % Script.scriptName]))

Script.parseCommandLine()

from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

gComponentInstaller.exitOnError = True


gComponentInstaller.setupPortal()
