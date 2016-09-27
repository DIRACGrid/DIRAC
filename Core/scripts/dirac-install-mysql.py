#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-install-mysql
# Author :  Ricardo Graciani
########################################################################
"""
Do the initial installation and configuration of the DIRAC MySQL server
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1] ] ) )

Script.parseCommandLine()


#
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
#
gComponentInstaller.exitOnError = True
#
gComponentInstaller.getMySQLPasswords()
#
gComponentInstaller.installMySQL()
#
gComponentInstaller._addMySQLToDiracCfg()
