#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-install-mysql
# Author :  Ricardo Graciani
########################################################################
"""
Install MySQL. The clever way to do this is to use the
dirac-admin-sysadmin-cli.

Do the initial installation and configuration of the DIRAC MySQL server
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)

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
