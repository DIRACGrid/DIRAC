#!/usr/bin/env python
# $HeadURL$
"""
Do the initial installation and configuration of the DIRAC MySQL server
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Utilities import InstallTools
#
InstallTools.exitOnError = True
#
InstallTools.getMySQLPasswords()
#
InstallTools.installMySQL()
#
InstallTools._addMySQLToDiracCfg()