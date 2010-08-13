#!/usr/bin/env python
# $HeadURL$
"""
Do the initial installation of a DIRAC Web portal
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Utilities import InstallTools
#
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.parseCommandLine()

InstallTools.installPortal()
