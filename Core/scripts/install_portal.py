#!/usr/bin/env python
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/scripts/dirac-install.py $
"""
Do the initial installation of a DIRAC Web portal
"""
__RCSID__ = "$Id: dirac-install.py 26844 2010-07-16 08:44:22Z rgracian $"
#
from DIRAC.Core.Utilities import InstallTools
#
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.parseCommandLine()

InstallTools.installPortal()
