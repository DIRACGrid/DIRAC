#!/usr/bin/env python
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/scripts/dirac-install.py $
"""
Do the initial installation and configuration of the DIRAC MySQL server
"""
__RCSID__ = "$Id: dirac-install.py 26844 2010-07-16 08:44:22Z rgracian $"
#
from DIRAC.Core.Utilities import InstallTools
#
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.disableCS()
#
print InstallTools.startMySQL()['Value'][1]

