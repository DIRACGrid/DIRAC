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
#
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName,
                                    'Arguments:',] ) )

Script.parseCommandLine()

InstallTools.installPortal()
