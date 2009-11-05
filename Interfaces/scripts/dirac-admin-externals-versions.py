#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-admin-externals-versions
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

diracAdmin = DiracAdmin()
diracAdmin.getExternalPackageVersions()
DIRAC.exit(0)

