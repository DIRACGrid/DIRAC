#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-admin-externals-versions.py,v 1.1 2008/10/16 09:21:28 paterson Exp $
# File :   dirac-admin-externals-versions
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-admin-externals-versions.py,v 1.1 2008/10/16 09:21:28 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

diracAdmin = DiracAdmin()
diracAdmin.getExternalPackageVersions()
DIRAC.exit(0)

