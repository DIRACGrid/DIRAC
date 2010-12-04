#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-externals-versions
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.parseCommandLine( ignoreErrors = True )

diracAdmin = DiracAdmin()
diracAdmin.getExternalPackageVersions()
DIRAC.exit( 0 )

