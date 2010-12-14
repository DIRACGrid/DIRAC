#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-externals-versions
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()
diracAdmin.getExternalPackageVersions()
DIRAC.exit( 0 )

