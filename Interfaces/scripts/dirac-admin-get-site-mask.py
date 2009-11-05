#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-admin-get-site-mask
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

result = diracAdmin.getSiteMask(printOutput=True)
if result['OK']:
  DIRAC.exit(0)
else:
  print result['Message']
  DIRAC.exit(2)

