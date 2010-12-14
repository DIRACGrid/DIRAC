#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-request-summary
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()

result = diracAdmin.getRequestSummary( printOutput = True )
if result['OK']:
  DIRAC.exit( 0 )
else:
  print result['Message']
  DIRAC.exit( 2 )

