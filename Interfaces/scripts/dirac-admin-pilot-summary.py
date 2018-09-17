#!/usr/bin/env python
########################################################################
# File :    dirac-admin-pilot-summary
# Author :  Stuart Paterson
########################################################################

__RCSID__ = "$Id$"

# pylint: disable=wrong-import-position

import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
diracAdmin = DiracAdmin()

result = diracAdmin.getPilotSummary()
if result['OK']:
  DIRAC.exit(0)
else:
  print result['Message']
  DIRAC.exit(2)
