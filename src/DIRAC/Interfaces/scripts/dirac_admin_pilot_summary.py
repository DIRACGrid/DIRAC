#!/usr/bin/env python
########################################################################
# File :    dirac-admin-pilot-summary
# Author :  Stuart Paterson
########################################################################

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

# pylint: disable=wrong-import-position

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()

  result = diracAdmin.getPilotSummary()
  if result['OK']:
    DIRAC.exit(0)
  else:
    print(result['Message'])
    DIRAC.exit(2)


if __name__ == "__main__":
  main()
