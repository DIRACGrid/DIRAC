#!/usr/bin/env python
########################################################################
# File :    dirac-admin-pilot-summary
# Author :  Stuart Paterson
########################################################################
"""
Usage:
  dirac-admin-pilot-summary [options] ...

Example:
  $ dirac-admin-pilot-summary
  CE                Status  Count Status Count Status Count Status  Count Status    Count Status    Count Status  Count
  sbgce1.in2p3.fr   Done    31
  lpsc-ce.in2p3.fr  Done    111
  lyogrid2.in2p3.fr Done    81
  egee-ce.jusieu.fr Aborted 81    Done   18
  cclcgce3.in2p3.fr Done    275
  marce01.in2p3.fr  Done    156
  node7.datagrid.fr Done    75
  cclcgceli01.fr    Aborted 1     Done   235
  ce0.m3pec.u-bo.fr Done    63
  grive11.ibcp.fr   Aborted 3     Done   90
  lptace01.msfg.fr  Aborted 3     Done   3     Done   90
  ipnls1.in2p3.fr   Done    87
  Total             Aborted 89    Done   1423  Ready  0     Running 0     Scheduled 0     Submitted 0     Waiting 0
  lappce01.in2p3.fr Aborted 1     Done   111
"""
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
