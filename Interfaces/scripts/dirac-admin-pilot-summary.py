#!/usr/bin/env python
########################################################################
# File :    dirac-admin-pilot-summary
# Author :  Stuart Paterson
########################################################################
"""
Usage:

  dirac-admin-pilot-summary (<options>|<cfgFile>)*

Example:

  $ dirac-admin-pilot-summary
  CE                          Status      Count       Status      Count       Status      Count       Status      Count       Status      Count         Status      Count       Status      Count
  sbgce1.in2p3.fr             Done        31
  lpsc-ce.in2p3.fr            Done        111
  lyogrid02.in2p3.fr          Done        81
  egee-ce.datagrid.jussieu.fr Aborted     81          Done        18
  cclcgceli03.in2p3.fr        Done        275
  marce01.in2p3.fr            Done        156
  node07.datagrid.cea.fr      Done        75
  cclcgceli01.in2p3.fr        Aborted     1           Done        235
  ce0.m3pec.u-bordeaux1.fr    Done        63
  grive11.ibcp.fr             Aborted     3           Done        90
  lptace01.msfg.fr            Aborted     3           Aborted_Day 3           Done        90
  ipnls2001.in2p3.fr          Done        87
  Total                       Aborted     89          Done        1423        Ready       0           Running     0           Scheduled   0              Submitted   0           Waiting     0
  lapp-ce01.in2p3.fr          Aborted     1           Done        111
"""
from __future__ import print_function
__RCSID__ = "$Id$"

# pylint: disable=wrong-import-position

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)
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
