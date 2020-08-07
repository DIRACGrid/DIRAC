#!/usr/bin/env python
########################################################################
# File :    dirac-wms-get-queue-normalization.py
# Author :  Ricardo Graciani
########################################################################
"""
  Report Normalization Factor applied by Site to the given Queue
"""
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import getQueueNormalization

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... Queue ...' % Script.scriptName,
                                  'Arguments:',
                                  '  Queue:     GlueCEUniqueID of the Queue (ie, juk.nikhef.nl:8443/cream-pbs-lhcb)']))
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) < 1:
  Script.showHelp()

exitCode = 0

for ceUniqueID in args:

  cpuNorm = getQueueNormalization(ceUniqueID)

  if not cpuNorm['OK']:
    print('ERROR %s:' % ceUniqueID, cpuNorm['Message'])
    exitCode = 2
    continue
  print(ceUniqueID, cpuNorm['Value'])

DIRAC.exit(exitCode)
