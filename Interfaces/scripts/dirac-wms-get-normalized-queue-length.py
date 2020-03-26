#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-wms-get-normalized-queue-length.py
# Author :  Ricardo Graciani
########################################################################
"""
  Report Normalized CPU length of queue

  This script was used by the dirac-pilot script to set the CPUTime limit for the matching but now this is no more the case
"""
from __future__ import print_function
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import queueNormalizedCPU

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

  normCPU = queueNormalizedCPU(ceUniqueID)

  if not normCPU['OK']:
    print('ERROR %s:' % ceUniqueID, normCPU['Message'])
    exitCode = 2
    continue
  print(ceUniqueID, normCPU['Value'])

DIRAC.exit(exitCode)
