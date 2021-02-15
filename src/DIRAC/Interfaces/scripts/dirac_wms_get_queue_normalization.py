#!/usr/bin/env python
########################################################################
# File :    dirac-wms-get-queue-normalization.py
# Author :  Ricardo Graciani
########################################################################
"""
Report Normalization Factor applied by Site to the given Queue

Usage:
  dirac-wms-get-queue-normalization [options] ... Queue ...

Arguments:
  Queue:     GlueCEUniqueID of the Queue (ie, juk.nikhef.nl:8443/cream-pbs-lhcb)

Example:
  $ dirac-wms-get-queue-normalization cclcgceli03.in2p3.fr:2119/jobmanager-bqs-long
  cclcgceli03.in2p3.fr:2119/jobmanager-bqs-long 2500.0
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  if len(args) < 1:
    Script.showHelp()

  exitCode = 0

  import DIRAC
  from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import getQueueNormalization

  for ceUniqueID in args:

    cpuNorm = getQueueNormalization(ceUniqueID)

    if not cpuNorm['OK']:
      print('ERROR %s:' % ceUniqueID, cpuNorm['Message'])
      exitCode = 2
      continue
    print(ceUniqueID, cpuNorm['Value'])

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
