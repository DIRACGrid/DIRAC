#!/usr/bin/env python
########################################################################
# File :    dirac-wms-get-queue-cpu-time.py
# Author :  Federico Stagni
########################################################################
"""
Report CPU length of queue, in seconds
This script is used by the dirac-pilot script to set the CPUTime left, which is a limit for the matching
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.registerSwitch("C:", "CPUNormalizationFactor=", "CPUNormalizationFactor, in case it is known")
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  CPUNormalizationFactor = 0.0
  for unprocSw in Script.getUnprocessedSwitches():
    if unprocSw[0] in ("C", "CPUNormalizationFactor"):
      CPUNormalizationFactor = float(unprocSw[1])

  from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import getCPUTime
  cpuTime = getCPUTime(CPUNormalizationFactor)
  # I hate this kind of output... PhC
  print("CPU time left determined as", cpuTime)
  DIRAC.exit(0)


if __name__ == "__main__":
  main()
