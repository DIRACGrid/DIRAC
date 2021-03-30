#!/usr/bin/env python
"""
Determine Normalization for current CPU. The main users of this script are the pilot jobs.

Pilots invoke dirac-wms-cpu-normalization which
- runs 1 iteration of singleDiracBenchmark(1) (for single processors only)
- stores in local cfg the following::

    LocalSite
    {
      CPUNormalizationFactor = 23.7 # corrected value (by JobScheduling/CPUNormalizationCorrection)
      DB12measured = 15.4
    }

DB12measured is up to now wrote down but never used.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

from DIRAC import gLogger, gConfig
from DIRAC.WorkloadManagementSystem.Client.DIRACbenchmark import singleDiracBenchmark
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations


@DIRACScript()
def main():
  Script.registerSwitch("U", "Update", "Update dirac.cfg with the resulting value")
  Script.registerSwitch("R:", "Reconfig=", "Update given configuration file with the resulting value")
  Script.parseCommandLine(ignoreErrors=True)

  update = False
  configFile = None

  for unprocSw in Script.getUnprocessedSwitches():
    if unprocSw[0] in ("U", "Update"):
      update = True
    elif unprocSw[0] in ("R", "Reconfig"):
      configFile = unprocSw[1]

  result = singleDiracBenchmark(1)

  if result is None:
    gLogger.error('Cannot make benchmark measurements')
    DIRAC.exit(1)

  db12Measured = round(result['NORM'], 1)
  corr = Operations().getValue('JobScheduling/CPUNormalizationCorrection', 1.)
  norm = round(result['NORM'] / corr, 1)

  gLogger.notice('Estimated CPU power is %.1f HS06' % norm)

  if update:
    gConfig.setOptionValue('/LocalSite/CPUNormalizationFactor', norm)
    gConfig.setOptionValue('/LocalSite/DB12measured', db12Measured)

    if configFile:
      gConfig.dumpLocalCFGToFile(configFile)
    else:
      gConfig.dumpLocalCFGToFile(gConfig.diracConfigFilePath)

  DIRAC.exit()


if __name__ == "__main__":
  main()
