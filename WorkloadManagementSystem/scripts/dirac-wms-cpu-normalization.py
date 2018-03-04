#!/usr/bin/env python
########################################################################
# File :    dirac-wms-cpu-normalization
# Author :  Andrew McNab
########################################################################
"""
  Determine Normalization for current CPU
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch("U", "Update", "Update dirac.cfg with the resulting value")
Script.registerSwitch("R:", "Reconfig=", "Update given configuration file with the resulting value")

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ' % Script.scriptName]))

Script.parseCommandLine(ignoreErrors=True)

update = False
configFile = None

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ("U", "Update"):
    update = True
  elif unprocSw[0] in ("R", "Reconfig"):
    configFile = unprocSw[1]


if __name__ == "__main__":

  from DIRAC import gLogger, gConfig
  from DIRAC.WorkloadManagementSystem.Client.DIRACbenchmark import singleDiracBenchmark
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  from DIRAC.Core.Utilities import MJF

  mjf = MJF.MJF()
  mjf.updateConfig()

  db12JobFeature = mjf.getJobFeature('db12')
  hs06JobFeature = mjf.getJobFeature('hs06')

  result = singleDiracBenchmark(1)

  if result is None:
    gLogger.error('Cannot make benchmark measurements')
    DIRAC.exit(1)

  db12Measured = round(result['NORM'], 1)
  corr = Operations().getValue('JobScheduling/CPUNormalizationCorrection', 1.)
  norm = round(result['NORM'] / corr, 1)

  gLogger.notice('Estimated CPU power is %.1f HS06' % norm)

  if update:
    gConfig.setOptionValue('/LocalSite/CPUScalingFactor', hs06JobFeature if hs06JobFeature else norm)  # deprecate?
    gConfig.setOptionValue('/LocalSite/CPUNormalizationFactor', norm)  # deprecate?
    gConfig.setOptionValue('/LocalSite/DB12measured', db12Measured)

    # Set DB12 to use by default. Remember db12JobFeature is still in /LocalSite/JOBFEATURES/db12
    if db12JobFeature is not None:
      gConfig.setOptionValue('/LocalSite/DB12', db12JobFeature)
    else:
      gConfig.setOptionValue('/LocalSite/DB12', db12Measured)

    if configFile:
      gConfig.dumpLocalCFGToFile(configFile)
    else:
      gConfig.dumpLocalCFGToFile(gConfig.diracConfigFilePath)

  DIRAC.exit()
