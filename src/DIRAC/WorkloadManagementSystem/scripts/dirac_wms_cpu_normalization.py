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
"""
from db12 import multiple_dirac_benchmark

import DIRAC
from DIRAC.Core.Base.Script import Script

from DIRAC import gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations


@Script()
def main():
    Script.registerSwitch("N:", "NumberOfProcessors=", "Run n parallel copies of the benchmark")
    Script.registerSwitch("U", "Update", "Update dirac.cfg with the resulting value")
    Script.registerSwitch("R:", "Reconfig=", "Update given configuration file with the resulting value")
    Script.parseCommandLine(ignoreErrors=True)

    update = False
    configFile = None
    numberOfProcessors = 0

    for unprocSw in Script.getUnprocessedSwitches():
        if unprocSw[0] in ("U", "Update"):
            update = True
        elif unprocSw[0] in ("R", "Reconfig"):
            configFile = unprocSw[1]
        elif unprocSw[0] in ("N", "NumberOfProcessors"):
            try:
                numberOfProcessors = int(unprocSw[1])
            except ValueError:
                gLogger.warn("Cannot make benchmark measurements: NumberOfProcessors is not a number")

    # if numberOfProcessors has not been provided, try to get it from the configuration
    if not numberOfProcessors:
        numberOfProcessors = gConfig.getValue("/Resources/Computing/CEDefaults/NumberOfProcessors", 1)

    gLogger.info("Computing benchmark measurements on", "%d processor(s)..." % numberOfProcessors)

    # we want to get the logs coming from db12
    gLogger.enableLogsFromExternalLibs()

    # multiprocessor allocations generally have a CPU Power lower than single core one.
    # in order to avoid having wrong estimations, we run multiple copies of the benchmark simultaneously
    result = multiple_dirac_benchmark(numberOfProcessors)

    if result is None:
        gLogger.error("Cannot make benchmark measurements")
        DIRAC.exit(1)

    # we take a conservative approach and use the minimum value returned as the CPU Power
    db12Result = min(result["raw"])
    # because hardware is continuously evolving, original benchmark scores might need a correction
    corr = Operations().getValue("JobScheduling/CPUNormalizationCorrection", 1.0)

    gLogger.info("Applying a correction on the CPU power:", corr)
    cpuPower = round(db12Result / corr, 1)

    gLogger.notice("Estimated CPU power is %.1f HS06" % cpuPower)

    if update:
        gConfig.setOptionValue("/LocalSite/CPUNormalizationFactor", cpuPower)

        if configFile:
            gConfig.dumpLocalCFGToFile(configFile)
        else:
            gConfig.dumpLocalCFGToFile(gConfig.diracConfigFilePath)

    DIRAC.exit()


if __name__ == "__main__":
    main()
