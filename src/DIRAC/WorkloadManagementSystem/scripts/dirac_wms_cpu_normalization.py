#!/usr/bin/env python
"""
Determine Normalization for current CPU. The main users of this script are the pilot jobs.

Pilots invoke dirac-wms-cpu-normalization which

- looks up the per-core HS23 score of the node in the HEPiX HEPScore23 table (see
  HS23Table.py), and stores it in the local cfg as ``CPUNormalizationFactorHS23``, tagging
  ``CPUNormalizationSource`` with the provenance of the value;
- runs, when required, 1 iteration of singleDiracBenchmark(1) (for single processors only) and
  stores in local cfg the following::

    LocalSite
    {
      CPUNormalizationFactor = 23.7 # corrected value (by JobScheduling/CPUNormalizationCorrection)
      CPUNormalizationFactorHS23 = 21.5 # per-core HS23 score from the HEPiX table
      CPUNormalizationSource = HS23table | DB12 # provenance of the node's CPU power
    }

Which of the two factors is to be used downstream is governed by the CS option
``JobScheduling/CPUNormalizationUnit`` (``DB12`` or ``HS23``, default ``DB12``); whether DB12 is
run at all is governed by ``JobScheduling/RunDB12Benchmark`` (unset: run it iff the unit is DB12).
DB12 is always run as fallback when the HS23 lookup fails. In this first iteration nothing
consumes ``CPUNormalizationFactorHS23``: the value is only recorded and reported (see
docs/adr/0001-hs23-cpu-power-from-hepix-table.md).
"""
from db12 import multiple_dirac_benchmark

import DIRAC
from DIRAC import gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.Script import Script
from DIRAC.WorkloadManagementSystem.Client.HS23Table import getHS23Score


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

    operations = Operations()
    unit = operations.getValue("JobScheduling/CPUNormalizationUnit", "DB12")
    if unit not in ("DB12", "HS23"):
        gLogger.warn(f"Unknown JobScheduling/CPUNormalizationUnit {unit!r}, falling back to DB12")
        unit = "DB12"
    runDB12Benchmark = operations.getValue("JobScheduling/RunDB12Benchmark", None)

    # if numberOfProcessors has not been provided, try to get it from the configuration
    if not numberOfProcessors:
        numberOfProcessors = gConfig.getValue("/Resources/Computing/CEDefaults/NumberOfProcessors", 1)

    # 1. HS23 score from the HEPiX table
    hs23Score = None
    result = getHS23Score()
    if result["OK"]:
        hs23Score = result["Value"]
        gLogger.notice(
            f"HS23 score is {hs23Score['Score']} (from {hs23Score['MatchedRows']} row(s) of the HEPiX table,"
            f" SMT {'enabled' if hs23Score['SMT'] else 'disabled'})"
        )
    else:
        gLogger.warn("Cannot get the HS23 score:", result["Message"])

    # 2. DB12 measurement: always the fallback when the HS23 lookup failed, otherwise
    #    governed by RunDB12Benchmark (unset: run it iff the unit is DB12)
    if runDB12Benchmark is None:
        needDB12 = hs23Score is None or unit == "DB12"
    else:
        needDB12 = runDB12Benchmark or hs23Score is None

    cpuPower = None
    if needDB12:
        gLogger.info("Computing benchmark measurements on", "%d processor(s)..." % numberOfProcessors)

        # multiprocessor allocations generally have a CPU Power lower than single core one.
        # in order to avoid having wrong estimations, we run multiple copies of the benchmark simultaneously
        result = multiple_dirac_benchmark(numberOfProcessors)

        if result is not None:
            # we take a conservative approach and use the minimum value returned as the CPU Power
            db12Result = min(result["raw"])
            # because hardware is continuously evolving, original benchmark scores might need a correction
            corr = operations.getValue("JobScheduling/CPUNormalizationCorrection", 1.0)

            gLogger.info("Applying a correction on the CPU power:", corr)
            cpuPower = round(db12Result / corr, 1)
            gLogger.notice(f"Estimated CPU power is {cpuPower:.1f} HS06")
        else:
            gLogger.error("Cannot make benchmark measurements")
            if hs23Score is None or unit == "DB12":
                # no DB12 and it is the (only) value for the active unit: this is fatal for a pilot
                DIRAC.exit(1)

    if not update:
        DIRAC.exit()

    # 3. Write everything to the local configuration
    if hs23Score is not None:
        gConfig.setOptionValue("/LocalSite/CPUNormalizationFactorHS23", hs23Score["Score"])
    if cpuPower is not None or hs23Score is not None:
        gConfig.setOptionValue("/LocalSite/CPUNormalizationSource", "HS23table" if hs23Score is not None else "DB12")
    if cpuPower is not None:
        gConfig.setOptionValue("/LocalSite/CPUNormalizationFactor", cpuPower)

    if configFile:
        gConfig.dumpLocalCFGToFile(configFile)
    else:
        gConfig.dumpLocalCFGToFile(gConfig.diracConfigFilePath)

    DIRAC.exit()


if __name__ == "__main__":
    main()
