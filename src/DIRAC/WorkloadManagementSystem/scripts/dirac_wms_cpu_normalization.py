#!/usr/bin/env python
"""
Determine Normalization for current CPU. The main users of this script are the pilot jobs.

Pilots invoke dirac-wms-cpu-normalization which

- looks up the per-core HS23 score of the node in the HEPiX HEPScore23 table bundled with DIRAC
  (see HS23Table.py) and stores it in the local cfg as ``CPUNormalizationFactorHS23``;
- runs, when required, 1 iteration of singleDiracBenchmark(1) (for single processors only) and
  stores in local cfg the following::

    LocalSite
    {
      CPUNormalizationFactor = 23.7 # corrected value (by JobScheduling/CPUNormalizationCorrection)
      CPUNormalizationFactorHS23 = 21.5 # per-core HS23 score from the bundled table
      CPUNormalizationSource = HS23table | DB12 # provenance of the active unit's value
    }

What is measured, looked up and used is governed by the CS option
``JobScheduling/CPUNormalizationUnit``, an ordered list whose default is ``["DB12"]``.

- **membership**: every unit in the list is recorded (DB12 measured, HS23 looked up) and
  published as job parameters;
- **order**: the first entry is the *active* unit, the one consumers will honor (see
  docs/adr/0001-hs23-cpu-power-from-hepix-table.md). ``["DB12"]`` (the default) is pure legacy
  behavior; ``["HS23", "DB12"]`` records both with HS23 active; ``["DB12", "HS23"]`` records
  both with DB12 active.

When the active unit is HS23 and the lookup fails (CPU model not in the table, e.g. brand new
hardware), DB12 is measured as fallback regardless of the list membership. The choice is
setup-global: it applies to all sites, there is no per-site override.

In this first iteration nothing consumes ``CPUNormalizationFactorHS23``: with the default
``["DB12"]``, consumers keep using ``CPUNormalizationFactor`` exactly as before, so sites
should not put HS23 first until phase 2.
"""

from db12 import multiple_dirac_benchmark

import DIRAC
from DIRAC import gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.Script import Script
from DIRAC.WorkloadManagementSystem.Client.HS23Table import getHS23Score

_KNOWN_UNITS = ("DB12", "HS23")


def _getUnitList():
    """Read and validate ``JobScheduling/CPUNormalizationUnit``.

    :return: deduplicated list of known units, preserving order, non-empty
    """
    units = Operations().getValue("JobScheduling/CPUNormalizationUnit", ["DB12"])
    if isinstance(units, str):
        units = [units]

    unitList = []
    for unit in units:
        if unit not in _KNOWN_UNITS:
            gLogger.warn("Ignoring unknown unit in JobScheduling/CPUNormalizationUnit:", unit)
            continue
        if unit not in unitList:
            unitList.append(unit)
    if not unitList:
        gLogger.warn("Empty or invalid JobScheduling/CPUNormalizationUnit, falling back to DB12")
        unitList = ["DB12"]
    return unitList


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

    unitList = _getUnitList()
    activeUnit = unitList[0]

    # if numberOfProcessors has not been provided, try to get it from the configuration
    if not numberOfProcessors:
        numberOfProcessors = gConfig.getValue("/Resources/Computing/CEDefaults/NumberOfProcessors", 1)

    # 1. HS23 score from the bundled table, when HS23 is in the list
    hs23Score = None
    if "HS23" in unitList:
        result = getHS23Score()
        if result["OK"]:
            hs23Score = result["Value"]
            gLogger.notice(f"HS23 score is {hs23Score['Score']} (model {hs23Score['Model']!r})")
        else:
            gLogger.warn("Cannot get the HS23 score:", result["Message"])

    # 2. DB12 measurement: when DB12 is in the list, or as fallback when the active
    #    unit is HS23 and the lookup failed
    cpuPower = None
    if "DB12" in unitList or hs23Score is None:
        gLogger.info("Computing benchmark measurements on", "%d processor(s)..." % numberOfProcessors)

        # multiprocessor allocations generally have a CPU Power lower than single core one.
        # in order to avoid having wrong estimations, we run multiple copies of the benchmark simultaneously
        result = multiple_dirac_benchmark(numberOfProcessors)

        if result is not None:
            # we take a conservative approach and use the minimum value returned as the CPU Power
            db12Result = min(result["raw"])
            # because hardware is continuously evolving, original benchmark scores might need a correction
            corr = Operations().getValue("JobScheduling/CPUNormalizationCorrection", 1.0)

            gLogger.info("Applying a correction on the CPU power:", corr)
            cpuPower = round(db12Result / corr, 1)
            gLogger.notice(f"Estimated CPU power is {cpuPower:.1f} HS06")
        else:
            gLogger.error("Cannot make benchmark measurements")
            if hs23Score is None or activeUnit == "DB12":
                # no DB12 and the active unit has no value: this is fatal for a pilot
                DIRAC.exit(1)

    # 3. The provenance is the source of the active unit's value
    source = "DB12"
    if activeUnit == "HS23" and hs23Score is not None:
        source = "HS23table"

    if not update:
        DIRAC.exit()

    # 4. Write everything to the local configuration
    if cpuPower is not None:
        gConfig.setOptionValue("/LocalSite/CPUNormalizationFactor", cpuPower)
    if hs23Score is not None:
        gConfig.setOptionValue("/LocalSite/CPUNormalizationFactorHS23", hs23Score["Score"])
    gConfig.setOptionValue("/LocalSite/CPUNormalizationSource", source)

    if configFile:
        gConfig.dumpLocalCFGToFile(configFile)
    else:
        gConfig.dumpLocalCFGToFile(gConfig.diracConfigFilePath)

    DIRAC.exit()


if __name__ == "__main__":
    main()
