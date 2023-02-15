#!/usr/bin/env python
########################################################################
# File :   dirac-configuration-dump-local-cache
# Author : Adria Casajus
########################################################################
"""
Dump DIRAC Configuration data
"""
import sys
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.localCfg.addDefaultEntry("LogLevel", "fatal")

    fileName = ""

    def setFilename(args):
        nonlocal fileName
        fileName = args
        return DIRAC.S_OK()

    raw = False

    def setRaw(args):
        nonlocal raw
        raw = True
        return DIRAC.S_OK()

    Script.registerSwitch("f:", "file=", "Dump Configuration data into <file>", setFilename)
    Script.registerSwitch("r", "raw", "Do not make any modification to the data", setRaw)
    Script.parseCommandLine()

    from DIRAC import gConfig, gLogger

    result = gConfig.dumpCFGAsLocalCache(fileName, raw)
    if not result["OK"]:
        print(f"Error: {result['Message']}")
        sys.exit(1)

    if not fileName:
        print(result["Value"])

    sys.exit(0)


if __name__ == "__main__":
    main()
