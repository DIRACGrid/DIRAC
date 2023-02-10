#!/usr/bin/env python

""" Performs a DIPS ping on a given target and exit with the return code.
It uses the local host certificate
The target is specified as ""<port>/System/Service"
The script does not print anything, and just exists with 0 in case of success,
or 1 in case of error """

import sys
import os
import time
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.parseCommandLine(ignoreErrors=True)
    with open(os.devnull, "w") as redirectStdout, open(os.devnull, "w") as redirectStderr:
        from DIRAC import gLogger
        from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

        gConfigurationData.setOptionInCFG("/DIRAC/Security/UseServerCertificate", "true")
        gLogger.setLevel("FATAL")
        from DIRAC.Core.Base.Client import Client

        rpc = Client(url=f"dips://localhost:{sys.argv[1]}")
        res = rpc.ping()
        time.sleep(0.1)
        sys.exit(0 if res["OK"] else 1)


if __name__ == "__main__":
    main()
