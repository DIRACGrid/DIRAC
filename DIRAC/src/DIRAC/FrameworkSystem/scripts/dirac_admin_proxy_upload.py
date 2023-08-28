#!/usr/bin/env python
########################################################################
# File :    dirac-admin-proxy-upload.py
# Author :  Adrian Casajus
########################################################################
"""
Upload proxy.

Example:
  $ dirac-admin-proxy-upload
"""
import sys

from DIRAC.Core.Base.Script import Script
from DIRAC.FrameworkSystem.Client.ProxyUpload import CLIParams, uploadProxy


@Script()
def main():
    cliParams = CLIParams()
    cliParams.registerCLISwitches()

    Script.parseCommandLine()

    retVal = uploadProxy(cliParams)
    if not retVal["OK"]:
        print(retVal["Message"])
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
