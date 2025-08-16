#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-pilot-output
# Author :  Stuart Paterson
########################################################################
"""
Retrieve output of a Grid pilot

Example:
  $ dirac-admin-get-pilot-output https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw
  $ ls -la
  drwxr-xr-x  2 hamar marseill      2048 Feb 21 14:13 pilot_26KCLKBFtxXKHF4_ZrQjkw
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["PilotID:  Grid ID of the pilot"])
    _, args = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC import exit as DIRACExit

    print("This command is not supported anymore with DIRAV V9.")

    DIRACExit(0)


if __name__ == "__main__":
    main()
