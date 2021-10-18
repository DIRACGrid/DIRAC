#!/usr/bin/env python
########################################################################
# File :    dirac-utils-file-md5
# Author :
########################################################################
"""
Calculate md5 of the supplied file

Example:
  $ dirac-utils-file-md5 Example.tgz
  Example.tgz 5C1A1102-EAFD-2CBA-25BD-0EFCCFC3623E
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["File:     File Name"])
    _, files = Script.parseCommandLine(ignoreErrors=False)

    exitCode = 0

    import DIRAC
    from DIRAC.Core.Utilities.File import makeGuid

    for file in files:
        try:
            md5 = makeGuid(file)
            if md5:
                print(file.rjust(100), md5.ljust(10))
            else:
                print("ERROR %s: Failed to get md5" % file)
                exitCode = 2
        except Exception as x:
            print("ERROR %s: Failed to get md5" % file, str(x))
            exitCode = 2

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
