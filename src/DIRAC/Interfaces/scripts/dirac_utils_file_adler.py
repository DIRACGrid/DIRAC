#!/usr/bin/env python
########################################################################
# File :    dirac-utils-file-adler
########################################################################
"""
Calculate alder32 of the supplied file

Example:
  $ dirac-utils-file-adler Example.tgz
  Example.tgz 88b4ca8b
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
    from DIRAC.Core.Utilities.Adler import fileAdler

    for fa in files:
        adler = fileAdler(fa)
        if adler:
            print(fa.rjust(100), adler.ljust(10))  # pylint: disable=no-member
        else:
            print("ERROR %s: Failed to get adler" % fa)
            exitCode = 2

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
