#!/usr/bin/env python
########################################################################
# File :   dirac-platform
# Author : Adria Casajus
########################################################################
"""
The *dirac-platform* script determines the "platform" of a certain node.
The platform is a string used to identify the minimal characteristics of the node,
enough to determine which version of DIRAC can be installed.

On a RHEL 6 node, for example, the determined dirac platform is "Linux_x86_64_glibc-2.5"

Usage:
  dirac-platform [options]

Example:
  $ dirac-platform
  Linux_x86_64_glibc-2.5

"""
try:
    from DIRAC.Core.Utilities.Platform import getPlatformString
except Exception:
    import argparse
    import platform

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_known_args()

    # Command line interface
    def getPlatformString():
        # Modified to return our desired platform string, R. Graciani
        platformTuple = (platform.system(), platform.machine())
        if platformTuple[0] == "Linux":
            platformTuple += ("-".join(platform.libc_ver()),)
        elif platformTuple[0] == "Darwin":
            platformTuple += (".".join(platform.mac_ver()[0].split(".")[:2]),)
        else:
            platformTuple += platform.release()

        platformString = "%s_%s_%s" % platformTuple

        return platformString

    def main():
        print(getPlatformString())

else:
    from DIRAC.Core.Base.Script import Script

    @Script()
    def main():
        print(getPlatformString())


if __name__ == "__main__":
    main()
