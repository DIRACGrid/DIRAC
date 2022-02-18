#!/usr/bin/env python
########################################################################
# File :   dirac-version
# Author : Ricardo Graciani
########################################################################
"""
Print version of current DIRAC installation

Usage:
  dirac-version [option]

Example:
  $ dirac-version

"""
import argparse

import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.parse_known_args()

    print(DIRAC.version)


if __name__ == "__main__":
    main()
