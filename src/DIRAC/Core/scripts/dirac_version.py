#!/usr/bin/env python
########################################################################
# File :   dirac-version
# Author : Ricardo Graciani
########################################################################
"""
    print version of current DIRAC installation
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import DIRAC
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  print(DIRAC.version)


if __name__ == "__main__":
  main()
