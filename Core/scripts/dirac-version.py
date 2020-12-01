#!/usr/bin/env python
########################################################################
# File :   dirac-version
# Author : Ricardo Graciani
########################################################################
"""
Print version of current DIRAC installation

Example:

  $ dirac-version        
  v5r12-pre9
"""
from __future__ import print_function

import DIRAC
print(DIRAC.version)
