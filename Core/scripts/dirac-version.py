#!/usr/bin/env python
########################################################################
# File :   dirac-version
# Author : Ricardo Graciani
########################################################################
"""
    print version of current DIRAC installation
"""
from __future__ import print_function
__RCSID__   = "$Id: dirac-version,v 1.3 2008/03/22 10:39:02 rgracian Exp $"
import DIRAC
print(DIRAC.version)
