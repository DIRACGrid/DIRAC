#!/usr/bin/env python

# This is a test that:
# - gets the (DIRAC-free) PilotWrapper.py (that should be in input)
# - use its functions to generate a pilot wrapper
# - starts it
#
# It should be executed for different versions of python, e.g.:
# - 2.7.x (x < 9)
# - 2.7.x (x >= 9)
# - 3.6.x
# - 3.9.x
#
#
# Invoke this with:
#
# python Test_GenerateAndExecutePilotWrapper.py url://to_PilotWrapper.py


from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import sys
import os
import time

# 1) gets the (DIRAC-free) PilotWrapper.py

# urllib is different between python 2 and 3
if sys.version_info < (3,):
    from urllib2 import urlopen as url_library_urlopen  # pylint: disable=import-error
else:
    from urllib.request import urlopen as url_library_urlopen  # pylint: disable=import-error,no-name-in-module


if sys.version_info >= (2, 7, 9):
    import ssl  # pylint: disable=import-error

    context = ssl._create_unverified_context()
    rf = url_library_urlopen(sys.argv[1], context=context)
else:
    rf = url_library_urlopen(sys.argv[1])
pilotBranch = sys.argv[2]

with open("PilotWrapper.py", "wb") as pj:
    pj.write(rf.read())


# 2)  use its functions to generate a pilot wrapper
time.sleep(1)
# by now this will be in the local dir
from PilotWrapper import pilotWrapperScript  # pylint: disable=import-error

res = pilotWrapperScript(
    pilotOptions="--setup=CI -N ce.dirac.org -Q DIRACQUEUE -n DIRAC.CI.ORG --pythonVersion=3 --debug",
    location="diracproject.web.cern.ch/diracproject/tars/Pilot/DIRAC/" + pilotBranch + "/,wrong.cern.ch",
)

with open("pilot-wrapper.sh", "wb") as pj:
    pj.write(res.encode())

# 3) now start it

ret = os.system("sh pilot-wrapper.sh")
if ret:
    sys.exit(1)
