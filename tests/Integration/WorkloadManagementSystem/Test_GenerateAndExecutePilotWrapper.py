#!/usr/bin/env python

# This is a test that:
# - gets the (DIRAC-free) PilotWrapper.py (that should be in input)
# - use its functions to generate a pilot wrapper
# - starts it
#
# It should be executed for different versions of python, e.g.:
# - 3.6.x
# - 3.11.x
#
#
# Invoke this with:
#
# python Test_GenerateAndExecutePilotWrapper.py url://to_PilotWrapper.py


import ssl  # pylint: disable=import-error
import subprocess
import sys
import time

# 1) gets the (DIRAC-free) PilotWrapper.py

from urllib.request import urlopen


context = ssl._create_unverified_context()
rf = urlopen(sys.argv[1], context=context)
locc = sys.argv[2]

with open("PilotWrapper.py", "wb") as pj:
    pj.write(rf.read())


# 2)  use its functions to generate a pilot wrapper
time.sleep(1)
# by now this will be in the local dir
from PilotWrapper import pilotWrapperScript  # pylint: disable=import-error

res = pilotWrapperScript(
    pilotOptions="-N ce.dirac.org -Q DIRACQUEUE -n DIRAC.CI.ORG --debug", location="wrong.cern.ch, " + locc
)

with open("pilot-wrapper.sh", "wb") as pj:
    pj.write(res.encode())

# 3) now start it

result = subprocess.call(["sh", "pilot-wrapper.sh"])
if result != 0:
    sys.exit(1)
sys.exit(0)
