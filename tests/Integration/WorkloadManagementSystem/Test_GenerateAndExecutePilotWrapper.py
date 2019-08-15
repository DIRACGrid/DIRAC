#!/usr/bin/env python

# This is a test that:
# - gets the (DIRAC-free) PilotWrapper.py (that should be in input)
# - use its functions to generate a pilot wrapper
# - starts it
#
# It should be executed for different versions of python, e.g.:
# - 2.6.x
# - 2.7.x (x < 9)
# - 2.7.x (x >= 9)
# (- 3.6.x)

from __future__ import print_function

import sys
import urllib2
import os
import time
import base64
import bz2

# gets the (DIRAC-free) PilotWrapper.py, and dirac-install.py

if sys.version_info >= (2, 7, 9):
  import ssl
  context = ssl._create_unverified_context()
  rf = urllib2.urlopen(sys.argv[1],
                       context=context)
  try:  # dirac-install.py location from the args, if provided
    di = urllib2.urlopen(sys.argv[2],
                         context=context)
  except IndexError:
    di = urllib2.urlopen('https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py',
                         context=context)
else:
  rf = urllib2.urlopen(sys.argv[1])
  try:  # dirac-install.py location from the args, if provided
    di = urllib2.urlopen(sys.argv[2])
  except IndexError:
    di = urllib2.urlopen('https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py')

with open('PilotWrapper.py', 'wb') as pj:
  pj.write(rf.read())
  pj.close()  # for python 2.6
with open('dirac-install.py', 'wb') as pj:
  pj.write(di.read())
  pj.close()  # for python 2.6


# use its functions to generate a pilot wrapper
time.sleep(1)
from PilotWrapper import pilotWrapperScript

diracInstall = os.path.join(os.getcwd(), 'dirac-install.py')
with open(diracInstall, "r") as fd:
  diracInstall = fd.read()
diracInstallEncoded = base64.b64encode(bz2.compress(diracInstall, 9))

res = pilotWrapperScript(
    pilotFilesCompressedEncodedDict={'dirac-install.py': diracInstallEncoded},
    pilotOptions="--commands CheckWorkerNode,InstallDIRAC --setup=DIRAC-Certification --debug",
    location='lbcertifdirac7.cern.ch:8443')

with open('pilot-wrapper.sh', 'wb') as pj:
  pj.write(res)

# now start it

os.system("sh pilot-wrapper.sh")
