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
#
#
# Invoke this with:
#
# python Test_GenerateAndExecutePilotWrapper.py url://to_PilotWrapper.py
# (and in this case it will download dirac-install.py from github)
# or
# python Test_GenerateAndExecutePilotWrapper.py url://to_PilotWrapper.py url://to_dirac-install.py
#


from __future__ import print_function

import sys
import os
import time
import base64
import bz2

# 1) gets the (DIRAC-free) PilotWrapper.py, and dirac-install.py

# urllib is different between python 2 and 3
if sys.version_info < (3,):
  from urllib2 import urlopen as url_library_urlopen  # pylint: disable=import-error
else:
  from urllib.request import urlopen as url_library_urlopen  # pylint: disable=import-error,no-name-in-module


if sys.version_info >= (2, 7, 9):
  import ssl  # pylint: disable=import-error
  context = ssl._create_unverified_context()
  rf = url_library_urlopen(sys.argv[1],
                           context=context)
  try:  # dirac-install.py location from the args, if provided
    di = url_library_urlopen(sys.argv[2],
                             context=context)
  except IndexError:
    di_loc = 'https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py'
    di = url_library_urlopen(di_loc,
                             context=context)
else:
  rf = url_library_urlopen(sys.argv[1])
  try:  # dirac-install.py location from the args, if provided
    di = url_library_urlopen(sys.argv[2])
  except IndexError:
    di_loc = 'https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py'
    di = url_library_urlopen(di_loc)

with open('PilotWrapper.py', 'wb') as pj:
  pj.write(rf.read())
  pj.close()  # for python 2.6
with open('dirac-install.py', 'wb') as pj:
  pj.write(di.read())
  pj.close()  # for python 2.6


# 2)  use its functions to generate a pilot wrapper
time.sleep(1)
# by now this will be in the local dir
from PilotWrapper import pilotWrapperScript  # pylint: disable=import-error

diracInstall = os.path.join(os.getcwd(), 'dirac-install.py')
with open(diracInstall, "r") as fd:
  diracInstall = fd.read()
if sys.version_info >= (3,):
  diracInstallEncoded = base64.b64encode(bz2.compress(diracInstall.encode(), 9))
else:
  diracInstallEncoded = base64.b64encode(bz2.compress(diracInstall, 9))

res = pilotWrapperScript(
    pilotFilesCompressedEncodedDict={'dirac-install.py': diracInstallEncoded},
    pilotOptions="--commands CheckWorkerNode,InstallDIRAC --setup=DIRAC-Certification --debug",
    location='lbcertifdirac7.cern.ch:8443,wrong.cern.ch')

with open('pilot-wrapper.sh', 'wb') as pj:
  pj.write(res.encode())

# 3) now start it

os.system("sh pilot-wrapper.sh")
