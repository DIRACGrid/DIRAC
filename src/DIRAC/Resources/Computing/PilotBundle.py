########################################################################
# File :   PilotBundle.py
# Author : Ricardo Graciani
########################################################################
"""
  Collection of Utilities to handle pilot jobs

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id:$"

import os
import base64
import bz2
import tempfile


def bundleProxy(executableFile, proxy):
  """ Create a self extracting archive bundling together an executable script and a proxy
  """

  compressedAndEncodedProxy = base64.b64encode(bz2.compress(proxy.dumpAllToString()['Value'])).decode()
  with open(executableFile, "rb") as fp:
    compressedAndEncodedExecutable = base64.b64encode(bz2.compress(fp.read(), 9)).decode()

  bundle = """#!/usr/bin/env python
# Wrapper script for executable and proxy
import os
import tempfile
import sys
import stat
import base64
import bz2
import shutil

try:
  workingDirectory = tempfile.mkdtemp(suffix='_wrapper', prefix='TORQUE_')
  os.chdir(workingDirectory)
  open('proxy', "w").write(bz2.decompress(base64.b64decode("%(compressedAndEncodedProxy)s")))
  open('%(executable)s', "w").write(bz2.decompress(base64.b64decode("%(compressedAndEncodedExecutable)s")))
  os.chmod('proxy', stat.S_IRUSR | stat.S_IWUSR)
  os.chmod('%(executable)s', stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
  os.environ["X509_USER_PROXY"] = os.path.join(workingDirectory, 'proxy')
except Exception as x:
  print >> sys.stderr, x
  sys.exit(-1)
cmd = "./%(executable)s"
print 'Executing: ', cmd
sys.stdout.flush()
os.system(cmd)

shutil.rmtree(workingDirectory)

""" % {'compressedAndEncodedProxy': compressedAndEncodedProxy,
       'compressedAndEncodedExecutable': compressedAndEncodedExecutable,
       'executable': os.path.basename(executableFile)}

  return bundle


def writeScript(script, writeDir=None):
  """
    Write script into a temporary unique file under provided writeDir
  """
  fd, name = tempfile.mkstemp(suffix='_pilotWrapper.py', prefix='DIRAC_', dir=writeDir)
  pilotWrapper = os.fdopen(fd, 'w')
  pilotWrapper.write(script)
  pilotWrapper.close()
  return name
