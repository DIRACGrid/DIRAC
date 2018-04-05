""" Module holding function(s) creating the pilot wrapper
"""

import os
import tempfile
import shutil
import tarfile

from cStringIO import StringIO

import requests


def pilotWrapperScript(compressedAndEncodedProxy = '',
                       compressedAndEncodedInstall = '',
                       pilotFilesString = '',
                       pilotExecDir = '',
                       install = '',
                       pilotOptions = '',
                       proxyFlag = ''):
  """ returns the content of the pilot wrapper script
  """

  localPilot = """#!/bin/bash
/usr/bin/env python << EOF

# imports
import os
import stat
import tempfile
import sys
import shutil
import base64
import bz2
import logging
import time

# setting up the logging
formatter = logging.Formatter(fmt='%%(asctime)s UTC %%(levelname)-8s %%(message)s', datefmt='%%Y-%%m-%%d %%H:%%M:%%S')
logging.Formatter.converter = time.gmtime
try:
  screen_handler = logging.StreamHandler(stream=sys.stdout)
except TypeError:  # python2.6
  screen_handler = logging.StreamHandler(strm=sys.stdout)
screen_handler.setFormatter(formatter)
logger = logging.getLogger('pilotLogger')
logger.setLevel(logging.DEBUG)
logger.addHandler(screen_handler)

# unpacking
try:
  pilotExecDir = '%(pilotExecDir)s'
  if not pilotExecDir:
    pilotExecDir = os.getcwd()
  pilotWorkingDirectory = tempfile.mkdtemp(suffix='pilot', prefix='DIRAC_', dir=pilotExecDir)
  pilotWorkingDirectory = os.path.realpath(pilotWorkingDirectory)
  os.chdir(pilotWorkingDirectory)
  if %(proxyFlag)s:
    with open('proxy', "w") as fd:
      fd.write(bz2.decompress(base64.b64decode(\"\"\"%(compressedAndEncodedProxy)s\"\"\")))
    os.chmod("proxy", stat.S_IRUSR | stat.S_IWUSR)
    os.environ["X509_USER_PROXY"]=os.path.join(pilotWorkingDirectory, 'proxy')
  with open('%(installScript)s', "w") as fd:
    fd.write(bz2.decompress(base64.b64decode(\"\"\"%(compressedAndEncodedInstall)s\"\"\")))
  os.chmod("%(installScript)s", stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
  %(pilotFilesString)s

except BaseException as x:
  print >> sys.stderr, x
  shutil.rmtree(pilotWorkingDirectory)
  sys.exit(-1)

# just logging the environment
print '==========================================================='
logger.debug('Environment of execution host\\n')
for key, val in os.environ.iteritems():
  logger.debug(key + '=' + val)
print '===========================================================\\n'

# now finally launching the pilot script
cmd = "python dirac-pilot.py %(pilotOptions)s"
logger.info('Executing: %%s' %% cmd)
sys.stdout.flush()
os.system(cmd)

# and cleaning up
shutil.rmtree(pilotWorkingDirectory)

EOF
""" % {'compressedAndEncodedProxy': compressedAndEncodedProxy,
       'compressedAndEncodedInstall': compressedAndEncodedInstall,
       'pilotFilesString': pilotFilesString,
       'pilotExecDir': pilotExecDir,
       'installScript': os.path.basename(install),
       'pilotOptions': ' '.join(pilotOptions),
       'proxyFlag': proxyFlag}

  return localPilot


def _writePilotWrapperFile(workingDirectory=None, localPilot=''):
  """ write the localPilot string to a file, return the file name
  """

  if workingDirectory is None:
    workingDirectory = os.getcwd()

  fd, name = tempfile.mkstemp(suffix='_pilotwrapper.py', prefix='DIRAC_', dir=workingDirectory)
  with os.fdopen(fd, 'w') as pilotWrapper:
    pilotWrapper.write(localPilot)
  return name


def getPilotFiles(pilotFilesDir = None, pilotFilesLocation = None):
  """ get the pilot files to be sent in a local directory (this is for pilot3 files)

     :param pilotFilesDir: the directory where to store the pilot files
     :type pilotFilesDir: basestring
     :param pilotFilesLocation: URL from where to the pilot files
     :type pilotFilesLocation: basestring

     :returns: list of pilot files (full path)
     :rtype: list
  """

  if pilotFilesDir is None:
    pilotFilesDir = os.getcwd()

  shutil.rmtree(pilotFilesDir) # make sure it's empty
  os.mkdir(pilotFilesDir)

  # getting the pilot files
  if pilotFilesLocation.startswith('http'):
    res = requests.get(pilotFilesLocation)
    if res.status_code != 200:
      raise IOError, res.text
    fileObj = StringIO(res.content)
    tar = tarfile.open(fileobj=fileObj)
  else: # maybe it's just a local file
    tar = tarfile.open(os.path.basename(pilotFilesLocation))

  tar.extractall(pilotFilesDir)
  # excluding some files that might got in
  pilotFiles = [pf for pf in os.listdir(pilotFilesDir) if pf not in ['__init__.py', 'dirac-install.py']]
  pilotFiles = [pf for pf in pilotFiles if pf.endswith('.py')]
  pilotFiles = [os.path.join(pilotFilesDir, pf) for pf in pilotFiles]

  return pilotFiles

