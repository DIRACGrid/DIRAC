""" Module holding function(s) creating the pilot wrapper.

    This is a DIRAC-free module, so it could possibly be used also outside of DIRAC installations.

    The main client of this module is the SiteDirector, that invokes the functions here more or less like this::

        pilotFilesCompressedEncodedDict = getPilotFilesCompressedEncodedDict(pilotFiles)
        localPilot = pilotWrapperScript(pilotFilesCompressedEncodedDict,
                                        pilotOptions,
                                        pilotExecDir)
        _writePilotWrapperFile(localPilot=localPilot)

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import tempfile
import base64
import bz2

pilotWrapperContent = """#!/bin/bash
/usr/bin/env python << EOF

# imports
from __future__ import print_function

import os
import stat
import tempfile
import sys
import shutil
import base64
import bz2
import logging
import time
import tarfile
import hashlib

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

# just logging the environment as first thing
logger.debug('===========================================================')
logger.debug('Environment of execution host\\n')
for key, val in os.environ.items():
  logger.debug(key + '=' + val)
logger.debug('===========================================================\\n')

# putting ourselves in the right directory
pilotExecDir = '%(pilotExecDir)s'
if not pilotExecDir:
  pilotExecDir = os.getcwd()
pilotWorkingDirectory = tempfile.mkdtemp(suffix='pilot', prefix='DIRAC_', dir=pilotExecDir)
pilotWorkingDirectory = os.path.realpath(pilotWorkingDirectory)
os.chdir(pilotWorkingDirectory)
logger.info("Launching dirac-pilot script from %%s" %%os.getcwd())
"""


def pilotWrapperScript(pilotFilesCompressedEncodedDict=None,
                       pilotOptions='',
                       pilotExecDir='',
                       envVariables=None,
                       location=''):
  """ Returns the content of the pilot wrapper script.

      The pilot wrapper script is a bash script that invokes the system python. Linux only.

     :param pilotFilesCompressedEncodedDict: this is a possible dict of name:compressed+encoded content files.
                        the proxy can be part of this, and of course the pilot files
     :type pilotFilesCompressedEncodedDict: dict
     :param pilotOptions: options with which to start the pilot
     :type pilotOptions: string
     :param pilotExecDir: pilot execution directory
     :type pilotExecDir: string
     :param envVariables: dictionary of environment variables
     :type envVariables: dict
     :param location: location where to get the pilot files
     :type location: string

     :returns: content of the pilot wrapper
     :rtype: string
  """

  if pilotFilesCompressedEncodedDict is None:
    pilotFilesCompressedEncodedDict = {}

  if envVariables is None:
    envVariables = {}

  compressedString = ""
  # are there some pilot files to unpack? Then we create the unpacking string
  for pfName, encodedPf in pilotFilesCompressedEncodedDict.items():
    compressedString += """
try:
  with open('%(pfName)s', 'wb') as fd:
    if sys.version_info < (3,):
      fd.write(bz2.decompress(base64.b64decode(\"\"\"%(encodedPf)s\"\"\")))
    else:
      fd.write(bz2.decompress(base64.b64decode(b'%(encodedPf)s')))
  os.chmod('%(pfName)s', stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
except Exception as x:
  print(x, file=sys.stderr)
  logger.error(x)
  shutil.rmtree(pilotWorkingDirectory)
  sys.exit(3)
""" % {'encodedPf': encodedPf.decode() if hasattr(encodedPf, "decode") else encodedPf,
       'pfName': pfName}

  envVariablesString = ""
  for name, value in envVariables.items():  # are there some environment variables to add?
    envVariablesString += """
os.environ[\"%(name)s\"]=\"%(value)s\"
""" % {'name': name,
       'value': value}

  # add X509_USER_PROXY to establish pilot env in Cluster WNs
  if 'proxy' in pilotFilesCompressedEncodedDict:
    envVariablesString += """
os.environ['X509_USER_PROXY'] = os.path.join(pilotWorkingDirectory, 'proxy')
"""

  # now building the actual pilot wrapper

  localPilot = pilotWrapperContent % {'pilotExecDir': pilotExecDir}

  if compressedString:
    localPilot += """
# unpacking lines
logger.info("But first unpacking pilot files")
%s
""" % compressedString

  if envVariablesString:
    localPilot += """
# Modifying the environment
%s
""" % envVariablesString

  if location:
    localPilot += """
# Getting the pilot files
logger.info("Getting the pilot files from %(location)s")

location = '%(location)s'.replace(' ', '').split(',')

import random
random.shuffle(location)

# we try from the available locations
locs = [os.path.join('https://', loc) for loc in location]
locations = locs + [os.path.join(loc, 'pilot') for loc in locs]

for loc in locations:
  print('Trying %%s' %% loc)

  # Getting the json, tar, and checksum file
  try:

    # urllib is different between python 2 and 3
    if sys.version_info < (3,):
      from urllib2 import urlopen as url_library_urlopen
      from urllib2 import URLError as url_library_URLError
    else:
      from urllib.request import urlopen as url_library_urlopen
      from urllib.error import URLError as url_library_URLError

    for fileName in ['pilot.json', 'pilot.tar', 'checksums.sha512']:
      # needs to distinguish whether urlopen method contains the 'context' param
      # in theory, it should be available from python 2.7.9
      # in practice, some prior versions may be composed of recent urllib version containing the param
      if 'context' in url_library_urlopen.__code__.co_varnames:
        import ssl
        context = ssl._create_unverified_context()
        remoteFile = url_library_urlopen(os.path.join(loc, fileName),
                                         timeout=10,
                                         context=context)

      else:
        remoteFile = url_library_urlopen(os.path.join(loc, fileName),
                                         timeout=10)

      localFile = open(fileName, 'wb')
      localFile.write(remoteFile.read())
      localFile.close()

      if fileName != 'pilot.tar':
        continue
      try:
        pt = tarfile.open('pilot.tar', 'r')
        pt.extractall()
        pt.close()
      except Exception as x:
        print("tarfile failed with message (this is normal!) %%s" %% repr(x), file=sys.stderr)
        logger.error("tarfile failed with message (this is normal!) %%s" %% repr(x))
        logger.warn("Trying tar command (tar -xvf pilot.tar)")
        res = os.system("tar -xvf pilot.tar")
        if res:
          logger.error("tar failed with exit code %%d, giving up (this is normal!)" %% int(res))
          print("tar failed with exit code %%d, giving up (this is normal!)" %% int(res), file=sys.stderr)
          raise
    # if we get here we break out of the loop of locations
    break
  except (url_library_URLError, Exception) as e:
    print('%%s unreacheable (this is normal!)' %% loc, file=sys.stderr)
    logger.error('%%s unreacheable (this is normal!)' %% loc)
    logger.exception(e)

else:
  print("None of the locations of the pilot files is reachable", file=sys.stderr)
  logger.error("None of the locations of the pilot files is reachable")
  sys.exit(-1)

# download was successful, now we check checksums
if os.path.exists('checksums.sha512'):
  checksumDict = {}
  chkSumFile = open('checksums.sha512', 'rt')
  for line in chkSumFile.read().split('\\n'):
    if not line.strip():  ## empty lines are ignored
      continue
    expectedHash, fileName = line.split('  ', 1)
    if not os.path.exists(fileName):
      continue
    logger.info('Checking %%r for checksum', fileName)
    fileHash = hashlib.sha512(open(fileName, 'rb').read()).hexdigest()
    if fileHash != expectedHash:
      print('Checksum mismatch for file %%r' %% fileName, file=sys.stderr)
      print('Expected %%r, found %%r' %%(expectedHash, fileHash), file=sys.stderr)
      logger.error('Checksum mismatch for file %%r', fileName)
      logger.error('Expected %%r, found %%r', expectedHash, fileHash)
      sys.exit(-1)
    logger.debug('Checksum matched')

""" % {'location': location}

  localPilot += """
# now finally launching the pilot script (which should be called dirac-pilot.py)
cmd = "python dirac-pilot.py %s"
logger.info('Executing: %%s' %% cmd)
sys.stdout.flush()
ret = os.system(cmd)

# and cleaning up
shutil.rmtree(pilotWorkingDirectory)

# did it fail?
if ret:
  sys.exit(1)

EOF
""" % pilotOptions

  return localPilot


def getPilotFilesCompressedEncodedDict(pilotFiles, proxy=None):
  """ this function will return the dictionary of pilot files names : encodedCompressedContent
      that we are going to send

     :param pilotFiles: list of pilot files (list of location on the disk)
     :type pilotFiles: list
     :param proxy: the proxy to send
     :type proxy: X509Chain
  """
  pilotFilesCompressedEncodedDict = {}

  for pf in pilotFiles:
    with open(pf, "r") as fd:
      pfContent = fd.read()
    pfContentEncoded = base64.b64encode(bz2.compress(pfContent.encode(), 9))
    pilotFilesCompressedEncodedDict[os.path.basename(pf)] = pfContentEncoded

  if proxy is not None:
    compressedAndEncodedProxy = base64.b64encode(bz2.compress(proxy.dumpAllToString()['Value']))
    pilotFilesCompressedEncodedDict['proxy'] = compressedAndEncodedProxy

  return pilotFilesCompressedEncodedDict


def _writePilotWrapperFile(workingDirectory=None, localPilot=''):
  """ write the localPilot string to a file, rurn the file name

     :param workingDirectory: the directory where to store the pilot wrapper file
     :type workingDirectory: string
     :param localPilot: content of the pilot wrapper
     :type localPilot: string

     :returns: file name of the pilot wrapper
     :rtype: string
  """

  fd, name = tempfile.mkstemp(suffix='_pilotwrapper.py', prefix='DIRAC_', dir=workingDirectory)
  with os.fdopen(fd, 'w') as pilotWrapper:
    pilotWrapper.write(localPilot)
  return name
