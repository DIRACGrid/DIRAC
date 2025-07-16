"""Module holding function(s) creating the pilot wrapper.

This is a DIRAC-free module, so it could possibly be used also outside of DIRAC installations.

The main client of this module is the SiteDirector, that invokes the functions here more or less like this::

    pilotFilesCompressedEncodedDict = getPilotFilesCompressedEncodedDict(pilotFiles)
    localPilot = pilotWrapperScript(pilotFilesCompressedEncodedDict,
                                    pilotOptions,
                                    pilotExecDir)
   _writePilotWrapperFile(localPilot=localPilot)

"""

from __future__ import absolute_import, division, print_function

import base64
import bz2
import os
import tempfile


pilotWrapperContent = """#!/bin/bash
# Reduce the maximum allowed number of open file descriptors as micromamba
# gets stuck due to https://github.com/DaanDeMeyer/reproc/pull/103
current_limit=$(ulimit -n)
new_limit=1048575
if [ "${current_limit}" = "unlimited" ] || [ "${current_limit}" -gt "${new_limit}" ]; then
    ulimit -n "${new_limit}"
fi
if command -v python &> /dev/null; then
  py='python'
elif command -v python3 &> /dev/null; then
  py='python3'
elif command -v python2 &> /dev/null; then
  py='python2'
fi
/usr/bin/env $py << EOF

# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import io
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
# for remote logging
import subprocess
import json
import os
import urllib
import ssl
import shlex
from uuid import uuid1

try:
    # For Python 3.0 and later
    from urllib.request import urlopen, HTTPError, URLError
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen, HTTPError, URLError
    from urllib import urlencode

try:
  from cStringIO import StringIO
except ImportError:
  from io import StringIO

# formatting with microsecond accuracy, (ISO-8601)

class MicrosecondFormatter(logging.Formatter):
  def formatTime(self, record, datefmt=None):
    ct = self.converter(record.created)
    if datefmt:
        s = time.strftime(datefmt, ct)
    else:
        t = time.strftime("%%Y-%%m-%%dT%%H:%%M:%%S", ct)
        s = "%%s,%%06dZ" %% (t, (record.created - int(record.created)) * 1e6)
    return s

# setting up the logging
# formatter = logging.Formatter(fmt='%%(asctime)s UTC %%(levelname)-8s %%(message)s', datefmt='%%Y-%%m-%%d %%H:%%M:%%S')
formatter = MicrosecondFormatter('%%(asctime)s %%(levelname)-8s [%%(name)s] %%(message)s')
logging.Formatter.converter = time.gmtime
try:
  screen_handler = logging.StreamHandler(stream=sys.stdout)
except TypeError:  # python2.6
  screen_handler = logging.StreamHandler(strm=sys.stdout)
screen_handler.setFormatter(formatter)

# add a string buffer handler
sio = StringIO()
buffer = logging.StreamHandler(sio)
buffer.setFormatter(formatter)

logger = logging.getLogger('pilotLogger')
logger.setLevel(logging.DEBUG)
logger.addHandler(screen_handler)
logger.addHandler(buffer)

# just logging the environment as first thing
logger.debug('===========================================================')
logger.debug('Environment of execution host\\n')
# Clear any non-UTF encodable environment variables as they cause the pilot to fail
# Use os.environb if it exists (gives raw bytes), else fall back to os.environ.
environb = getattr(os, "environb", os.environ)
for key, val in environb.items():
    try:
        # Decode if these are bytes; if they're already text, leave them as-is.
        if isinstance(key, bytes):
            key_decoded = key.decode("utf-8")
        else:
            key_decoded = key
        if isinstance(val, bytes):
            val_decoded = val.decode("utf-8")
        else:
            val_decoded = val
    except UnicodeDecodeError as e:
        logger.error("Dropping %%s=%%s due to decode error: %%s", key, val, e)
        del environb[key]
        continue

    logger.debug(key_decoded + '=' + val_decoded)
logger.debug('===========================================================\\n')

logger.debug(sys.version)

# putting ourselves in the right directory
pilotExecDir = '%(pilotExecDir)s'
if not pilotExecDir:
  pilotExecDir = os.getcwd()
pilotWorkingDirectory = tempfile.mkdtemp(suffix='pilot', prefix='DIRAC_', dir=pilotExecDir)
pilotWorkingDirectory = os.path.realpath(pilotWorkingDirectory)
os.chdir(pilotWorkingDirectory)
logger.info("Launching dirac-pilot script from %%s" %%os.getcwd())
"""


def pilotWrapperScript(
    pilotFilesCompressedEncodedDict=None,
    pilotOptions="",
    pilotExecDir="",
    envVariables=None,
    location="",
    CVMFS_locations=None,
):
    """Returns the content of the pilot wrapper script.

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
    :param CVMFS_locations: optional CVMFS locations of where to get the pilot files
    :type CVMFS_locations: list

    :returns: content of the pilot wrapper
    :rtype: string
    """

    if pilotFilesCompressedEncodedDict is None:
        pilotFilesCompressedEncodedDict = {}

    if envVariables is None:
        envVariables = {}

    if not CVMFS_locations:
        # What is in this location is almost certainly incorrect, especially the pilot.json
        CVMFS_locs = '["file:/cvmfs/dirac.egi.eu/pilot"]'
    else:
        # Here we are making the assumption that, if CVMFS_locations is, e.g., ['/cvmfs/somewhere', '/cvmfs/elsewhere']
        # and the project is 'LHCb',
        # then the pilot can maybe be found at locations
        # - file:/cvmfs/somewhere/lhcbdirac/pilot
        # - file:/cvmfs/elsewhere/lhcbdirac/pilot
        project = "dirac"
        if " -l " in pilotOptions:
            project = pilotOptions.split(" ")[pilotOptions.split(" ").index("-l") + 1].lower() + "dirac"
        CVMFS_locs = "[" + ",".join('"file:' + os.path.join(loc, project, 'pilot"') for loc in CVMFS_locations) + "]"

    compressedString = ""
    # are there some pilot files to unpack? Then we create the unpacking string
    for pfName, encodedPf in pilotFilesCompressedEncodedDict.items():
        compressedString += """
try:
  fd = os.open('%(pfName)s', os.O_WRONLY | os.O_CREAT | os.O_TRUNC, stat.S_IRUSR | stat.S_IWUSR)
  with io.open(fd, 'wb') as fd:
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
""" % {
            "encodedPf": encodedPf.decode() if hasattr(encodedPf, "decode") else encodedPf,
            "pfName": pfName,
        }

    envVariablesString = ""
    for name, value in envVariables.items():  # are there some environment variables to add?
        envVariablesString += """
os.environ[\"%(name)s\"]=\"%(value)s\"
""" % {
            "name": name,
            "value": value,
        }

    # add X509_USER_PROXY to establish pilot env in Cluster WNs
    if "proxy" in pilotFilesCompressedEncodedDict:
        envVariablesString += """
os.environ['X509_USER_PROXY'] = os.path.join(pilotWorkingDirectory, 'proxy')
"""

    # now building the actual pilot wrapper

    localPilot = pilotWrapperContent % {"pilotExecDir": pilotExecDir}

    if compressedString:
        localPilot += (
            """
# unpacking lines
logger.info("But first unpacking pilot files")
%s
"""
            % compressedString
        )

    if envVariablesString:
        localPilot += (
            """
# Modifying the environment
%s
"""
            % envVariablesString
        )

    if location:
        localPilot += """
# Getting the pilot files
logger.info("Getting the pilot files from %(location)s")

location = '%(location)s'.replace(' ', '').split(',')

# we try from the available locations
locs = [os.path.join('https://', loc) if not loc.startswith(('file:', 'https:')) else loc for loc in location]
locations = locs + [os.path.join(loc, 'pilot') for loc in locs]
# adding also the cvmfs locations
locations += %(CVMFS_locs)s

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

    for fileName in ['checksums.sha512', 'pilot.json', 'pilot.tar']:
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

""" % {
            "location": location,
            "CVMFS_locs": CVMFS_locs,
        }

    localPilot += (
        """
# now finally launching the pilot script (which should be called dirac-pilot.py)
# get the setup name an -z, if present to get remote logging in place
opt = "%s"
# try to get a pilot stamp from the environment:
UUID =  os.environ.get('DIRAC_PILOT_STAMP')
if UUID is None:
    UUID = str(uuid1())
opt = opt + " --pilotUUID " + UUID

args = opt.split()

# let's see if we have remote logging enabled (-z), if not run the pilot script with os.system, as before

logger.info("dirac-pilot.py  will be called: with %%s " %% args)

# call dirac-pilot.py and pass log records accumulated so far as a standard input. A decision to use the buffer
# in a remote logger is made later in dirac-pilot.py script

proc = subprocess.Popen(shlex.split("$py dirac-pilot.py " + opt), bufsize = 1, stdin = subprocess.PIPE,
                        stdout=sys.stdout, stderr=sys.stderr, universal_newlines = True)
proc.communicate(buffer.stream.getvalue())
ret = proc.returncode
# and cleaning up
buffer.stream.close()
shutil.rmtree(pilotWorkingDirectory)

# did it fail?
if ret:
  sys.exit(1)

EOF
"""
        % pilotOptions
    )

    return localPilot


def getPilotFilesCompressedEncodedDict(pilotFiles, proxy=None):
    """this function will return the dictionary of pilot files names : encodedCompressedContent
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
        from pathlib import Path  # pylint: disable=import-error
        from DIRAC.Core.Security.ProxyFile import writeChainToTemporaryFile  # pylint: disable=import-error

        retVal = writeChainToTemporaryFile(proxy)
        if not retVal["OK"]:
            raise RuntimeError("Failed to write proxy to temporary file: %s" % retVal["Message"])
        proxyLocation = Path(retVal["Value"])
        proxy_string = proxyLocation.read_text()
        proxyLocation.unlink()

        compressedAndEncodedProxy = base64.b64encode(bz2.compress(proxy_string.encode()))
        pilotFilesCompressedEncodedDict["proxy"] = compressedAndEncodedProxy

    return pilotFilesCompressedEncodedDict


def _writePilotWrapperFile(workingDirectory=None, localPilot=""):
    """write the localPilot string to a file, rurn the file name

    :param workingDirectory: the directory where to store the pilot wrapper file
    :type workingDirectory: string
    :param localPilot: content of the pilot wrapper
    :type localPilot: string

    :returns: file name of the pilot wrapper
    :rtype: string
    """

    fd, name = tempfile.mkstemp(suffix="_pilotwrapper.py", prefix="DIRAC_", dir=workingDirectory)
    with os.fdopen(fd, "w") as pilotWrapper:
        pilotWrapper.write(localPilot)
    return name
