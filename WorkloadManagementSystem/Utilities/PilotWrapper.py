""" Module holding function(s) creating the pilot wrapper.

    This is a DIRAC-free module, so it could possibly be used also outside of DIRAC installations.

    The main client of this module is the SiteDirector, that invokes the functions here more or less like this:

        pilotFiles = getPilotFiles()
        pilotFilesCompressedEncodedDict = getPilotFilesCompressedEncodedDict(pilotFiles)
        localPilot = pilotWrapperScript(pilotFilesCompressedEncodedDict,
                                        pilotOptions,
                                        pilotExecDir)
        _writePilotWrapperFile(localPilot=localPilot)

"""

import os
import tempfile
import shutil
import tarfile
import json
import base64
import bz2

from cStringIO import StringIO

import requests


def pilotWrapperScript(pilotFilesCompressedEncodedDict=None,
                       pilotOptions='',
                       pilotExecDir=''):
  """ Returns the content of the pilot wrapper script.

      The pilot wrapper script is a bash script that invokes the system python. Linux only.

     :param pilotFilesCompressedEncodedDict: this is a possible dict of name:compressed+encoded content files.
                        the proxy can be part of this, and of course the pilot files
     :type pilotFilesCompressedEncodedDict: dict
     :param pilotOptions: options with which to start the pilot
     :type pilotOptions: basestring
     :param pilotExecDir: pilot execution directory
     :type pilotExecDir: basestring

     :returns: content of the pilot wrapper
     :rtype: basestring
  """

  mString = ""
  if pilotFilesCompressedEncodedDict:  # are there some pilot files to unpack? then we create the unpacking string
    for pfName, encodedPf in pilotFilesCompressedEncodedDict.iteritems():
      mString += """
try:
  with open('%(pfName)s', "w") as fd:
    fd.write(bz2.decompress(base64.b64decode(\"\"\"%(encodedPf)s\"\"\")))
  os.chmod('%(pfName)s', stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
except BaseException as x:
  print >> sys.stderr, x
  shutil.rmtree(pilotWorkingDirectory)
  sys.exit(-1)
""" % {'encodedPf': encodedPf,
       'pfName': pfName}

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

# just logging the environment as first thing
print '==========================================================='
logger.debug('Environment of execution host\\n')
for key, val in os.environ.iteritems():
  logger.debug(key + '=' + val)
print '===========================================================\\n'

# putting ourselves in the right directory
pilotExecDir = '%(pilotExecDir)s'
if not pilotExecDir:
  pilotExecDir = os.getcwd()
pilotWorkingDirectory = tempfile.mkdtemp(suffix='pilot', prefix='DIRAC_', dir=pilotExecDir)
pilotWorkingDirectory = os.path.realpath(pilotWorkingDirectory)
os.chdir(pilotWorkingDirectory)
logger.info("Launching dirac-pilot script from %%s" %%os.getcwd())

# unpacking lines
logger.info("But first unpacking pilot files")
%(mString)s

# now finally launching the pilot script (which should be called dirac-pilot.py)
cmd = "python dirac-pilot.py %(pilotOptions)s"
logger.info('Executing: %%s' %% cmd)
sys.stdout.flush()
os.system(cmd)

# and cleaning up
shutil.rmtree(pilotWorkingDirectory)

EOF
""" % {'mString': mString,
       'pilotOptions': pilotOptions,
       'pilotExecDir': pilotExecDir}

  return localPilot


def getPilotFilesCompressedEncodedDict(pilotFiles, proxy=None):
  """ this function will return the dictionary of pilot files names : encodedCompressedContent
      that we are going to send

     :param pilotFiles: list of pilot files
     :type pilotFiles: list
     :param proxy: proxy
     :type proxy: basestring
  """
  pilotFilesCompressedEncodedDict = {}

  for pf in pilotFiles:
    with open(pf, "r") as fd:
      pfContent = fd.read()
    pfContentEncoded = base64.b64encode(bz2.compress(pfContent, 9))
    pilotFilesCompressedEncodedDict[os.path.basename(pf)] = pfContentEncoded

  if proxy is not None:
    compressedAndEncodedProxy = base64.b64encode(bz2.compress(proxy.dumpAllToString()['Value']))
    pilotFilesCompressedEncodedDict['proxy'] = compressedAndEncodedProxy

  return pilotFilesCompressedEncodedDict


def _writePilotWrapperFile(workingDirectory=None, localPilot=''):
  """ write the localPilot string to a file, rurn the file name

     :param workingDirectory: the directory where to store the pilot wrapper file
     :type workingDirectory: basestring
     :param localPilot: content of the pilot wrapper
     :type localPilot: basestring

     :returns: file name of the pilot wrapper
     :rtype: basestring
  """

  fd, name = tempfile.mkstemp(suffix='_pilotwrapper.py', prefix='DIRAC_', dir=workingDirectory)
  with os.fdopen(fd, 'w') as pilotWrapper:
    pilotWrapper.write(localPilot)
  return name


def getPilotFiles(pilotFilesDir=None, pilotFilesLocation=None):
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

  shutil.rmtree(pilotFilesDir)  # make sure it's empty
  os.mkdir(pilotFilesDir)

  # getting the pilot files
  if pilotFilesLocation.startswith('http'):
    res = requests.get(pilotFilesLocation)
    if res.status_code != 200:
      raise IOError(res.text)
    fileObj = StringIO(res.content)
    tar = tarfile.open(fileobj=fileObj)

    res = requests.get(os.path.join(os.path.dirname(pilotFilesLocation), 'pilot.json'))
    if res.status_code != 200:
      raise IOError(res.text)
    jsonCFG = res.json()
  else:  # maybe it's just a local file
    tar = tarfile.open(os.path.basename(pilotFilesLocation))

  tar.extractall(pilotFilesDir)
  with open(os.path.join(pilotFilesDir, 'pilot.json'), 'w') as fd:
    json.dump(jsonCFG, fd)

  # excluding some files that might got in
  pilotFiles = [pf for pf in os.listdir(pilotFilesDir) if pf not in ['__init__.py', 'dirac-install.py']]
  pilotFiles = [pf for pf in pilotFiles if pf.endswith('.py') or pf.endswith('.json')]
  pilotFiles = [os.path.join(pilotFilesDir, pf) for pf in pilotFiles]

  return pilotFiles
