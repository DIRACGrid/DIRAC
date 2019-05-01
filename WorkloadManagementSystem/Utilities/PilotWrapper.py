""" Module holding function(s) creating the pilot wrapper.

    This is a DIRAC-free module, so it could possibly be used also outside of DIRAC installations.

    The main client of this module is the SiteDirector, that invokes the functions here more or less like this::

        pilotFilesCompressedEncodedDict = getPilotFilesCompressedEncodedDict(pilotFiles)
        localPilot = pilotWrapperScript(pilotFilesCompressedEncodedDict,
                                        pilotOptions,
                                        pilotExecDir)
        _writePilotWrapperFile(localPilot=localPilot)

"""

import os
import tempfile
import base64
import bz2

pilotWrapperContent = """#!/bin/bash
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
import urllib2
import tarfile

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
     :type pilotOptions: basestring
     :param pilotExecDir: pilot execution directory
     :type pilotExecDir: basestring
     :param envVariables: dictionary of environment variables
     :type envVariables: dict
     :param location: location where to get the pilot files
     :type location: basestring

     :returns: content of the pilot wrapper
     :rtype: basestring
  """

  if pilotFilesCompressedEncodedDict is None:
    pilotFilesCompressedEncodedDict = {}

  if envVariables is None:
    envVariables = {}

  compressedString = ""
  for pfName, encodedPf in pilotFilesCompressedEncodedDict.iteritems():  # are there some pilot files to unpack?
                                                                         # then we create the unpacking string
    compressedString += """
try:
  with open('%(pfName)s', 'w') as fd:
    fd.write(bz2.decompress(base64.b64decode(\"\"\"%(encodedPf)s\"\"\")))
  os.chmod('%(pfName)s', stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
except BaseException as x:
  print >> sys.stderr, x
  shutil.rmtree(pilotWorkingDirectory)
  sys.exit(-1)
""" % {'encodedPf': encodedPf,
       'pfName': pfName}

  envVariablesString = ""
  for name, value in envVariables.iteritems():  # are there some environment variables to add?
    envVariablesString += """
os.environ[\"%(name)s\"]=\"%(value)s\"
""" % {'name': name,
       'value': value}

  # add X509_USER_PROXY to etablish pilot env in Cluster WNs
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

# Getting the json file
rJson = urllib2.urlopen('http://' + '%(location)s' + '/pilot/pilot.json')
with open('pilot.json', 'wb') as pj:
  pj.write(rJson.read())
  pj.close()

# Getting the tar file
rTar = urllib2.urlopen('http://' + '%(location)s' + '/pilot/pilot.tar')
with open('pilot.tar', 'wb') as pt:
  pt.write(rTar.read())
  pt.close()
with tarfile.open('pilot.tar', 'r') as pt:
  pt.extractall()
  pt.close()
""" % {'location': location}

  localPilot += """
# now finally launching the pilot script (which should be called dirac-pilot.py)
cmd = "python dirac-pilot.py %s"
logger.info('Executing: %%s' %% cmd)
sys.stdout.flush()
os.system(cmd)

# and cleaning up
shutil.rmtree(pilotWorkingDirectory)

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
