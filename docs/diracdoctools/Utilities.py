"""Utilities used by the documentation scripts."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from __future__ import absolute_import

from builtins import open

import atexit
import os
import re
import sys
import shlex
import logging
import subprocess32 as subprocess


LOG = logging.getLogger(__name__)


def mkdir(folder):
  """Create a folder, ignore if it exists.

  :param str folder: folder to create
  """
  try:
    folder = os.path.join(os.getcwd(), folder)
    os.mkdir(folder)
  except OSError as e:
    LOG.debug('Exception when creating folder %s: %r', folder, e)


def writeLinesToFile(filename, lines):
  """Write a list of lines into a file.

  Checks that there are actual changes to be done.

  :param str filename: name of the files
  :param list lines: list of lines to write to the file
  """
  if isinstance(lines, list):
    newContent = '\n'.join(lines)
  else:
    newContent = lines
  oldContent = None
  if os.path.exists(filename):
    with open(filename, 'r') as oldFile:
      oldContent = ''.join(oldFile.readlines())
  # try creating the directory in case it is not there
  # and filename is an absolute path
  elif os.path.isabs(filename):
    try:
      os.makedirs(os.path.dirname(filename))
    # the directory may already exist
    except OSError:
      LOG.debug('Cannot create directory %s', os.path.dirname(filename))
  if oldContent is None or oldContent != newContent:
    with open(filename, 'w') as rst:
      LOG.info('Writing new content for %s', os.path.join(os.getcwd(), filename))
      try:  # decode only needed/possible in python2
        newContent = newContent.decode('utf-8')
      except AttributeError:
        # ignore decode if newContent is python3 str
        pass
      except (UnicodeDecodeError) as e:
        LOG.error('Failed to decode newContent with "utf-8": %r', e)
        raise
      rst.write(newContent)
  else:
    LOG.debug('Not updating file content for %s', os.path.join(os.getcwd(), filename))


def setUpReadTheDocsEnvironment(moduleName='DIRAC', location='../../'):
  """Create the necessary links and environment variables to create documentation inside readthedocs.

  Ensure that ``moduleName`` is in the PYTHONPATH by creating a link pointing to the basefolder of the source code.

  :param str moduleName: name of the base source code module, default 'DIRAC'
  :param str location: Path to the location where the module can be found relative to the 'conf.py'. Default '../../'
  """
  LOG.info('Running for READTHEDOCS')
  sys.path.append(os.path.abspath('.'))
  diracPath = os.path.abspath(os.path.join(os.getcwd(), location))
  LOG.info('Path To Module(?): %r', diracPath)

  buildfolder = '_build'
  mkdir(os.path.abspath('../' + buildfolder))

  # We need to have the moduleName somewhere, or we cannot import it, as
  # readtheDocs clones the repo into something based on the branchname
  if not os.path.exists(os.path.join(location, moduleName)):
    diracLink = os.path.abspath(os.path.join(os.getcwd(), '../', buildfolder, moduleName))
    LOG.info('Link: %r', diracLink)
    if not os.path.exists(diracLink):
      LOG.info('Creating symbolic link %r -> %r', diracPath, diracLink)
      os.symlink(diracPath, diracLink)
    diracPath = os.path.abspath(os.path.join(diracLink, '..'))

  sys.path.insert(0, diracPath)

  for path in sys.path:
    LOG.info('Adding locations to PYTHONPATH: %r', path)
    os.environ['PYTHONPATH'] = os.environ.get('PYTHONPATH', '') + ':' + path

  os.environ['DIRAC'] = diracPath
  LOG.info('DIRAC ENVIRON: %r', os.environ['DIRAC'])


def runCommand(command):
  """Execute shell command, return output, catch exceptions."""
  try:
    result = subprocess.check_output(shlex.split(command), stderr=subprocess.STDOUT,
                                     universal_newlines=True)
    if 'NOTICE:' in result:
      lines = []
      LOG.warning('NOTICE in output for: %s; cleaning output from datestamp..', command)
      for line in result.split('\n'):
        lines.append(re.sub(r"^.*NOTICE: ", "", line))
      # if the output is less than 3 lines something went wrong
      result = "\n".join(lines) if len(lines) > 2 else ''
    return result
  except (OSError, subprocess.CalledProcessError) as e:
    LOG.error('Error when runnning command %s: %r', command, getattr(e, "output", None))
    return ''


def makeLogger(name):
  """Create a logger and return instance."""
  logging.basicConfig(level=logging.INFO, format='%(name)25s: %(levelname)8s: %(message)s',
                      stream=sys.stdout)
  return logging.getLogger(name)


def registerValidatingExitHandler():
  """Registers an exit handler which checks for errors after the build completes"""
  def check():
    outputDir = "build/html"
    for arg in sys.argv:
      if arg.endswith("/html"):
        outputDir = arg
        LOG.info("Found outputDir as %s", outputDir)
        break
    cmd = [
        "grep", "--color", "-nH",
        "-e", " :param",  # :param: is a legit thing that happens,
        "-e", ":param ",  # so we look for space before or after
        "-e", ":return", "-r", os.path.join(outputDir, "CodeDocumentation")
    ]
    ret = subprocess.run(cmd, check=False)
    if ret.returncode != 1:
      print("Return code from {} was {}".format(" ".join(cmd), ret.returncode))
      print("This means :param or :return in the html and points to faulty "
            "syntax, missing empty lines, etc.")
      # https://bugs.python.org/issue27035
      os._exit(1)  # pylint: disable=protected-access

  atexit.register(check)
