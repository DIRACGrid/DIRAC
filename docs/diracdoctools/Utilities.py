"""Utilities used by the documentation scripts."""

import logging
import os
import sys
import subprocess32 as subprocess
import shlex


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
  if oldContent is None or oldContent != newContent:
    with open(filename, 'w') as rst:
      LOG.info('Writing new content for %s', os.path.join(os.getcwd(), filename))
      rst.write(newContent)
  else:
    LOG.debug('Not updating file content for %s', os.path.join(os.getcwd(), filename))


def setUpReadTheDocsEnvironment(moduleName='DIRAC'):
  """Create the necessary links and environment variables to create documentation inside readthedocs.

  Ensure that ``moduleName`` is in the PYTHONPATH by creating a link pointing to the basefolder of the source code.

  :param str moduleName: name of the base source code module, default 'DIRAC'
  """
  LOG.info('Running for READTHEDOCS')
  sys.path.append(os.path.abspath('.'))
  diracPath = os.path.abspath(os.path.join(os.getcwd(), '../..'))
  LOG.info('Path To Module(?): %r', diracPath)

  buildfolder = '_build'
  mkdir(os.path.abspath('../' + buildfolder))

  # We need to have the moduleName somewhere, or we cannot import it, as
  # readtheDocs clones the repo into something based on the branchname
  if not os.path.exists(os.path.join('../../', moduleName)):
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
      LOG.warn('NOTICE in output for: %s', command)
      return ''
    return result
  except (OSError, subprocess.CalledProcessError) as e:
    LOG.error('Error when runnning command %s: %r', command, e.output)
    return ''


def makeLogger(name):
  """Create a logger and return instance."""
  logging.basicConfig(level=logging.INFO, format='%(name)25s: %(levelname)8s: %(message)s',
                      stream=sys.stdout)
  return logging.getLogger(name)
