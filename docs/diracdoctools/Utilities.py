"""Utilities used by the documentation scripts."""

import logging
import os
import sys

LOG = logging.getLogger(__name__)

# name of the Package
BASE_MODULE_NAME = 'DIRAC'

# where the source code can be found
PACKAGE_PATH = os.path.join(os.environ.get('DIRAC', ''), BASE_MODULE_NAME)


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
      LOG.info('Writing new content for %s', filename)
      rst.write(newContent)
  else:
    LOG.debug('Not updating file content for %s', filename)


def setUpReadTheDocsEnvironment():
  """Create the necessary links and environment variables to create documentation inside readthedocs.

  Ensure that DIRAC is in the PYTHONPATH by creating a link pointing to the basefolder of the source code.
  """
  LOG.info('Running for READTHEDOCS')
  sys.path.append(os.path.abspath('.'))
  diracPath = os.path.abspath(os.path.join(os.getcwd(), '../..'))
  LOG.info('DiracPath: %r', diracPath)

  buildfolder = '_build'
  mkdir(os.path.abspath('../' + buildfolder))

  # We need to have the DIRAC module somewhere, or we cannot import it, as
  # readtheDocs clones the repo into something based on the branchname
  if not os.path.exists('../../DIRAC'):
    diracLink = os.path.abspath(os.path.join(os.getcwd(), '../', buildfolder, 'DIRAC'))
    LOG.info('DiracLink: %r', diracLink)
    if not os.path.exists(diracLink):
      LOG.info('Creating symbolic link')
      os.symlink(diracPath, diracLink)
    diracPath = os.path.abspath(os.path.join(diracLink, '..'))

  sys.path.insert(0, diracPath)

  for path in sys.path:
    LOG.info('Adding locations to PYTHONPATH')
    os.environ['PYTHONPATH'] = os.environ.get('PYTHONPATH', '') + ':' + path

  os.environ['DIRAC'] = diracPath
  LOG.info('DIRAC ENVIRON: %r', os.environ['DIRAC'])
