"""Utilities used by the documentation scripts."""

import logging
import os
import sys

LOG = logging.getLogger(__name__)

# name of the Package
BASE_MODULE_NAME = 'DIRAC'

# Location where we can find CustomizedDocs
CODE_CUSTOM_DOCS_FOLDER = os.path.join(os.path.basename(__file__), 'CustomizedDocs/')

# Add private members in autodoc
CODE_FORCE_ADD_PRIVATE = ['FCConditionParser']

# inherited functions give warnings in docstrings
CODE_NO_INHERITED = []

# where in doc the code documentation ends up
CODE_DOC_TARGET_PATH = os.path.join(packagePath(), 'docs/source/CodeDocumentation')

# files that call parseCommandLine or similar issues
CODE_BAD_FILES = ('lfc_dfc_copy',
                  'lfc_dfc_db_copy',
                  'JobWrapperTemplate',
                  'PlotCache',  # PlotCache creates a thread on import, which keeps sphinx from exiting
                  'PlottingHandler',
                  'setup.py',  # configuration for style check
                  )


# list of commands: get the module docstring from the file to add to the docstring
COMMANDS_GET_MOD_STRING = ['dirac-install',
                           ]

# Scripts that either do not have -h, are obsolete or cause havoc when called
COMMANDS_BAD_SCRIPTS = ['dirac-deploy-scripts',  # does not have --help, deploys scripts
                        'dirac-compile-externals',  # does not have --help, starts compiling externals
                        'dirac-install-client',  # does not have --help
                        'dirac-framework-self-ping',  # does not have --help
                        'dirac-dms-add-files',  # obsolete
                        'dirac-version',  # just prints version, no help
                        'dirac-platform',  # just prints platform, no help
                        'dirac-agent',  # no doc, purely internal use
                        'dirac-executor',  # no doc, purely internal use
                        'dirac-service',  # no doc, purely internal use
                        ]


# tuples: list of patterns to match in script names,
#         Title of the index file
#         list of script names, filled during search, can be pre-filled
#         list of patterns to reject scripts
COMMANDS_MARKERS_SECTIONS_SCRIPTS = [
    (['dms'], 'Data Management', [], []),
    (['wms'], 'Workload Management', [], []),
    (['dirac-proxy', 'dirac-info', 'myproxy'], 'Others', [], ['dirac-cert-convert.sh', 'dirac-platform', 'dirac-version']),
    (['admin', 'accounting', 'FrameworkSystem', 'framework', 'install', 'utils', 'dirac-repo-monitor', 'dirac-jobexec',
      'dirac-info', 'ConfigurationSystem', 'Core', 'rss', 'transformation', 'stager'], 'Admin',
        [], ['dirac-cert-convert.sh', 'dirac-platform', 'dirac-version']),
]


def packagePath():
  """Where the source code can be found."""
  return os.path.join(os.environ.get('DIRAC', ''), BASE_MODULE_NAME)


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
  if not os.path.exists(os.path.join('../../', BASE_MODULE_NAME)):
    diracLink = os.path.abspath(os.path.join(os.getcwd(), '../', buildfolder, BASE_MODULE_NAME))
    LOG.info('DiracLink: %r', diracLink)
    if not os.path.exists(diracLink):
      LOG.info('Creating symbolic link')
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
    result = subprocess.check_output(shlex.split(command), stderr=subprocess.STDOUT)
    if 'NOTICE:' in result:
      LOG.warn('NOTICE in output for: %s', command)
      return ''
    return result
  except (OSError, subprocess.CalledProcessError) as e:
    LOG.error('Error when runnning command %s: %r', command, e.output)
    return ''
