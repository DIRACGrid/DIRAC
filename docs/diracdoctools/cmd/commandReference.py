"""Module to create command references for DIRAC."""
import ast
import logging
import glob
import os
import shutil
import sys
import subprocess
import shlex

from diracdoctools.Utilities import writeLinesToFile, mkdir, PACKAGE_PATH

# if true (-ddd on the command line) print also the content for all files
SUPER_DEBUG = False

logging.basicConfig(level=logging.INFO, format='%(name)s: %(levelname)8s: %(message)s', stream=sys.stdout)
LOG = logging.getLogger('ScriptDoc')

# Scripts that either do not have -h, are obsolete or cause havoc when called
BAD_SCRIPTS = ['dirac-deploy-scripts',  # does not have --help, deploys scripts
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


# list of commands: get the module docstring from the file to add to the docstring
GET_MOD_STRING = ['dirac-install',
                  ]


# tuples: list of patterns to match in script names,
#         Title of the index file
#         list of script names
#         list of patterns to reject scripts
MARKERS_SECTIONS_SCRIPTS = [(['dms'],
                             'Data Management', [], []),
                            (['wms'], 'Workload Management', [], []),
                            (['dirac-proxy', 'dirac-info', 'myproxy'],
                             'Others', [], ['dirac-cert-convert.sh', 'dirac-platform', 'dirac-version']),
                            # (['rss'],'Resource Status Management', [], []),
                            #  (['rms'],'Request Management', [], []),
                            # (['stager'],'Storage Management', [], []),
                            # (['transformation'], 'Transformation Management', [], []),
                            (['admin', 'accounting', 'FrameworkSystem', 'framework', 'install', 'utils',
                              'dirac-repo-monitor', 'dirac-jobexec', 'dirac-info',
                              'ConfigurationSystem', 'Core', 'rss', 'transformation', 'stager'], 'Admin',
                             [], ['dirac-cert-convert.sh', 'dirac-platform', 'dirac-version']),
                            # ([''], 'CatchAll', [], []),
                            ]

EXITCODE = 0


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


def getScripts():
  """Get all scripts in the Dirac System, split by type admin/wms/rms/other."""

  if not os.path.exists(PACKAGE_PATH):
    LOG.error('%s does not exist' % PACKAGE_PATH)
    raise RuntimeError('Package not found')

  # Get all scripts
  scriptsPath = os.path.join(PACKAGE_PATH, '*', 'scripts', '*.py')

  # Get all scripts on scriptsPath and sorts them, this will make our life easier afterwards
  scripts = glob.glob(scriptsPath)
  scripts.sort()
  for scriptPath in scripts:
    # Few modules still have __init__.py on the scripts directory
    if '__init__' in scriptPath:
      LOG.debug('Ignoring init file %s', scriptPath)
      continue

    for mT in MARKERS_SECTIONS_SCRIPTS:
      if any(pattern in scriptPath for pattern in mT[0]) and \
         not any(pattern in scriptPath for pattern in mT[3]):
        mT[2].append(scriptPath)

  return


def createUserGuideFoldersAndIndices():
  """ creates the index files and folders where the RST files will go

  e.g.:
  source/UserGuide/CommandReference
  """
  # create the main UserGuide Index file
  userIndexRST = """
==================
Commands Reference
==================

.. this page is created in docs/Tools/dirac-docs-build-commands.py


This page is the work in progress. See more material here soon !

.. toctree::
   :maxdepth: 1

""".lstrip()

  for mT in MARKERS_SECTIONS_SCRIPTS:
    system = mT[1]
    if system == 'Admin':
      continue
    systemString = system.replace(' ', '')
    userIndexRST += '   %s/index\n' % systemString

    LOG.debug('Index file:\n%s', userIndexRST) if SUPER_DEBUG else None
    sectionPath = os.path.join(PACKAGE_PATH, 'docs/source/UserGuide/CommandReference/', systemString)
    mkdir(sectionPath)
    createSectionIndex(mT, sectionPath)

  userIndexPath = os.path.join(PACKAGE_PATH, 'docs/source/UserGuide/CommandReference/index.rst')
  writeLinesToFile(userIndexPath, userIndexRST)


def createAdminGuideCommandReference():
  """Create the command reference for the AdministratorGuide.

  source/AdministratorGuide/CommandReference
  """

  sectionPath = os.path.join(PACKAGE_PATH, 'docs/source/AdministratorGuide/CommandReference/')

  # read the script index
  with open(os.path.join(sectionPath, 'index.rst')) as adminIndexFile:
    adminCommandList = adminIndexFile.read().replace('\n', '')

  # find the list of admin scripts
  mT = ([], '', [])
  for mT in MARKERS_SECTIONS_SCRIPTS:
    if mT[1] == 'Admin':
      break

  missingCommands = []
  for script in mT[2]:
    scriptName = os.path.basename(script)
    if scriptName.endswith('.py'):
      scriptName = scriptName[:-3]
    if createScriptDocFiles(script, sectionPath, scriptName, referencePrefix='admin_') and \
       scriptName not in adminCommandList:
      missingCommands.append(scriptName)

  if missingCommands:
    LOG.error("The following admin commands are not in the command index: \n\t\t\t\t%s",
              "\n\t\t\t\t".join(missingCommands))
    global EXITCODE
    EXITCODE = 1


def cleanAdminGuideReference():
  """Make sure no superfluous commands are documented in the AdministratorGuide"""
  existingCommands = {os.path.basename(com).replace('.py', '') for mT in MARKERS_SECTIONS_SCRIPTS
                      for com in mT[2] + mT[3] if mT[1] == 'Admin'}
  sectionPath = os.path.join(PACKAGE_PATH, 'docs/source/AdministratorGuide/CommandReference/')
  # read the script index
  documentedCommands = set()
  with open(os.path.join(sectionPath, 'index.rst')) as adminIndexFile:
    adminCommandList = adminIndexFile.readlines()
  for command in adminCommandList:
    if command.strip().startswith('dirac'):
      documentedCommands.add(command.strip())
  LOG.debug('Documented commands: %s', documentedCommands)
  LOG.debug('Existing commands: %s', existingCommands)
  superfluousCommands = documentedCommands - existingCommands
  if superfluousCommands:
    LOG.error('Commands that are documented, but do not exist and should be removed from the index page: \n\t\t\t\t%s',
              '\n\t\t\t\t'.join(sorted(superfluousCommands)))
    for com in superfluousCommands:
      shutil.move(os.path.join(sectionPath, com + '.rst'), os.path.join(sectionPath, 'obs_' + com + '.rst'))
    global EXITCODE
    EXITCODE = 1


def createSectionIndex(mT, sectionPath):
  """ create the index """

  systemName = mT[1]
  systemHeader = systemName + " Command Reference"
  systemHeader = "%s\n%s\n%s\n" % ("=" * len(systemHeader), systemHeader, "=" * len(systemHeader))
  sectionIndexRST = systemHeader + """
In this subsection the %s commands are collected

.. this page is created in docs/Tools/dirac-docs-build-commands.py


.. toctree::
   :maxdepth: 2

""" % systemName

  listOfScripts = []
  # these scripts use pre-existing rst files, cannot re-create them automatically
  listOfScripts.extend(mT[3])

  for script in mT[2]:
    scriptName = os.path.basename(script)
    if scriptName.endswith('.py'):
      scriptName = scriptName[:-3]
    if createScriptDocFiles(script, sectionPath, scriptName):
      listOfScripts.append(scriptName)

  for scriptName in sorted(listOfScripts):
    sectionIndexRST += "   %s\n" % scriptName

  sectionIndexPath = os.path.join(sectionPath, 'index.rst')
  writeLinesToFile(sectionIndexPath, sectionIndexRST)


def createScriptDocFiles(script, sectionPath, scriptName, referencePrefix=''):
  """Create the RST files for all the scripts.

  Folders and indices already exist, just call the scripts and get the help messages. Format the help message.

  """
  if scriptName in BAD_SCRIPTS:
    return False

  LOG.info("Creating Doc for %s", scriptName)
  helpMessage = runCommand("python %s -h" % script)
  if not helpMessage:
    LOG.warning("NO DOC for %s", scriptName)
    return False

  rstLines = []
  rstLines.append(' .. _%s%s:' % (referencePrefix, scriptName))
  rstLines.append('')
  rstLines.append('=' * len(scriptName))
  rstLines.append('%s' % scriptName)
  rstLines.append('=' * len(scriptName))
  rstLines.append('')
  lineIndented = False
  genOptions = False
  for line in helpMessage.splitlines():
    line = line.rstrip()
    if not line:
      pass
    # strip general options from documentation
    elif line.lower().strip() == 'general options:':
      LOG.debug("Found general options in line %r", line) if SUPER_DEBUG else None
      genOptions = True
      continue
    elif genOptions and line.startswith(' '):
      LOG.debug("Skipping General options line %r", line) if SUPER_DEBUG else None
      continue
    elif genOptions and not line.startswith(' '):
      LOG.debug("General options done") if SUPER_DEBUG else None
      genOptions = False

    newLine = '\n' + line + ':\n' if line.endswith(':') else line
    # ensure dedented lines are separated by newline from previous block
    if lineIndented and not newLine.startswith(' '):
      newLine = '\n' + newLine
    rstLines.append(newLine)
    lineIndented = newLine.startswith(' ')

  scriptRSTPath = os.path.join(sectionPath, scriptName + '.rst')
  fileContent = '\n'.join(rstLines).strip() + '\n'

  for index, marker in enumerate(['example', '.. note::']):
    if scriptName in GET_MOD_STRING:
      if index == 0:
        content = getContentFromModuleDocstring(script)
        fileContent += '\n' + content.strip() + '\n'
    else:
      content = getContentFromScriptDoc(scriptRSTPath, marker)
      if not content:
        break  # nothing in content, files probably does not exist
      if content and marker not in fileContent.lower():
        fileContent += '\n' + content.strip() + '\n'
    LOG.debug('\n' + '*' * 88 + '\n' + fileContent + '\n' + '*' * 88) if SUPER_DEBUG else None
  while '\n\n\n' in fileContent:
    fileContent = fileContent.replace('\n\n\n', '\n\n')

  # remove the standalone '-' when no short option exists
  fileContent = fileContent.replace('-   --', '--')
  writeLinesToFile(scriptRSTPath, fileContent)
  return True


def getContentFromModuleDocstring(script):
  """Parse the given python file and return its module docstring."""
  LOG.info('Checking AST for modulestring: %s', script)
  try:
    with open(script) as scriptContent:
      parse = ast.parse(scriptContent.read(), script)
      return ast.get_docstring(parse)
  except IOError as e:
    global EXITCODE
    EXITCODE = 1
    LOG.error('Cannot open %r: %r', script, e)
  return ''


def getContentFromScriptDoc(scriptRSTPath, marker):
  """Get an some existing information, if any, from an existing file."""
  content = []
  inContent = False
  if not os.path.exists(scriptRSTPath):
    LOG.info('Script file %r does not exist yet!', scriptRSTPath)
    return ''
  with open(scriptRSTPath) as rstFile:
    for line in rstFile.readlines():
      if inContent and not line.rstrip():
        content.append(line)
        continue
      line = line.rstrip()
      if line and inContent and line.startswith(' '):
        content.append(line)
      elif line and inContent and not line.startswith(' '):
        inContent = False
      elif line.lower().startswith(marker.lower()):
        inContent = True
        content.append(line)

  return '\n'.join(content)


def run(arguments=sys.argv):
  """Create the rst files right in the source tree of the docs."""
  global SUPER_DEBUG
  if '-ddd' in ''.join(arguments):
    LOG.setLevel(logging.DEBUG)
    SUPER_DEBUG = True
  if '-dd' in ''.join(arguments):
    LOG.setLevel(logging.DEBUG)
    SUPER_DEBUG = False
  getScripts()
  createUserGuideFoldersAndIndices()
  createAdminGuideCommandReference()
  cleanAdminGuideReference()

  LOG.info('Done')
  return EXITCODE


if __name__ == "__main__":
  exit(run())
