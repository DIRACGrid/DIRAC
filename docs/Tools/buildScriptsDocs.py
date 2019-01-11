#!/usr/bin/env python
'''buildScriptsDocs

  Build scripts documentation from the scripts docstrings. The scripts are not
  very uniform

'''
import logging
import glob
import os
import sys
import subprocess
import shlex
import sys

from DIRAC import rootPath

logging.basicConfig(level=logging.INFO, format='%(name)s: %(levelname)8s: %(message)s', stream=sys.stdout)
LOG = logging.getLogger('ScriptDoc')

# Scripts that either do not have -h, are obsolete or cause havoc when called
BAD_SCRIPTS = ['dirac-deploy-scripts', 'dirac-install', 'dirac-compile-externals',
               'dirac-install-client',
               'dirac-framework-self-ping', 'dirac-dms-add-files',
               ]

MARKERS_SECTIONS_SCRIPTS = [(['dms'],
                             'Data Management', [], []),
                            (['wms'], 'Workload Management', [], []),
                            (['dirac-proxy', 'dirac-info', 'dirac-version', 'myproxy', 'dirac-platform'],
                             'Others', [], ['dirac-cert-convert.sh']),
                            # (['rss'],'Resource Status Management', [], []),
                            #  (['rms'],'Request Management', [], []),
                            # (['stager'],'Storage Management', [], []),
                            # (['transformation'], 'Transformation Management', [], []),
                            (['admin', 'accounting', 'FrameworkSystem', 'framework', 'install', 'utils',
                              'ConfigurationSystem', 'Core', 'rss', 'transformation', 'stager'], 'Admin',
                             ['dirac-repo-monitor', 'dirac-jobexec'], ['dirac-cert-convert.sh']),
                            # ([''], 'CatchAll', [], []),
                            ]

EXITCODE = 0

def mkdir(path):
  """ save mkdir, ignores exceptions """
  try:
    os.makedirs(path)
  except OSError:
    pass


def runCommand(command):
  """ execute shell command, return output, catch exceptions """
  try:
    result = subprocess.check_output(shlex.split(command), stderr=subprocess.STDOUT)
    if 'NOTICE:' in result:
      LOG.warn('NOTICE in output for: %s', command)
      return ''
    return result
  except (OSError, subprocess.CalledProcessError) as e:
    LOG.error("Error when runnning command %s: %r", command, e)
    return ''


def getScripts():
  """Get all scripts in the Dirac System, split by type admin/wms/rms/other."""

  diracPath = os.path.join(rootPath, 'DIRAC')
  if not os.path.exists(diracPath):
    sys.exit('%s does not exist' % diracPath)

  # Get all scripts
  scriptsPath = os.path.join(diracPath, '*', 'scripts', '*.py')

  # Get all scripts on scriptsPath and sorts them, this will make our life easier afterwards
  scripts = glob.glob(scriptsPath)
  scripts.sort()
  for scriptPath in scripts:
    # Few modules still have __init__.py on the scripts directory
    if '__init__' in scriptPath or 'build' in scriptPath:
      LOG.debug("Ignoring %s", scriptPath)
      continue

    for mT in MARKERS_SECTIONS_SCRIPTS:
      if any(pattern in scriptPath for pattern in mT[0]):
        mT[2].append(scriptPath)
        break

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

.. this page is created in docs/Tools/buildScriptsDocs.py

This page is the work in progress. See more material here soon !

.. toctree::
   :maxdepth: 1

""".lstrip()

  for mT in MARKERS_SECTIONS_SCRIPTS:
    system = mT[1]
    if system == 'Admin':
      continue
    systemString = system.replace(" ", "")
    userIndexRST += "   %s/index\n" % systemString

    LOG.debug("Index file:\n%s", userIndexRST)
    sectionPath = os.path.join(rootPath, 'DIRAC/docs/source/UserGuide/CommandReference/', systemString)
    mkdir(sectionPath)
    createSectionIndex(mT, sectionPath)

  userIndexPath = os.path.join(rootPath, 'DIRAC/docs/source/UserGuide/CommandReference/index.rst')
  with open(userIndexPath, 'w') as userIndexFile:
    LOG.info('Writting to: %s', userIndexPath)
    userIndexFile.write(userIndexRST)


def createAdminGuideCommandReference():
  """Create the command reference for the AdministratorGuide.

  source/AdministratorGuide/CommandReference
  """

  sectionPath = os.path.join(rootPath, 'DIRAC/docs/source/AdministratorGuide/CommandReference/')

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
    if createScriptDocFiles(script, sectionPath, scriptName) and scriptName not in adminCommandList:
      missingCommands.append(scriptName)

  if missingCommands:
    LOG.error("The following admin commands are not in the command index: \n\t\t\t\t%s",
              "\n\t\t\t\t".join(missingCommands))
    global EXITCODE
    EXITCODE = 1


def cleanAdminGuideReference():
  """Make sure no superfluous commands are documented in the AdministratorGuide"""
  existingCommands = {os.path.basename(com).replace('.py', '') for mT in MARKERS_SECTIONS_SCRIPTS for com in mT[2] + mT[3]}
  sectionPath = os.path.join(rootPath, 'DIRAC/docs/source/AdministratorGuide/CommandReference/')
  # read the script index
  documentedCommands = set()
  with open(os.path.join(sectionPath, 'index.rst')) as adminIndexFile:
    adminCommandList = adminIndexFile.readlines()
  for command in adminCommandList:
    if command.strip().startswith('dirac'):
      documentedCommands.add(command.strip())
  LOG.debug('Admin commands: %s', documentedCommands)
  LOG.debug('Existing commands: %s', existingCommands)
  superfluousCommands = documentedCommands - existingCommands
  if superfluousCommands:
    LOG.error('Superfluous commands: \n\t\t\t\t%s',
              '\n\t\t\t\t'.join(sorted(superfluousCommands)))
    global EXITCODE
    EXITCODE = 1

def createSectionIndex(mT, sectionPath):
  """ create the index """

  systemName = mT[1]
  systemHeader = systemName + " Command Reference"
  systemHeader = "%s\n%s\n%s\n" % ("=" * len(systemHeader), systemHeader, "=" * len(systemHeader))
  sectionIndexRST = systemHeader + """
In this subsection the %s commands are collected

.. this page is created in docs/Tools/buildScriptsDocs.py

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
  with open(sectionIndexPath, 'w') as sectionIndexFile:
    LOG.info('Writting to: %s', sectionIndexPath)
    sectionIndexFile.write(sectionIndexRST)


def createScriptDocFiles(script, sectionPath, scriptName):
  """Create the RST files for all the scripts.

  Folders and indices already exist, just call the scripts and get the help messages. Format the help message.

  """
  if scriptName in BAD_SCRIPTS:
    return False

  LOG.info("Creating Doc for %s", scriptName)
  helpMessage = runCommand("%s -h" % scriptName)
  if not helpMessage:
    LOG.warning("NO DOC for %s", scriptName)
    return False

  rstLines = []
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
      LOG.debug("Found general options in line %r", line)
      genOptions = True
      continue
    elif genOptions and line.startswith(' '):
      LOG.debug("Skipping General options line %r", line)
      continue
    elif genOptions and not line.startswith(' '):
      LOG.debug("General options done")
      genOptions = False

    newLine = '\n' + line + ':\n' if line.endswith(':') else line
    # ensure dedented lines are separated by newline from previous block
    if lineIndented and not newLine.startswith(' '):
      newLine = '\n' + newLine
    rstLines.append(newLine)
    lineIndented = newLine.startswith(' ')

  scriptRSTPath = os.path.join(sectionPath, scriptName + '.rst')
  fileContent = '\n'.join(rstLines).strip() + '\n'
  for marker in ('example', '.. note::'):
    content = getContentFromScriptDoc(scriptRSTPath, marker)
    if content and marker not in fileContent.lower():
      fileContent += '\n' + content.strip() + '\n'
    LOG.debug('\n' + '*' * 88 + '\n' + fileContent + '\n' + '*' * 88)
  while '\n\n\n' in fileContent:
    fileContent = fileContent.replace('\n\n\n', '\n\n')

  # remove the standalone '-' when no short option exists
  fileContent = fileContent.replace('-   --', '--')
  with open(scriptRSTPath, 'w') as rstFile:
    LOG.info('Writting to: %s', scriptRSTPath)
    rstFile.write(fileContent)
  return True


def getContentFromScriptDoc(scriptRSTPath, marker):
  """Get an some existing information, if any, from an existing file."""
  content = []
  inContent = False
  if not os.path.exists(scriptRSTPath):
    LOG.warn('Script file %r does not exist yet!', scriptRSTPath)
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


def run():
  ''' creates the rst files right in the source tree of the docs
  '''
  getScripts()
  createUserGuideFoldersAndIndices()
  createAdminGuideCommandReference()
  cleanAdminGuideReference()

  LOG.info('Done')


if __name__ == "__main__":
  run()
  exit(EXITCODE)
