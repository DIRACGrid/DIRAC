#!/usr/bin/env python
"""Module to create command references for DIRAC."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import ast
import logging
import glob
import os
import shutil
import textwrap

from diracdoctools.Utilities import writeLinesToFile, mkdir, runCommand, makeLogger
from diracdoctools.Config import Configuration


LOG = makeLogger('CommandReference')


TITLE = 'title'
PATTERN = 'pattern'
SCRIPTS = 'scripts'
MANUAL = 'manual'
EXCLUDE = 'exclude'
SECTION_PATH = 'sectionPath'
INDEX_FILE = 'indexFile'
PREFIX = 'prefix'


class CommandReference(object):

  def __init__(self, configFile='docs.conf', debug=False):
    self.config = Configuration(configFile, sections=['Commands'])
    self.exitcode = 0
    self.debug = debug

    self.sectionDicts = self.config.com_MSS

  def getScripts(self):
    """Get all scripts in the Dirac System, split by type admin/wms/rms/other."""
    LOG.info('Looking for scripts')
    if not os.path.exists(self.config.sourcePath):
      LOG.error('%s does not exist' % self.config.sourcePath)
      raise RuntimeError('Package not found')

    # Get all scripts
    scriptsPath = os.path.join(self.config.sourcePath, '*', 'scripts', '*.py')

    # Get all scripts on scriptsPath and sorts them, this will make our life easier afterwards
    scripts = glob.glob(scriptsPath)
    scripts.sort()
    for scriptPath in scripts:
      # Few modules still have __init__.py on the scripts directory
      if '__init__' in scriptPath:
        LOG.debug('Ignoring init file %s', scriptPath)
        continue

      for mT in self.sectionDicts:
        if any(pattern in scriptPath for pattern in mT[PATTERN]) and \
           not any(pattern in scriptPath for pattern in mT[EXCLUDE]):
          mT[SCRIPTS].append(scriptPath)

    return

  def createFilesAndIndex(self, sectionDict):
    """Create the index file and folder where the RST files will go

    e.g.:
    source/UserGuide/CommandReference/DataManagement
    """
    sectionPath = os.path.join(self.config.docsPath, sectionDict[SECTION_PATH])
    mkdir(sectionPath)

    systemName = sectionDict[TITLE]
    systemHeader = systemName + " Command Reference"
    systemHeader = "%s\n%s\n%s\n" % ("=" * len(systemHeader), systemHeader, "=" * len(systemHeader))
    sectionIndexRST = systemHeader + textwrap.dedent("""
                                                     In this subsection the %s commands are collected

                                                     .. this page automatically is created in %s


                                                     .. toctree::
                                                        :maxdepth: 2

                                                     """ % (systemName, __name__))

    listOfScripts = []
    # these scripts use pre-existing rst files, cannot re-create them automatically
    listOfScripts.extend(sectionDict[MANUAL])
    sectionPath = os.path.join(self.config.docsPath, sectionDict[SECTION_PATH])
    for script in sectionDict[SCRIPTS]:
      scriptName = os.path.basename(script)
      if scriptName.endswith('.py'):
        scriptName = scriptName[:-3]
      prefix = sectionDict[PREFIX].lower()
      prefix = prefix + '_' if prefix else ''
      if self.createScriptDocFiles(script, sectionPath, scriptName, referencePrefix=prefix):
        listOfScripts.append(scriptName)

    for scriptName in sorted(listOfScripts):
      sectionIndexRST += "   %s\n" % scriptName

    writeLinesToFile(os.path.join(self.config.docsPath, sectionDict[SECTION_PATH], 'index.rst'), sectionIndexRST)

  def createFiles(self, sectionDict):
    """Create the command reference when an index already exists.

    source/AdministratorGuide/CommandReference
    """

    sectionPath = os.path.join(self.config.docsPath, sectionDict[SECTION_PATH])
    LOG.info('Creating references for %r', sectionPath)
    # read the script index
    with open(os.path.join(sectionPath, 'index.rst')) as indexFile:
      commandList = indexFile.read().replace('\n', '')

    missingCommands = []
    for script in sectionDict[SCRIPTS] + sectionDict[MANUAL]:
      scriptName = os.path.basename(script)
      if scriptName.endswith('.py'):
        scriptName = scriptName[:-3]
      prefix = sectionDict[PREFIX].lower()
      prefix = prefix + '_' if prefix else ''
      if self.createScriptDocFiles(script, sectionPath, scriptName, referencePrefix=prefix) and \
         scriptName not in commandList:
        missingCommands.append(scriptName)

    if missingCommands:
      LOG.error("The following commands are not in the command index: \n\t\t\t\t%s",
                "\n\t\t\t\t".join(missingCommands))
      LOG.error("Add them to docs/source/AdministratorGuide/CommandReference/index.rst")
      self.exitcode = 1

  def cleanExistingIndex(self, sectionDict):
    """Make sure no superfluous commands are documented in an existing index file.

    If an rst file exists for a command, we move it.
    An existing entry for a non existing rst file will create a warning when running sphinx.
    """
    existingCommands = {os.path.basename(com).replace('.py', '') for com in sectionDict[SCRIPTS] + sectionDict[MANUAL]}
    sectionPath = os.path.join(self.config.docsPath, sectionDict[SECTION_PATH])
    LOG.info('Checking %r for non-existent commands', sectionPath)
    # read the script index
    documentedCommands = set()
    with open(os.path.join(sectionPath, 'index.rst')) as indexFile:
      commandList = indexFile.readlines()
    for command in commandList:
      if command.strip().startswith('dirac'):
        documentedCommands.add(command.strip())
    LOG.debug('Documented commands: %s', documentedCommands)
    LOG.debug('Existing commands: %s', existingCommands)
    superfluousCommands = documentedCommands - existingCommands
    if superfluousCommands:
      LOG.error(
          'Commands that are documented, but do not exist and should be removed from the index page: \n\t\t\t\t%s',
          '\n\t\t\t\t'.join(
              sorted(superfluousCommands)))
      for com in superfluousCommands:
        commandDocPath = os.path.join(sectionPath, com + '.rst')
        if os.path.exists(commandDocPath):
          shutil.move(commandDocPath, os.path.join(sectionPath, 'obs_' + com + '.rst'))
      self.exitcode = 1

  def createScriptDocFiles(self, script, sectionPath, scriptName, referencePrefix=''):
    """Create the RST files for all the scripts.

    Folders and indices already exist, just call the scripts and get the help messages. Format the help message.

    """
    if scriptName in self.config.com_ignore_commands:
      return False

    LOG.info('Creating Doc for %r in %r', scriptName, sectionPath)
    helpMessage = runCommand('python %s -h' % script)
    if not helpMessage:
      LOG.warning('NO DOC for %s', scriptName)
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
        LOG.debug("Found general options in line %r", line) if self.debug else None
        genOptions = True
        continue
      elif genOptions and line.startswith(' '):
        LOG.debug("Skipping General options line %r", line) if self.debug else None
        continue
      elif genOptions and not line.startswith(' '):
        LOG.debug("General options done") if self.debug else None
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
      if scriptName in self.config.com_module_docstring:
        if index == 0:
          content = self.getContentFromModuleDocstring(script)
          fileContent += '\n' + content.strip() + '\n'
      else:
        content = self.getContentFromScriptDoc(scriptRSTPath, marker)
        if not content:
          break  # nothing in content, files probably does not exist
        if content and marker not in fileContent.lower():
          fileContent += '\n' + content.strip() + '\n'
      LOG.debug('\n' + '*' * 88 + '\n' + fileContent + '\n' + '*' * 88) if self.debug else None
    while '\n\n\n' in fileContent:
      fileContent = fileContent.replace('\n\n\n', '\n\n')

    # remove the standalone '-' when no short option exists
    fileContent = fileContent.replace('-   --', '--')
    writeLinesToFile(scriptRSTPath, fileContent)
    return True

  def getContentFromModuleDocstring(self, script):
    """Parse the given python file and return its module docstring."""
    LOG.info('Checking AST for modulestring: %s', script)
    try:
      with open(script) as scriptContent:
        parse = ast.parse(scriptContent.read(), script)
        return ast.get_docstring(parse)
    except IOError as e:
      self.exitcode = 1
      LOG.error('Cannot open %r: %r', script, e)
    return ''

  def getContentFromScriptDoc(self, scriptRSTPath, marker):
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


def run(configFile='docs.conf', logLevel=logging.INFO, debug=False):
  """Create the rst files for dirac commands, parsed form the --help message.

  :param str configFile: path to the configFile
  :param logLevel: logging level to use
  :param bool debug: if true even more debug information is printed
  :returns: return value 1 or 0
  """
  logging.getLogger().setLevel(logLevel)
  commands = CommandReference(configFile=configFile, debug=debug)
  commands.getScripts()
  for sectionDict in commands.sectionDicts:
    if sectionDict[INDEX_FILE] is None:
      commands.createFilesAndIndex(sectionDict)
    else:
      commands.createFiles(sectionDict)
      commands.cleanExistingIndex(sectionDict)

  LOG.info('Done')
  return commands.exitcode


if __name__ == "__main__":
  exit(run())
