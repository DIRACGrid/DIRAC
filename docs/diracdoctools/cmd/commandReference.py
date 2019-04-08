"""Module to create command references for DIRAC."""
import ast
import logging
import glob
import os
import shutil
import sys
import shlex
import textwrap
from diracdoctools.Utilities import writeLinesToFile, mkdir, runCommand
from diracdoctools.Config import Configuration


logging.basicConfig(level=logging.INFO, format='%(name)25s: %(levelname)8s: %(message)s', stream=sys.stdout)
LOG = logging.getLogger('CommandReference')


TITLE = 'title'
PATTERN = 'pattern'
SCRIPTS = 'scripts'
IGNORE = 'ignore'
EXISTING_INDEX = 'existingIndex'
SECTION_PATH = 'sectionPath'


class CommandReference(object):

  def __init__(self, confFile='docs.conf', debug=False):
    self.config = Configuration(confFile)
    self.exitcode = 0
    self.debug = debug
    # tuples: list of patterns to match in script names,
    #         Title of the index file
    #         list of script names, filled during search, can be pre-filled
    #         list of patterns to reject scripts
    #         existing index
#     self.commands_markers_sections_scripts = [
#       (['dms'], 'Data Management', [], []),
#       (['wms'], 'Workload Management', [], []),
#       (['dirac-proxy', 'dirac-info', 'myproxy'], 'Others', [], ['dirac-cert-convert.sh', 'dirac-platform', 'dirac-version']),
#       (['admin', 'accounting', 'FrameworkSystem', 'framework', 'install', 'utils', 'dirac-repo-monitor', 'dirac-jobexec',
#         'dirac-info', 'ConfigurationSystem', 'Core', 'rss', 'transformation', 'stager'], 'Admin',
#        [], ['dirac-cert-convert.sh', 'dirac-platform', 'dirac-version']),
# ]

    self.commands_markers_sections_scripts = self.config.com_MSS

  def getScripts(self):
    """Get all scripts in the Dirac System, split by type admin/wms/rms/other."""
    LOG.info('Looking for scripts')
    if not os.path.exists(self.config.packagePath):
      LOG.error('%s does not exist' % self.config.packagePath)
      raise RuntimeError('Package not found')

    # Get all scripts
    scriptsPath = os.path.join(self.config.packagePath, '*', 'scripts', '*.py')

    # Get all scripts on scriptsPath and sorts them, this will make our life easier afterwards
    scripts = glob.glob(scriptsPath)
    scripts.sort()
    for scriptPath in scripts:
      # Few modules still have __init__.py on the scripts directory
      if '__init__' in scriptPath:
        LOG.debug('Ignoring init file %s', scriptPath)
        continue

      for mT in self.commands_markers_sections_scripts:
        if any(pattern in scriptPath for pattern in mT[PATTERN]) and \
           not any(pattern in scriptPath for pattern in mT[IGNORE]):
          mT[SCRIPTS].append(scriptPath)

    return

  def createUserGuideFoldersAndIndices(self):
    """ creates the index files and folders where the RST files will go

    e.g.:
    source/UserGuide/CommandReference
    """
    # create the main UserGuide Index file
    userIndexRST = textwrap.dedent("""
    ==================
    Commands Reference
    ==================

    .. this page is created in docs/Tools/dirac-docs-build-commands.py


    This page is the work in progress. See more material here soon !

    .. toctree::
       :maxdepth: 1

    """).lstrip()

    for mT in self.commands_markers_sections_scripts:
      system = mT[TITLE]
      existingIndex = mT[EXISTING_INDEX]
      if existingIndex:
        continue
      systemString = system.replace(' ', '')
      userIndexRST += '   %s/index\n' % systemString

      LOG.debug('Index file:\n%s', userIndexRST) if self.debug else None
      sectionPath = os.path.join(self.config.packagePath, mT[SECTION_PATH])
      mkdir(sectionPath)
      self.createSectionIndex(mT, sectionPath)

    userIndexPath = os.path.join(self.config.packagePath, self.config.com_index_file)
    writeLinesToFile(userIndexPath, userIndexRST)

  def createCommandReferenceForExistingIndex(self, mT):
    """Create the command reference for the AdministratorGuide.

    source/AdministratorGuide/CommandReference
    """

    sectionPath = os.path.join(self.config.packagePath, mT[SECTION_PATH])
    LOG.info('Creating references for %r', sectionPath)
    # read the script index
    with open(os.path.join(sectionPath, 'index.rst')) as indexFile:
      commandList = indexFile.read().replace('\n', '')

    missingCommands = []
    for script in mT[SCRIPTS]:
      scriptName = os.path.basename(script)
      if scriptName.endswith('.py'):
        scriptName = scriptName[:-3]
      refPre = mT[TITLE].replace(' ', '').lower() + '_'
      if self.createScriptDocFiles(script, sectionPath, scriptName, referencePrefix=refPre) and \
         scriptName not in commandList:
        missingCommands.append(scriptName)

    if missingCommands:
      LOG.error("The following commands are not in the command index: \n\t\t\t\t%s",
                "\n\t\t\t\t".join(missingCommands))
      self.exitcode = 1

  def cleanExistingIndex(self, mT):
    """Make sure no superfluous commands are documented in an existing index file"""
    if not mT[EXISTING_INDEX]:
      return
    existingCommands = {os.path.basename(com).replace('.py', '') for com in mT[SCRIPTS] + mT[IGNORE]}
    sectionPath = os.path.join(self.config.packagePath, mT[SECTION_PATH])
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
        shutil.move(os.path.join(sectionPath, com + '.rst'), os.path.join(sectionPath, 'obs_' + com + '.rst'))
      self.exitcode = 1

  def createSectionIndex(self, mT, sectionPath):
    """ create the index """

    systemName = mT[TITLE]
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
    listOfScripts.extend(mT[IGNORE])

    for script in mT[SCRIPTS]:
      scriptName = os.path.basename(script)
      if scriptName.endswith('.py'):
        scriptName = scriptName[:-3]
      if self.createScriptDocFiles(script, sectionPath, scriptName):
        listOfScripts.append(scriptName)

    for scriptName in sorted(listOfScripts):
      sectionIndexRST += "   %s\n" % scriptName

    sectionIndexPath = os.path.join(sectionPath, 'index.rst')
    writeLinesToFile(sectionIndexPath, sectionIndexRST)

  def createScriptDocFiles(self, script, sectionPath, scriptName, referencePrefix=''):
    """Create the RST files for all the scripts.

    Folders and indices already exist, just call the scripts and get the help messages. Format the help message.

    """
    if scriptName in self.config.com_ignore_commands:
      return False

    LOG.info('Creating Doc for %r', scriptName)
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


def run(arguments=sys.argv):
  """Create the rst files right in the source tree of the docs."""
  debug = False
  if '-ddd' in ''.join(arguments):
    LOG.setLevel(logging.DEBUG)
    debug = True
  if '-dd' in ''.join(arguments):
    LOG.setLevel(logging.DEBUG)
    debug = False
  LOG.setLevel(logging.DEBUG)
  C = CommandReference(debug=debug)
  C.getScripts()
  C.createUserGuideFoldersAndIndices()
  for mT in C.commands_markers_sections_scripts:
    if not mT[EXISTING_INDEX]:
      continue
    C.createCommandReferenceForExistingIndex(mT)
    C.cleanExistingIndex(mT)

  LOG.info('Done')
  return C.exitcode


if __name__ == "__main__":
  exit(run())
