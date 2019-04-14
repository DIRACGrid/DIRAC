"""Configuration for the documentation scripts."""

from collections import defaultdict
import logging
import os
from pprint import pformat
import ConfigParser


LOG = logging.getLogger(__name__)


def listify(values):
  return [entry.strip() for entry in values.split(',') if entry]


class Configuration(object):
  """Provide configuraiton to the scripts."""

  def __init__(self, confFile):
    LOG.info('Reading configFile %r', os.path.join(os.getcwd(), confFile))
    config = ConfigParser.SafeConfigParser(dict_type=dict)
    config.read(confFile)
    # config.optionxform = str  # do not transform options to lowercase
    self.docsPath = os.path.dirname(os.path.abspath(confFile))

    relativeSourceFolder = config.get('Docs', 'source_folder')
    self.sourcePath = self._fullPath(relativeSourceFolder)

    self.moduleName = config.get('Docs', 'module_name')
    self.codeTargetPath = self._fullPath(config.get('Code', 'docs_target_path'))
    self.customDocsPath = self._fullPath(config.get('Code', 'customdocs_folder'))

    self.privateMembers = listify(config.get('Code', 'document_private_members'))
    self.noInherited = listify(config.get('Code', 'no_inherited_members'))
    self.badFiles = listify(config.get('Code', 'ignore_files'))
    self.com_ignore_commands = listify(config.get('Commands', 'ignore_commands'))
    self.com_module_docstring = listify(config.get('Commands', 'module_docstring'))

    self.com_MSS = defaultdict(list)

    for section in sorted(config.sections()):
      LOG.info('Parsing config sections: %r', section)
      if section.startswith('commands.'):
        pattern = listify(config.get(section, 'pattern'))
        title = config.get(section, 'title')
        scripts = listify(config.get(section, 'scripts'))
        ignore = listify(config.get(section, 'ignore'))
        sectionPath = config.get(section, 'sectionpath').replace(' ', '')
        existingIndex = config.getboolean(
            section, 'existingindex') if config.has_option(
            section, 'existingindex') else False
        indexFile = config.get(section, 'indexfile')

        self.com_MSS[indexFile].append(dict(pattern=pattern,
                                            title=title,
                                            scripts=scripts,
                                            ignore=ignore,
                                            existingIndex=existingIndex,
                                            indexFile=indexFile,
                                            sectionPath=sectionPath))

    self.cfg_targetFile = self._fullPath(config.get('CFG', 'target_file'))
    self.cfg_baseFile = self._fullPath(config.get('CFG', 'base_file'))

    for var, val in sorted(vars(self).items()):
      LOG.info('Parsed options: %s = %s', var, pformat(val))

  def _fullPath(self, path):
    """Return absolut path based on docsPath."""
    return os.path.abspath(os.path.join(self.docsPath, path))

  def __str__(self):
    """Return string containg options and values."""
    theStr = ''
    for var, val in vars(self).items():
      theStr += '%s = %s\n' % (var, val)

    return theStr
