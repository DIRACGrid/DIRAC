"""Configuration for the documentation scripts."""

import argparse
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

    self.com_MSS = []

    for section in sorted(config.sections()):
      LOG.info('Parsing config sections: %r', section)
      if section.startswith('commands.'):
        pattern = listify(config.get(section, 'pattern'))
        title = config.get(section, 'title')
        scripts = listify(config.get(section, 'scripts'))
        ignore = listify(config.get(section, 'ignore'))
        sectionPath = config.get(section, 'sectionpath').replace(' ', '')
        indexFile = self._fullPath(config.get(section, 'indexfile')) if \
            config.has_option(section, 'indexfile') else None

        self.com_MSS.append(dict(pattern=pattern,
                                 title=title,
                                 scripts=scripts,
                                 ignore=ignore,
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


class CLParser(object):

  def __init__(self):
    self.log = LOG.getChild('CLParser')
    self.parsed = None
    self.debug = False
    self.parser = argparse.ArgumentParser("DiracDocTool",
                                          formatter_class=argparse.RawTextHelpFormatter)

    self.parser.add_argument('--configFile', action='store', default='docs.conf',
                             dest='configFile',
                             help='Name of the config file')

    self.parser.add_argument('-d', '--debug', action='count', dest='debug', help='d, dd, ddd',
                             default=0)

  def parse(self):
    self.log.info('Parsing common options')
    self.parsed = self.parser.parse_args()
    self.logLevel = self._parsePrintLevel(self.parsed.debug)
    self.configFile = self.parsed.configFile

  def optionDict(self):
    """Return dictionary of options."""
    if not self.parsed:
      self.parse()
    return (dict(configFile=self.configFile,
                 logLevel=self.logLevel,
                 debug=self.debug,
                 ))

  def _parsePrintLevel(self, level):
    """Translate debug count to logging level."""
    level = level if level <= 2 else 2
    self.debug = level == 2
    return [logging.INFO,
            logging.INFO,
            logging.DEBUG,
            ][level]
