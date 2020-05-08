"""Configuration for the documentation scripts."""

from __future__ import absolute_import

import argparse
import logging
import os
from pprint import pformat

try:
  from configparser import ConfigParser  # python3
except ImportError:
  from ConfigParser import SafeConfigParser as ConfigParser  # python2


LOG = logging.getLogger(__name__)


K_CODE = 'Code'
K_COMMANDS = 'Commands'
K_CFG = 'CFG'


def listify(values):
  return [entry.strip() for entry in values.split(',') if entry]


class Configuration(object):
  """Provide configuration to the scripts."""

  def __init__(self, confFile, sections=None):
    if sections is None:
      sections = [K_CODE, K_COMMANDS, K_CFG]
    LOG.info('Reading configFile %r', os.path.join(os.getcwd(), confFile))
    config = ConfigParser(dict_type=dict)
    config.read(confFile)
    # config.optionxform = str  # do not transform options to lowercase
    self.docsPath = os.path.dirname(os.path.abspath(confFile))

    relativeSourceFolder = config.get('Docs', 'source_folder')
    self.sourcePath = self._fullPath(relativeSourceFolder)

    self.moduleName = config.get('Docs', 'module_name')

    if K_CODE in sections:
      self.code_targetPath = self._fullPath(config.get(K_CODE, 'docs_target_path'))
      self.code_customDocsPath = self._fullPath(config.get(K_CODE, 'customdocs_folder'))
      self.code_privateMembers = listify(config.get(K_CODE, 'document_private_members'))
      self.code_noInherited = listify(config.get(K_CODE, 'no_inherited_members'))
      self.code_dummyFiles = listify(config.get(K_CODE, 'create_dummy_files'))
      self.code_ignoreFiles = listify(config.get(K_CODE, 'ignore_files'))
      self.code_ignoreFolders = listify(config.get(K_CODE, 'ignore_folders'))

    if K_COMMANDS in sections:
      self.com_ignore_commands = listify(config.get(K_COMMANDS, 'ignore_commands'))
      self.com_module_docstring = listify(config.get(K_COMMANDS, 'add_module_docstring'))

      self.com_MSS = []

      for section in sorted(config.sections()):
        if section.startswith('commands.'):
          LOG.info('Parsing config section: %r', section)
          pattern = listify(config.get(section, 'pattern'))
          title = config.get(section, 'title')
          manual = listify(config.get(section, 'manual'))
          self.com_ignore_commands.extend(manual)
          exclude = listify(config.get(section, 'exclude'))
          sectionPath = config.get(section, 'sectionpath').replace(' ', '')
          indexFile = self._fullPath(config.get(section, 'indexfile')) if \
              config.has_option(section, 'indexfile') else None
          prefix = config.get(section, 'prefix') if \
              config.has_option(section, 'prefix') else ''

          self.com_MSS.append(dict(pattern=pattern,
                                   title=title,
                                   scripts=[],
                                   exclude=exclude,
                                   manual=manual,
                                   indexFile=indexFile,
                                   prefix=prefix,
                                   sectionPath=sectionPath))

    if K_CFG in sections:
      self.cfg_targetFile = self._fullPath(config.get(K_CFG, 'target_file'))
      self.cfg_baseFile = self._fullPath(config.get(K_CFG, 'base_file'))

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
