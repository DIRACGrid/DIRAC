"""Configuration for the documentation scripts."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from __future__ import absolute_import

import argparse
import logging
import os
from pprint import pformat
import glob

try:
    from configparser import ConfigParser  # python3
except ImportError:
    from ConfigParser import SafeConfigParser as ConfigParser  # python2


LOG = logging.getLogger(__name__)


K_CODE = "Code"
K_COMMANDS = "Commands"
K_CFG = "CFG"


def listify(values):
    return [entry.strip() for entry in values.split(",") if entry]


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
      self.code_add_commands_section = None
      if config.has_option(K_CODE, 'add_commands_section'):
        self.code_add_commands_section = config.get(K_CODE, 'add_commands_section')

    if K_COMMANDS in sections:
      fileName = config.get(K_COMMANDS, 'fileName') if config.has_option(K_COMMANDS, 'fileName') else ''
      self.com_rst_path = os.path.join(config.get(K_COMMANDS, 'sectionpath'), fileName or 'index.rst')
      self.com_ignore_commands = listify(config.get(K_COMMANDS, 'ignore_commands'))

      # List all scripts paths
      self.allScripts = glob.glob(os.path.join(self.sourcePath, '*', 'scripts', '[!_]*.py'))
      self.allScripts += glob.glob(os.path.join(self.sourcePath, '*', 'scripts', '[!_]*.sh'))
      self.allScripts.sort()

      self.scripts = {}  # Sorted by group/subgroup

      for section in [s for s in sorted(config.sections()) if s.startswith('commands.')]:
        sp = section.split('.')
        if len(sp) == 2:
          group = sp[-1]
          subgroup = None
        else:
          group = sp[-2]
          subgroup = sp[-1]

        LOG.info('Parsing config section: %r', section)

        title = config.get(section, 'title')
        pattern = listify(config.get(section, 'pattern'))
        manual = listify(config.get(section, 'manual')) if config.has_option(section, 'manual') else []
        self.com_ignore_commands.extend(manual)
        exclude = listify(config.get(section, 'exclude')) if config.has_option(section, 'exclude') else []
        prefix = config.get(section, 'prefix') if config.has_option(section, 'prefix') else ''

        # Search scripts for group/subgroup pattern
        _scripts = []
        for sPath in self.allScripts:
          path = sPath[len(self.sourcePath):].replace("_", "-")
          if any(p in path for p in pattern) and not any(p in path for p in exclude):
            _scripts.append(sPath)

        if not subgroup:
          # Path to RST file
          fileName = config.get(section, 'filename').strip() if config.has_option(section, 'filename') else ''
          sectionPath = self._fullPath(config.get(section, 'sectionpath').replace(' ', ''))
          # Collect scripts paths and metadata for group
          self.scripts[group] = dict(scripts=_scripts,
                                     title=title,
                                     manual=manual,
                                     prefix=prefix,
                                     rstPath=os.path.join(sectionPath, fileName or 'index.rst'),
                                     subgroups=[])
        else:
          # Collect scripts paths and metadata for subgroup
          self.scripts[group]['subgroups'].append(subgroup)
          # Sub group scripts is a subset of the group scripts
          subgroupScripts = [s for s in _scripts if s in self.scripts[group]['scripts']]
          self.scripts[group][subgroup] = dict(title=title,
                                               manual=manual,
                                               prefix=prefix,
                                               scripts=subgroupScripts)
          # Remove subgroup scripts from group
          self.scripts[group]['scripts'] = [s for s in self.scripts[group]['scripts'] if s not in _scripts]

    if K_CFG in sections:
      self.cfg_targetFile = self._fullPath(config.get(K_CFG, 'target_file'))
      self.cfg_baseFile = self._fullPath(config.get(K_CFG, 'base_file'))

    for var, val in sorted(vars(self).items()):
      LOG.info('Parsed options: %s = %s', var, pformat(val))

  def _fullPath(self, path):
    """Return absolute path based on docsPath."""
    return os.path.abspath(os.path.join(self.docsPath, path))

  def __str__(self):
    """Return string containing options and values."""
    theStr = ''
    for var, val in vars(self).items():
      theStr += '%s = %s\n' % (var, val)

    return theStr


class CLParser(object):
    def __init__(self):
        self.log = LOG.getChild("CLParser")
        self.parsed = None
        self.debug = False
        self.parser = argparse.ArgumentParser("DiracDocTool", formatter_class=argparse.RawTextHelpFormatter)

        self.parser.add_argument(
            "--configFile", action="store", default="docs.conf", dest="configFile", help="Name of the config file"
        )

        self.parser.add_argument("-d", "--debug", action="count", dest="debug", help="d, dd, ddd", default=0)

    def parse(self):
        self.log.info("Parsing common options")
        self.parsed = self.parser.parse_args()
        self.logLevel = self._parsePrintLevel(self.parsed.debug)
        self.configFile = self.parsed.configFile

    def optionDict(self):
        """Return dictionary of options."""
        if not self.parsed:
            self.parse()
        return dict(
            configFile=self.configFile,
            logLevel=self.logLevel,
            debug=self.debug,
        )

    def _parsePrintLevel(self, level):
        """Translate debug count to logging level."""
        level = level if level <= 2 else 2
        self.debug = level == 2
        return [
            logging.INFO,
            logging.INFO,
            logging.DEBUG,
        ][level]
