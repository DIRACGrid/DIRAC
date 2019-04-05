"""Configuration for the documentation scripts."""

import logging
import os
import ConfigParser

LOG = logging.getLogger(__name__)


def listify(values):
  return [entry.strip() for entry in values.split(',')]


class Configuration(object):
  """Provide configuraiton to the scripts."""

  def __init__(self, confFile):
    LOG.info('Reading configFile %r', confFile)

    config = ConfigParser.SafeConfigParser(dict_type=dict)
    config.read(confFile)
    config.optionxform = str  # do not transform options to lowercase

    self.moduleName = config.get('Docs', 'module_name')
    self.packagePath = os.path.join(os.environ.get('DIRAC', ''), self.moduleName)
    self.codeTargetPath = os.path.join(self.packagePath, config.get('Code', 'docs_target_path'))
    self.customDocsPath = config.get('Code', 'customdocs_folder')
    self.privateMembers = listify(config.get('Code', 'document_private_members'))
    self.noInherited = listify(config.get('Code', 'no_inherited_members'))
    self.badFiles = listify(config.get('Code', 'ignore_files'))
    relativeSourceFolder = config.get('Code', 'source_folder')
    self.sourcePath = os.path.join(self.packagePath, relativeSourceFolder)
    for var, val in vars(self).items():
       LOG.info('Parsed options: %s = %s', var, val)

  def __str__(self):
    """Return string containg options and values."""
    theStr = ''
    for var, val in vars(self).items():
      theStr += '%s = %s\n' % (var, val)

    return theStr
