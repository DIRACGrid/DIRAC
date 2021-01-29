#!/usr/bin/env python
"""script to concatenate the dirac.cfg file's Systems sections with the content of the ConfigTemplate.cfg files."""

from collections import OrderedDict
import logging
import os
import re
import textwrap
import sys
import shutil

from diracdoctools.Config import Configuration
from diracdoctools.Utilities import makeLogger

# Try/except for python3 compatibility to ignore errors in ``import DIRAC`` while they last
# ultimate protection against not having the symbols imported is also done in the ``run`` function
try:
  from DIRAC.Core.Utilities.CFG import CFG
  from DIRAC import S_OK, S_ERROR
except ImportError:
  pass


LOG = makeLogger('ConcatCFG')


class ConcatCFG(object):

  def __init__(self, configFile='docs.conf'):
    self.config = Configuration(configFile, sections=['CFG'])
    self.retVal = 0

  def prepareDiracCFG(self):
    """Copy dirac.cfg file to source dir."""
    LOG.info('Copy %r to source directory', self.config.cfg_baseFile)
    shutil.copy(self.config.cfg_baseFile, '/'.join([self.config.docsPath, 'source/']))

  def updateCompleteDiracCFG(self):
    """Read the dirac.cfg and update the Systems sections from the ConfigTemplate.cfg files."""
    compCfg = CFG()
    mainDiracCfgPath = self.config.cfg_baseFile

    if not os.path.exists(mainDiracCfgPath):
      LOG.error('Failed to find Main Dirac cfg at %r', mainDiracCfgPath)
      return 1

    self.prepareDiracCFG()

    LOG.info('Extracting default configuration from %r', mainDiracCfgPath)
    loadCFG = CFG()
    loadCFG.loadFromFile(mainDiracCfgPath)
    compCfg = loadCFG.mergeWith(compCfg)

    cfg = self.getSystemsCFG()
    compCfg = compCfg.mergeWith(cfg)
    diracCfgOutput = self.config.cfg_targetFile

    LOG.info('Writing output to %r', diracCfgOutput)

    with open(diracCfgOutput, 'w') as rst:
      rst.write(textwrap.dedent("""
                                ==========================
                                Full Configuration Example
                                ==========================

                                .. This file is created by docs/Tools/UpdateDiracCFG.py

                                Below is a complete example configuration with anotations for some sections::

                                """))
      # indent the cfg text
      cfgString = ''.join('  ' + line for line in str(compCfg).splitlines(True))
      # fix the links, add back the # for targets
      # match .html with following character using positive look ahead
      htmlMatch = re.compile(r'\.html(?=[a-zA-Z0-9])')
      cfgString = re.sub(htmlMatch, '.html#', cfgString)
      rst.write(cfgString)
    return self.retVal

  def getSystemsCFG(self):
    """Find all the ConfigTemplates and collate them into one CFG object."""
    cfg = CFG()
    cfg.createNewSection('/Systems')
    templateLocations = self.findConfigTemplates()
    for templatePath in templateLocations:
      cfgRes = self.parseConfigTemplate(templatePath, cfg)
      if cfgRes['OK']:
        cfg = cfgRes['Value']
    return cfg

  def findConfigTemplates(self):
    """Traverse folders in DIRAC and find ConfigTemplate.cfg files."""
    configTemplates = dict()
    for baseDirectory, _subdirectories, files in os.walk(self.config.sourcePath):
      LOG.debug('Looking in %r', baseDirectory)
      if 'ConfigTemplate.cfg' in files:
        system = baseDirectory.rsplit('/', 1)[1]
        LOG.info('Found Template for %r in %r', system, baseDirectory)
        configTemplates[system] = baseDirectory
    return OrderedDict(sorted(configTemplates.items(), key=lambda t: t[0])).values()

  def parseConfigTemplate(self, templatePath, cfg):
    """Parse the ConfigTemplate.cfg files.

    :param str templatePath: path to the folder containing a ConfigTemplate.cfg file
    :param CFG cfg: cfg to merge with the systems config
    :returns: CFG object
    """
    system = os.path.split(templatePath.rstrip('/'))[1]
    if system.lower().endswith('system'):
      system = system[:-len('System')]

    templatePath = os.path.join(templatePath, 'ConfigTemplate.cfg')
    if not os.path.exists(templatePath):
      return S_ERROR('File not found: %s' % templatePath)

    loadCfg = CFG()
    try:
      loadCfg.loadFromFile(templatePath)
    except ValueError as err:
      LOG.error('Failed loading file %r: %r', templatePath, err)
      self.retVal = 1
      return S_ERROR()
    cfg.createNewSection('/Systems/%s' % system, contents=loadCfg)

    return S_OK(cfg)


def run(configFile='docs.conf', logLevel=logging.INFO, debug=False):
  """Add sections from System/ConfigTemplates to main dirac.cfg file

  :param str configFile: path to the configFile
  :param logLevel: logging level to use
  :param bool debug: unused
  :returns: return value 1 or 0
  """
  try:
    logging.getLogger().setLevel(logLevel)
    concat = ConcatCFG(configFile=configFile)
    return concat.updateCompleteDiracCFG()
  except (ImportError, NameError):
    return 1

if __name__ == '__main__':
  sys.exit(run())
