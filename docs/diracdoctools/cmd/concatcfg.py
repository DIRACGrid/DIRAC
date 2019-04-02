#!/bin/env python
"""script to concatenate the dirac.cfg file's Systems sections with the content of the ConfigTemplate.cfg files."""

from collections import OrderedDict
import os
import textwrap
import re

from DIRAC.Core.Utilities.CFG import CFG
from DIRAC import gLogger, S_OK, S_ERROR

from diracdoctools.Utilities import packagePath


def updateCompleteDiracCFG():
  """Read the dirac.cfg and update the Systems sections from the ConfigTemplate.cfg files."""
  compCfg = CFG()
  mainDiracCfgPath = os.path.join(packagePath(), 'dirac.cfg')

  if not os.path.exists(mainDiracCfgPath):
    raise RuntimeError("Dirac.cfg not found at %s" % mainDiracCfgPath)

  gLogger.notice('Extracting default configuration from', mainDiracCfgPath)
  loadCFG = CFG()
  loadCFG.loadFromFile(mainDiracCfgPath)
  compCfg = loadCFG.mergeWith(compCfg)

  cfg = getSystemsCFG()
  compCfg = compCfg.mergeWith(cfg)
  diracCfgOutput = os.path.join(packagePath(), 'docs/source/AdministratorGuide/Configuration/ExampleConfig.rst')

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
    htmlMatch = re.compile(r"\.html(?=[a-zA-Z0-9])")
    cfgString = re.sub(htmlMatch, '.html#', cfgString)
    rst.write(cfgString)


def getSystemsCFG():
  """Find all the ConfigTemplates and collate them into one CFG object."""
  cfg = CFG()
  cfg.createNewSection('/Systems')
  templateLocations = findConfigTemplates()
  for templatePath in templateLocations:
    cfgRes = parseConfigTemplate(templatePath, cfg)
    if cfgRes['OK']:
      cfg = cfgRes['Value']
  return cfg


def findConfigTemplates():
  """Traverse folders in DIRAC and find ConfigTemplate.cfg files."""
  configTemplates = dict()
  for baseDirectory, _subdirectories, files in os.walk(packagePath()):
    gLogger.debug('Looking in %r' % baseDirectory)
    if 'ConfigTemplate.cfg' in files:
      system = baseDirectory.rsplit('/', 1)[1]
      gLogger.notice('Found Template for %s in %s' % (system, baseDirectory))
      configTemplates[system] = baseDirectory
  return OrderedDict(sorted(configTemplates.items(), key=lambda t: t[0])).values()


def parseConfigTemplate(templatePath, cfg):
  """Parse the ConfigTemplate.cfg files.

  :param str templatePath: path to the folder containing a ConfigTemplate.cfg file
  :param CFG cfg: cfg to merge with the systems config
  :returns: CFG object
  """
  system = os.path.split(templatePath.rstrip("/"))[1]
  if system.lower().endswith('system'):
    system = system[:-len('System')]

  templatePath = os.path.join(templatePath, 'ConfigTemplate.cfg')
  if not os.path.exists(templatePath):
    return S_ERROR("File not found: %s" % templatePath)

  loadCfg = CFG()
  loadCfg.loadFromFile(templatePath)

  cfg.createNewSection("/Systems/%s" % system, contents=loadCfg)

  return S_OK(cfg)


def run():
  """Wrapper around main working horse."""
  updateCompleteDiracCFG()


if __name__ == '__main__':
  run()
