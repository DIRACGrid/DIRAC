#!/bin/env python
"""script to update the complete dirac.cfg file's Systems sections with the content of the ConfigTemplate.cfg."""

import os

from DIRAC.Core.Utilities.CFG import CFG
from DIRAC import gLogger, S_OK, S_ERROR


def updateCompleteDiracCFG():
  """Read the dirac.cfg and update the Systems sections from the ConfigTemplate.cfg files."""
  compCfg = CFG()
  rootpath = os.environ['DIRAC']
  mainDiracCfgPath = os.path.join(rootpath, 'DIRAC', 'dirac.cfg')

  if not os.path.exists(mainDiracCfgPath):
    raise RuntimeError("Dirac.cfg not found at %s" % mainDiracCfgPath)

  gLogger.notice('Extracting default configuration from', mainDiracCfgPath)
  loadCFG = CFG()
  loadCFG.loadFromFile(mainDiracCfgPath)
  compCfg = loadCFG.mergeWith(compCfg)

  cfg = getSystemsCFG()
  compCfg = compCfg.mergeWith(cfg)
  compCfg.writeToFile(mainDiracCfgPath)


def getSystemsCFG():
  """Find all the ConfigTemplates and collate them into one CFG object."""
  cfg = CFG()
  cfg.createNewSection("/Systems")
  templateLocations = findConfigTemplates()
  for templatePath in templateLocations:
    cfgRes = parseConfigTemplate(templatePath, cfg)
    if cfgRes['OK']:
      cfg = cfgRes['Value']
  return cfg


def findConfigTemplates():
  """Traverse folders in DIRAC and find ConfigTemplate.cfg files."""
  configTemplates = set()
  diracPath = os.environ.get("DIRAC") + "/DIRAC"
  for baseDirectory, _subdirectories, files in os.walk(diracPath):
    if 'docs' in baseDirectory:
      continue
    if 'ConfigTemplate.cfg' in files:
      configTemplates.add(baseDirectory)
  return sorted(configTemplates)


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


if __name__ == "__main__":
  updateCompleteDiracCFG()
