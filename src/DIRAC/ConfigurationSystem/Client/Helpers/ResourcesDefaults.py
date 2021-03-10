########################################################################
# File :   ResourcesDefaults.py
# Author : Ricardo Graciani
########################################################################
"""
Some Helper class to access Default options for Different Resources (CEs, SEs, Catalags,...)
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
from diraccfg import CFG
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgResourceSection, cfgPath, cfgInstallPath, cfgPathToList

__RCSID__ = "$Id$"


def defaultSection(resource):
  """
  Build the path for the Defaults section
  """
  return cfgPath(cfgResourceSection, 'Defaults', resource)


def getComputingElementDefaults(ceName='', ceType='', cfg=None, currentSectionPath=''):
  """
  Return cfgDefaults with defaults for the given CEs defined either in arguments or in the provided cfg
  """
  cesCfg = CFG()
  if cfg:
    try:
      cesCfg.loadFromFile(cfg)
      cesPath = cfgInstallPath('ComputingElements')
      if cesCfg.isSection(cesPath):
        for section in cfgPathToList(cesPath):
          cesCfg = cesCfg[section]
    except Exception:
      return CFG()

  # Overwrite the cfg with Command line arguments
  if ceName:
    if not cesCfg.isSection(ceName):
      cesCfg.createNewSection(ceName)
    if currentSectionPath:
      # Add Options from Command Line
      optionsDict = __getExtraOptions(currentSectionPath)
      for name, value in optionsDict.items():
        cesCfg[ceName].setOption(name, value)  # pylint: disable=no-member
    if ceType:
      cesCfg[ceName].setOption('CEType', ceType)  # pylint: disable=no-member

  ceDefaultSection = cfgPath(defaultSection('ComputingElements'))
  # Load Default for the given type from Central configuration is defined
  ceDefaults = __gConfigDefaults(ceDefaultSection)
  for ceName in cesCfg.listSections():
    if 'CEType' in cesCfg[ceName]:
      ceType = cesCfg[ceName]['CEType']
      if ceType in ceDefaults:
        for option in ceDefaults[ceType].listOptions():  # pylint: disable=no-member
          if option not in cesCfg[ceName]:
            cesCfg[ceName].setOption(option, ceDefaults[ceType][option])  # pylint: disable=unsubscriptable-object

  return cesCfg


def __gConfigDefaults(defaultPath):
  """
  Build a cfg from a Default Section
  """
  from DIRAC import gConfig
  cfgDefaults = CFG()
  result = gConfig.getSections(defaultPath)
  if not result['OK']:
    return cfgDefaults
  for name in result['Value']:
    typePath = cfgPath(defaultPath, name)
    cfgDefaults.createNewSection(name)
    result = gConfig.getOptionsDict(typePath)
    if result['OK']:
      optionsDict = result['Value']
      for option, value in optionsDict.items():
        cfgDefaults[name].setOption(option, value)

  return cfgDefaults


def __getExtraOptions(currentSectionPath):
  from DIRAC import gConfig
  optionsDict = {}
  if not currentSectionPath:
    return optionsDict
  result = gConfig.getOptionsDict(currentSectionPath)
  if not result['OK']:
    return optionsDict
  print(result)
  return result['Value']
