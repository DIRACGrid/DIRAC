"""
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from diraccfg import CFG

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.JDL import loadJDLAsCFG, dumpCFGAsJDL


class JobManifest(object):

  def __init__(self, manifest=""):
    self.__manifest = CFG()
    self.__dirty = False
    self.__ops = False
    if manifest:
      result = self.load(manifest)
      if not result['OK']:
        raise Exception(result['Message'])

  def isDirty(self):
    return self.__dirty

  def setDirty(self):
    self.__dirty = True

  def clearDirty(self):
    self.__dirty = False

  def load(self, dataString):
    """
    Auto discover format type based on [ .. ] of JDL
    """
    dataString = dataString.strip()
    if dataString[0] == "[" and dataString[-1] == "]":
      return self.loadJDL(dataString)
    else:
      return self.loadCFG(dataString)

  def loadJDL(self, jdlString):
    """
    Load job manifest from JDL format
    """
    result = loadJDLAsCFG(jdlString.strip())
    if not result['OK']:
      self.__manifest = CFG()
      return result
    self.__manifest = result['Value'][0]
    return S_OK()

  def loadCFG(self, cfgString):
    """
    Load job manifest from CFG format
    """
    try:
      self.__manifest.loadFromBuffer(cfgString)
    except Exception as e:
      return S_ERROR("Can't load manifest from cfg: %s" % str(e))
    return S_OK()

  def dumpAsCFG(self):
    return str(self.__manifest)

  def getAsCFG(self):
    return self.__manifest.clone()

  def dumpAsJDL(self):
    return dumpCFGAsJDL(self.__manifest)

  def __getCSValue(self, varName, defaultVal=None):
    if not self.__ops:
      self.__ops = Operations(group=self.__manifest['OwnerGroup'], setup=self.__manifest['DIRACSetup'])
    if varName[0] != "/":
      varName = "JobDescription/%s" % varName
    return self.__ops.getValue(varName, defaultVal)

  def __checkNumericalVar(self, varName, defaultVal, minVal, maxVal):
    """
    Check a numerical var
    """
    initialVal = False
    if varName not in self.__manifest:
      varValue = self.__getCSValue("Default%s" % varName, defaultVal)
    else:
      varValue = self.__manifest[varName]
      initialVal = varValue
    try:
      varValue = int(varValue)
    except ValueError:
      return S_ERROR("%s must be a number" % varName)
    minVal = self.__getCSValue("Min%s" % varName, minVal)
    maxVal = self.__getCSValue("Max%s" % varName, maxVal)
    varValue = max(minVal, min(varValue, maxVal))
    if initialVal != varValue:
      self.__manifest.setOption(varName, varValue)
    return S_OK(varValue)

  def __checkChoiceVar(self, varName, defaultVal, choices):
    """
    Check a choice var
    """
    initialVal = False
    if varName not in self.__manifest:
      varValue = self.__getCSValue("Default%s" % varName, defaultVal)
    else:
      varValue = self.__manifest[varName]
      initialVal = varValue
    if varValue not in self.__getCSValue("Choices%s" % varName, choices):
      return S_ERROR("%s is not a valid value for %s" % (varValue, varName))
    if initialVal != varValue:
      self.__manifest.setOption(varName, varValue)
    return S_OK(varValue)

  def __checkMultiChoice(self, varName, choices):
    """
    Check a multi choice var
    """
    initialVal = False
    if varName not in self.__manifest:
      return S_OK()
    else:
      varValue = self.__manifest[varName]
      initialVal = varValue
    choices = self.__getCSValue("Choices%s" % varName, choices)
    for v in List.fromChar(varValue):
      if v not in choices:
        return S_ERROR("%s is not a valid value for %s" % (v, varName))
    if initialVal != varValue:
      self.__manifest.setOption(varName, varValue)
    return S_OK(varValue)

  def __checkMaxInputData(self, maxNumber):
    """
    Check Maximum Number of Input Data files allowed
    """
    varName = "InputData"
    if varName not in self.__manifest:
      return S_OK()
    varValue = self.__manifest[varName]
    if len(List.fromChar(varValue)) > maxNumber:
      return S_ERROR('Number of Input Data Files (%s) greater than current limit: %s' %
                     (len(List.fromChar(varValue)), maxNumber))
    return S_OK()

  def __contains__(self, key):
    """ Check if the manifest has the required key
    """
    return key in self.__manifest

  def setOptionsFromDict(self, varDict):
    for k in sorted(varDict):
      self.setOption(k, varDict[k])

  def check(self):
    """
    Check that the manifest is OK
    """
    for k in ['OwnerName', 'OwnerDN', 'OwnerGroup', 'DIRACSetup']:
      if k not in self.__manifest:
        return S_ERROR("Missing var %s in manifest" % k)

    # Check CPUTime
    result = self.__checkNumericalVar("CPUTime", 86400, 100, 500000)
    if not result['OK']:
      return result

    result = self.__checkNumericalVar("Priority", 1, 0, 10)
    if not result['OK']:
      return result

    maxInputData = Operations().getValue("JobDescription/MaxInputData", 500)
    result = self.__checkMaxInputData(maxInputData)
    if not result['OK']:
      return result

    operation = Operations(group=self.__manifest['OwnerGroup'])
    allowedJobTypes = operation.getValue("JobDescription/AllowedJobTypes", ['User', 'Test', 'Hospital'])
    transformationTypes = operation.getValue("Transformations/DataProcessing", [])
    result = self.__checkMultiChoice("JobType", allowedJobTypes + transformationTypes)
    if not result['OK']:
      return result
    return S_OK()

  def createSection(self, secName, contents=False):
    if secName not in self.__manifest:
      if contents and not isinstance(contents, CFG):
        return S_ERROR("Contents for section %s is not a cfg object" % secName)
      self.__dirty = True
      return S_OK(self.__manifest.createNewSection(secName, contents=contents))
    return S_ERROR("Section %s already exists" % secName)

  def getSection(self, secName):
    self.__dirty = True
    if secName not in self.__manifest:
      return S_ERROR("%s does not exist" % secName)
    sec = self.__manifest[secName]
    if not sec:
      return S_ERROR("%s section empty" % secName)
    return S_OK(sec)

  def setSectionContents(self, secName, contents):
    if contents and not isinstance(contents, CFG):
      return S_ERROR("Contents for section %s is not a cfg object" % secName)
    self.__dirty = True
    if secName in self.__manifest:
      self.__manifest[secName].reset()
      self.__manifest[secName].mergeWith(contents)
    else:
      self.__manifest.createNewSection(secName, contents=contents)

  def setOption(self, varName, varValue):
    """
    Set a var in job manifest
    """
    self.__dirty = True
    levels = List.fromChar(varName, "/")
    cfg = self.__manifest
    for l in levels[:-1]:
      if l not in cfg:
        cfg.createNewSection(l)
      cfg = cfg[l]
    cfg.setOption(levels[-1], varValue)

  def remove(self, opName):
    levels = List.fromChar(opName, "/")
    cfg = self.__manifest
    for l in levels[:-1]:
      if l not in cfg:
        return S_ERROR("%s does not exist" % opName)
      cfg = cfg[l]
    if cfg.deleteKey(levels[-1]):
      self.__dirty = True
      return S_OK()
    return S_ERROR("%s does not exist" % opName)

  def getOption(self, varName, defaultValue=None):
    """
     Get a variable from the job manifest
    """
    cfg = self.__manifest
    return cfg.getOption(varName, defaultValue)

  def getOptionList(self, section=""):
    """
    Get a list of variables in a section of the job manifest
    """
    cfg = self.__manifest.getRecursive(section)
    if not cfg or 'value' not in cfg:
      return []
    cfg = cfg['value']
    return cfg.listOptions()

  def isOption(self, opName):
    """
    Check if it is a valid option
    """
    return self.__manifest.isOption(opName)

  def getSectionList(self, section=""):
    """
    Get a list of sections in the job manifest
    """
    cfg = self.__manifest.getRecursive(section)
    if not cfg or 'value' not in cfg:
      return []
    cfg = cfg['value']
    return cfg.listSections()
