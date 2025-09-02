from __future__ import annotations

from typing import Literal, TypedDict

from diraccfg import CFG

from DIRACCommon.Core.Utilities import List
from DIRACCommon.Core.Utilities.JDL import dumpCFGAsJDL, loadJDLAsCFG
from DIRACCommon.Core.Utilities.ReturnValues import S_ERROR, S_OK


class JobManifestNumericalVar(TypedDict):
    CPUTime: int
    Priority: int


class JobManifestConfig(TypedDict):
    """Dictionary type for defining the information JobManifest needs from the CS"""

    defaultForGroup: JobManifestNumericalVar
    minForGroup: JobManifestNumericalVar
    maxForGroup: JobManifestNumericalVar
    allowedJobTypesForGroup: list[str]

    maxInputData: int


class JobManifest:
    def __init__(self, manifest=""):
        self.__manifest = CFG()
        self.__dirty = False
        if manifest:
            result = self.load(manifest)
            if not result["OK"]:
                raise Exception(result["Message"])

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
        if not result["OK"]:
            self.__manifest = CFG()
            return result
        self.__manifest = result["Value"][0]
        return S_OK()

    def loadCFG(self, cfgString):
        """
        Load job manifest from CFG format
        """
        try:
            self.__manifest.loadFromBuffer(cfgString)
        except Exception as e:
            return S_ERROR(f"Can't load manifest from cfg: {str(e)}")
        return S_OK()

    def dumpAsCFG(self):
        return str(self.__manifest)

    def getAsCFG(self):
        return self.__manifest.clone()

    def dumpAsJDL(self):
        return dumpCFGAsJDL(self.__manifest)

    def _checkNumericalVar(
        self,
        varName: Literal["CPUTime", "Priority"],
        defaultVal: int,
        minVal: int,
        maxVal: int,
        config: JobManifestConfig,
    ):
        """
        Check a numerical var
        """
        if varName in self.__manifest:
            varValue = self.__manifest[varName]
        else:
            varValue = config["defaultForGroup"].get(varName, defaultVal)
        try:
            varValue = int(varValue)
        except ValueError:
            return S_ERROR(f"{varName} must be a number")
        minVal = config["minForGroup"][varName]
        maxVal = config["maxForGroup"][varName]
        varValue = max(minVal, min(varValue, maxVal))
        if varName not in self.__manifest:
            self.__manifest.setOption(varName, varValue)
        return S_OK(varValue)

    def __contains__(self, key):
        """Check if the manifest has the required key"""
        return key in self.__manifest

    def setOptionsFromDict(self, varDict):
        for k in sorted(varDict):
            self.setOption(k, varDict[k])

    def check(self, *, config: JobManifestConfig):
        """
        Check that the manifest is OK
        """
        for k in ["Owner", "OwnerGroup"]:
            if k not in self.__manifest:
                return S_ERROR(f"Missing var {k} in manifest")

        # Check CPUTime
        result = self._checkNumericalVar("CPUTime", 86400, 100, 500000, config=config)
        if not result["OK"]:
            return result

        result = self._checkNumericalVar("Priority", 1, 0, 10, config=config)
        if not result["OK"]:
            return result

        if "InputData" in self.__manifest:
            nInput = len(List.fromChar(self.__manifest["InputData"]))
            if nInput > config["maxInputData"]:
                return S_ERROR(
                    f"Number of Input Data Files ({nInput}) greater than current limit: {config['maxInputData']}"
                )

        if "JobType" in self.__manifest:
            varValue = self.__manifest["JobType"]
            for v in List.fromChar(varValue):
                if v not in config["allowedJobTypesForGroup"]:
                    return S_ERROR(f"{v} is not a valid value for JobType")

        return S_OK()

    def createSection(self, secName, contents=False):
        if secName not in self.__manifest:
            if contents and not isinstance(contents, CFG):
                return S_ERROR(f"Contents for section {secName} is not a cfg object")
            self.__dirty = True
            return S_OK(self.__manifest.createNewSection(secName, contents=contents))
        return S_ERROR(f"Section {secName} already exists")

    def getSection(self, secName):
        self.__dirty = True
        if secName not in self.__manifest:
            return S_ERROR(f"{secName} does not exist")
        sec = self.__manifest[secName]
        if not sec:
            return S_ERROR(f"{secName} section empty")
        return S_OK(sec)

    def setSectionContents(self, secName, contents):
        if contents and not isinstance(contents, CFG):
            return S_ERROR(f"Contents for section {secName} is not a cfg object")
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
                return S_ERROR(f"{opName} does not exist")
            cfg = cfg[l]
        if cfg.deleteKey(levels[-1]):
            self.__dirty = True
            return S_OK()
        return S_ERROR(f"{opName} does not exist")

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
        if not cfg or "value" not in cfg:
            return []
        cfg = cfg["value"]
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
        if not cfg or "value" not in cfg:
            return []
        cfg = cfg["value"]
        return cfg.listSections()
