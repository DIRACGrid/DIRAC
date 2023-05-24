from diraccfg import CFG

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.JDL import loadJDLAsCFG, dumpCFGAsJDL


class JobManifest:
    def __init__(self, manifest=""):
        self.__manifest = CFG()
        self.__dirty = False
        self.__ops = False
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

    def __contains__(self, key):
        """Check if the manifest has the required key"""
        return key in self.__manifest

    def setOptionsFromDict(self, varDict):
        for k in sorted(varDict):
            self.setOption(k, varDict[k])

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
