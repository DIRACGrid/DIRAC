""" This is the guy that actually modifies the content of the CS
"""
import zlib
import difflib
import datetime

from diraccfg import CFG
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.Security.ProxyInfo import getProxyInfo


class Modificator:
    def __init__(self, rpcClient=False, commiterId="unknown"):
        self.commiterTag = "@@-"
        self.commiterId = commiterId
        self.cfgData = CFG()
        self.rpcClient = None
        if rpcClient:
            self.setRPCClient(rpcClient)

    def loadCredentials(self):
        retVal = getProxyInfo()
        if retVal["OK"]:
            credDict = retVal["Value"]
            self.commiterId = "{}@{} - {}".format(
                credDict["username"],
                credDict["group"],
                datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            )
            return retVal
        return retVal

    def setRPCClient(self, rpcClient):
        self.rpcClient = rpcClient

    def loadFromRemote(self):
        retVal = self.rpcClient.getCompressedData()
        if retVal["OK"]:
            self.cfgData = CFG()
            data = retVal["Value"]
            if isinstance(data, str):
                data = data.encode(errors="surrogateescape")
            self.cfgData.loadFromBuffer(zlib.decompress(data).decode())
        return retVal

    def getCFG(self):
        return self.cfgData

    def getSections(self, sectionPath):
        return gConfigurationData.getSectionsFromCFG(sectionPath, self.cfgData)

    def getComment(self, sectionPath):
        return gConfigurationData.getCommentFromCFG(sectionPath, self.cfgData)

    def getOptions(self, sectionPath):
        return gConfigurationData.getOptionsFromCFG(sectionPath, self.cfgData)

    def getOptionsDict(self, sectionPath):
        """Gives the options of a CS section in a Python dict with values as
        lists"""

        opts = self.getOptions(sectionPath)
        pathDict = {o: self.getValue(f"{sectionPath}/{o}") for o in opts}
        return pathDict

    def getDictRootedAt(self, relpath="", root=""):
        """Gives the configuration rooted at path in a Python dict. The
        result is a Python dictionary that reflects the structure of the
        config file."""

        def getDictRootedAt(path):
            retval = {}
            opts = self.getOptionsDict(path)
            secs = self.getSections(path)
            for k in opts:
                retval[k] = opts[k]
            for i in secs:
                retval[i] = getDictRootedAt(path + "/" + i)
            return retval

        return getDictRootedAt(root + "/" + relpath)

    def getValue(self, optionPath):
        return gConfigurationData.extractOptionFromCFG(optionPath, self.cfgData)

    def sortAlphabetically(self, path, ascending=True):
        cfg = self.__getParentCFG(path, parentLevel=0)
        if cfg:
            if cfg.sortAlphabetically(ascending):
                self.__setCommiter(path)

    def __getParentCFG(self, path, parentLevel=1):
        sectionList = List.fromChar(path, "/")
        cfg = self.cfgData
        try:
            if parentLevel > 0:
                sectionList = sectionList[:-parentLevel]
            for section in sectionList:
                cfg = cfg[section]
            return cfg
        except Exception:
            return False

    def __setCommiter(self, entryPath, cfg=False):
        if not cfg:
            cfg = self.__getParentCFG(entryPath)
        entry = List.fromChar(entryPath, "/")[-1]
        comment = cfg.getComment(entry)
        filteredComment = [line.strip() for line in comment.split("\n") if line.find(self.commiterTag) != 0]
        filteredComment.append(f"{self.commiterTag}{self.commiterId}")
        cfg.setComment(entry, "\n".join(filteredComment))

    def setOptionValue(self, optionPath, value):
        levelList = [level.strip() for level in optionPath.split("/") if level.strip() != ""]
        parentPath = f"/{'/'.join(levelList[:-1])}"
        optionName = List.fromChar(optionPath, "/")[-1]
        self.createSection(parentPath)
        cfg = self.__getParentCFG(optionPath)
        if not cfg:
            return
        cfg.setOption(optionName, value)
        self.__setCommiter(optionPath, cfg)

    def createSection(self, sectionPath):
        levelList = [level.strip() for level in sectionPath.split("/") if level.strip() != ""]
        currentPath = ""
        cfg = self.cfgData
        createdSection = False
        for section in levelList:
            currentPath += f"/{section}"
            if section not in cfg.listSections():
                cfg.createNewSection(section)
                self.__setCommiter(currentPath)
                createdSection = True
            cfg = cfg[section]
        return createdSection

    def setComment(self, entryPath, value):
        cfg = self.__getParentCFG(entryPath)
        entry = List.fromChar(entryPath, "/")[-1]
        if cfg.setComment(entry, value):
            self.__setCommiter(entryPath)
            return True
        return False

    def existsSection(self, sectionPath):
        sectionList = List.fromChar(sectionPath, "/")
        cfg = self.cfgData
        try:
            for section in sectionList[:-1]:
                cfg = cfg[section]
            return len(sectionList) == 0 or sectionList[-1] in cfg.listSections()
        except Exception:
            return False

    def existsOption(self, optionPath):
        sectionList = List.fromChar(optionPath, "/")
        cfg = self.cfgData
        try:
            for section in sectionList[:-1]:
                cfg = cfg[section]
            return sectionList[-1] in cfg.listOptions()
        except Exception:
            return False

    def renameKey(self, path, newName):
        parentCfg = self.cfgData.getRecursive(path, -1)
        if not parentCfg:
            return False
        pathList = List.fromChar(path, "/")
        oldName = pathList[-1]
        if parentCfg["value"].renameKey(oldName, newName):
            pathList[-1] = newName
            self.__setCommiter(f"/{'/'.join(pathList)}")
            return True
        else:
            return False

    def copyKey(self, originalKeyPath, newKey):
        parentCfg = self.cfgData.getRecursive(originalKeyPath, -1)
        if not parentCfg:
            return False
        pathList = List.fromChar(originalKeyPath, "/")
        originalKey = pathList[-1]
        if parentCfg["value"].copyKey(originalKey, newKey):
            self.__setCommiter(f"/{'/'.join(pathList[:-1])}/{newKey}")
            return True
        return False

    def removeOption(self, optionPath):
        if not self.existsOption(optionPath):
            return False
        cfg = self.__getParentCFG(optionPath)
        optionName = List.fromChar(optionPath, "/")[-1]
        return cfg.deleteKey(optionName)

    def removeSection(self, sectionPath):
        if not self.existsSection(sectionPath):
            return False
        cfg = self.__getParentCFG(sectionPath)
        sectionName = List.fromChar(sectionPath, "/")[-1]
        return cfg.deleteKey(sectionName)

    def loadFromBuffer(self, data):
        self.cfgData = CFG()
        self.cfgData.loadFromBuffer(data)

    def loadFromFile(self, filename):
        self.cfgData = CFG()
        self.mergeFromFile(filename)

    def dumpToFile(self, filename):
        with open(filename, "w") as fd:
            fd.write(str(self.cfgData))

    def mergeFromFile(self, filename):
        cfg = CFG()
        cfg.loadFromFile(filename)
        self.cfgData = self.cfgData.mergeWith(cfg)

    def mergeFromCFG(self, cfg):
        self.cfgData = self.cfgData.mergeWith(cfg)

    def mergeSectionFromCFG(self, sectionPath, cfg):
        parentDict = self.cfgData.getRecursive(sectionPath, -1)
        parentCFG = parentDict["value"]
        secName = [lev.strip() for lev in sectionPath.split("/") if lev.strip()][-1]
        secCFG = parentCFG[secName]
        if not secCFG:
            return False
        mergedCFG = secCFG.mergeWith(cfg)
        parentCFG.deleteKey(secName)
        parentCFG.createNewSection(secName, parentDict["comment"], mergedCFG)
        self.__setCommiter(sectionPath)
        return True

    def __str__(self):
        return str(self.cfgData)

    def commit(self):
        compressedData = zlib.compress(str(self.cfgData).encode(), 9)
        return self.rpcClient.commitNewData(compressedData)

    def getHistory(self, limit=0):
        retVal = self.rpcClient.getCommitHistory(limit)
        if retVal["OK"]:
            return retVal["Value"]
        return []

    def showCurrentDiff(self):
        retVal = self.rpcClient.getCompressedData()
        if retVal["OK"]:
            data = retVal["Value"]
            if isinstance(data, str):
                data = data.encode(errors="surrogateescape")
            remoteData = zlib.decompress(data).decode().splitlines()
            localData = str(self.cfgData).splitlines()
            return difflib.ndiff(remoteData, localData)
        return []

    def getVersionDiff(self, fromDate, toDate):
        retVal = self.rpcClient.getVersionContents([fromDate, toDate])
        if retVal["OK"]:
            fromData = retVal["Value"][0]
            if isinstance(fromData, str):
                fromData = fromData.encode(errors="surrogateescape")
            fromData = zlib.decompress(fromData).decode()

            toData = retVal["Value"][1]
            if isinstance(toData, str):
                toData = toData.encode(errors="surrogateescape")
            toData = zlib.decompress(toData).decode()

            return difflib.ndiff(fromData.split("\n"), toData.split("\n"))
        return []

    def mergeWithServer(self):
        retVal = self.rpcClient.getCompressedData()
        if retVal["OK"]:
            remoteCFG = CFG()
            data = retVal["Value"]
            if isinstance(data, str):
                data = data.encode(errors="surrogateescape")
            remoteCFG.loadFromBuffer(zlib.decompress(data).decode())
            serverVersion = gConfigurationData.getVersion(remoteCFG)
            self.cfgData = remoteCFG.mergeWith(self.cfgData)
            gConfigurationData.setVersion(serverVersion, self.cfgData)
        return retVal

    def rollbackToVersion(self, version):
        return self.rpcClient.rollbackToVersion(version)

    def updateGConfigurationData(self):
        gConfigurationData.setRemoteCFG(self.cfgData)
