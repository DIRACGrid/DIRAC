""" Priority corrector for the group and in-group shares
"""
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.private.correctors.BaseCorrector import BaseCorrector


class SharesCorrector:
    def __init__(self, opsHelper):
        if not opsHelper:
            opsHelper = Operations()
        self.__opsHelper = opsHelper
        self.__log = gLogger.getSubLogger(self.__class__.__name__)
        self.__shareCorrectors = {}
        self.__correctorsOrder = []
        self.__baseCS = "JobScheduling/ShareCorrections"

    def __getCSValue(self, path, defaultValue=""):
        return self.__opsHelper.getValue(f"{self.__baseCS}/{path}", defaultValue)

    def __getCorrectorClass(self, correctorName):
        baseImport = "WorkloadManagementSystem.private.correctors"
        fullCN = f"{baseImport}.{correctorName}Corrector"
        result = ObjectLoader().getObjects(baseImport, ".*Corrector", parentClass=BaseCorrector)
        if not result["OK"]:
            return result
        data = result["Value"]
        if fullCN not in data:
            return S_ERROR("Can't find corrector %s" % fullCN)
        return S_OK(data[fullCN])

    def instantiateRequiredCorrectors(self):
        correctorsToStart = self.__getCSValue("ShareCorrectorsToStart", [])
        self.__correctorsOrder = correctorsToStart
        self.__log.info("Correctors requested: %s" % ", ".join(correctorsToStart))
        for corrector in self.__shareCorrectors:
            if corrector not in correctorsToStart:
                self.__log.info("Stopping corrector %s" % corrector)
                del self.__shareCorrectors[corrector]
        for corrector in correctorsToStart:
            if corrector not in self.__shareCorrectors:
                self.__log.info("Starting corrector %s" % corrector)
                result = self.__opsHelper.getSections(f"{self.__baseCS}/{corrector}")
                if not result["OK"]:
                    self.__log.error(
                        "Cannot get list of correctors to instantiate",
                        " for corrector type {}: {}".format(corrector, result["Message"]),
                    )
                    continue
                groupCorrectors = result["Value"]
                self.__shareCorrectors[corrector] = {}
                result = self.__getCorrectorClass(corrector)
                if not result["OK"]:
                    self.__log.error("Cannot instantiate corrector", "{} {}".format(corrector, result["Message"]))
                    continue
                correctorClass = result["Value"]
                for groupCor in groupCorrectors:
                    groupPath = f"{corrector}/{groupCor}/Group"
                    groupToCorrect = self.__getCSValue(groupPath, "")
                    if groupToCorrect:
                        groupKey = "gr:%s" % groupToCorrect
                    else:
                        groupKey = "global"
                    self.__log.info(f"Instantiating group corrector {groupCor} ({groupToCorrect}) of type {corrector}")
                    if groupKey in self.__shareCorrectors[corrector]:
                        self.__log.error(
                            "There are two group correctors defined",
                            f" for {corrector} type (group {groupToCorrect})",
                        )
                    else:
                        groupCorPath = f"{self.__baseCS}/{corrector}/{groupCor}"
                        correctorObj = correctorClass(self.__opsHelper, groupCorPath, groupToCorrect)
                        result = correctorObj.initialize()
                        if not result["OK"]:
                            self.__log.error(
                                "Could not initialize corrector %s for %s: %s"
                                % (corrector, groupKey, result["Message"])
                            )
                        else:
                            self.__shareCorrectors[corrector][groupKey] = correctorObj
        return S_OK()

    def updateCorrectorsKnowledge(self):
        for corrector in self.__shareCorrectors:
            for groupTC in self.__shareCorrectors[corrector]:
                self.__shareCorrectors[corrector][groupTC].updateHistoryKnowledge()

    def update(self):
        self.instantiateRequiredCorrectors()
        self.updateCorrectorsKnowledge()

    def correctShares(self, shareDict, group=""):
        if group:
            groupKey = "gr:%s" % group
        else:
            groupKey = "global"
        for corrector in self.__shareCorrectors:
            if groupKey in self.__shareCorrectors[corrector]:
                shareDict = self.__shareCorrectors[corrector][groupKey].applyCorrection(shareDict)
        return shareDict
