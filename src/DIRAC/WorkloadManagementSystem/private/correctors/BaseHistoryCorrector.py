""" BaseHistoryCorrector is a base class for correctors of user shares within
    a given group based on the history of the resources consumption
"""
import time as nativetime

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.WorkloadManagementSystem.private.correctors.BaseCorrector import BaseCorrector


class BaseHistoryCorrector(BaseCorrector):
    _GLOBAL_MAX_CORRECTION = "MaxGlobalCorrection"
    _SLICE_TIME_SPAN = "TimeSpan"
    _SLICE_WEIGHT = "Weight"
    _SLICE_MAX_CORRECTION = "MaxCorrection"

    def initialize(self):
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.__usageHistory = {}
        self.__slices = {}
        self.__lastHistoryUpdate = 0
        self.__globalCorrectionFactor = 5
        self._fillSlices()
        return S_OK()

    def _fillSlices(self):
        self.log.info("Filling time slices...")
        self.__slices = {}
        self.__globalCorrectionFactor = self.getCSOption(self._GLOBAL_MAX_CORRECTION, 5)
        result = self.getCSSections()
        if not result["OK"]:
            self.log.error("Cound not get configured time slices", result["Message"])
            return
        timeSlices = result["Value"]
        for timeSlice in timeSlices:
            self.__slices[timeSlice] = {}
            for key, defaultValue in (
                (self._SLICE_TIME_SPAN, 604800),
                (self._SLICE_WEIGHT, 1),
                (self._SLICE_MAX_CORRECTION, 3),
            ):
                self.__slices[timeSlice][key] = self.getCSOption(f"{timeSlice}/{key}", defaultValue)
        # Weight has to be normalized to sum 1
        weightSum = 0
        for timeSlice in self.__slices:
            weightSum += self.__slices[timeSlice][self._SLICE_WEIGHT]
        for timeSlice in self.__slices:
            self.__slices[timeSlice][self._SLICE_WEIGHT] /= float(weightSum)
        self.log.info(f"Found {len(self.__slices)} time slices")

    def updateHistoryKnowledge(self):
        updatePeriod = self.getCSOption("UpdateHistoryPeriod", 900)
        now = nativetime.time()
        if self.__lastHistoryUpdate + updatePeriod > now:
            self.log.verbose(f"Skipping history update. Last update was less than {updatePeriod} secs ago")
            return
        self.__lastHistoryUpdate = now
        self.log.info("Updating history knowledge")
        self.__usageHistory = {}
        for timeSlice in self.__slices:
            result = self._getUsageHistoryForTimeSpan(self.__slices[timeSlice][self._SLICE_TIME_SPAN], self.getGroup())
            if not result["OK"]:
                self.__usageHistory = {}
                self.log.warn("Could not get history for slice", f"{timeSlice}: {result['Message']}")
                return
            self.__usageHistory[timeSlice] = result["Value"]
            self.log.verbose(
                f"Got history for slice {timeSlice} ({len(self.__usageHistory[timeSlice])} entities in slice)"
            )
        self.log.info("Updated history knowledge")

    def _getHistoryData(self, _timeSpan, _groupToUse):
        """Get history data from an external source to be defined in a derived class

        :param int timeSpan: time span
        :param str groupToUse: requested user group
        :return: dictionary with history data
        """
        return S_ERROR("Not implemented !")

    def _getUsageHistoryForTimeSpan(self, timeSpan, groupToUse=""):
        result = self._getHistoryData(timeSpan, groupToUse)

        if not result["OK"]:
            self.log.error("Cannot get history data", result["Message"])
            return result
        data = result["Value"].get("data", [])
        if not data:
            message = "Empty history data"
            self.log.warn(message)
            return S_ERROR(message)

        # Map the usernames to DNs
        if groupToUse:
            mappedData = {}
            for userName in data:
                result = getDNForUsername(userName)
                if not result["OK"]:
                    self.log.error("User does not have any DN assigned", f"{userName} :{result['Message']}")
                    continue
                for userDN in result["Value"]:
                    mappedData[userDN] = data[userName]
            data = mappedData

        return S_OK(data)

    def __normalizeShares(self, entityShares):
        totalShare = 0.0
        normalizedShares = {}
        # Normalize shares
        for entity in entityShares:
            totalShare += entityShares[entity]
        self.log.verbose(f"Total share for given entities is {totalShare:.3f}")
        for entity in entityShares:
            normalizedShare = entityShares[entity] / totalShare
            normalizedShares[entity] = normalizedShare
            self.log.verbose(f"Normalized share for {entity}: {normalizedShare:.3f}")

        return normalizedShares

    def applyCorrection(self, entitiesExpectedShare):
        # Normalize expected shares
        normalizedShares = self.__normalizeShares(entitiesExpectedShare)

        if not self.__usageHistory:
            self.log.verbose("No history knowledge available. Correction is 1 for all entities")
            return entitiesExpectedShare

        entitiesSliceCorrections = {entity: [] for entity in entitiesExpectedShare}
        for timeSlice in self.__usageHistory:
            self.log.verbose(f"Calculating correction for time slice {timeSlice}")
            sliceTotal = 0.0
            sliceHistory = self.__usageHistory[timeSlice]
            for entity in entitiesExpectedShare:
                if entity in sliceHistory:
                    sliceTotal += sliceHistory[entity]
                    self.log.verbose(f"Usage for {entity}: {sliceHistory[entity]:.3f}")
            self.log.verbose(f"Total usage for slice {sliceTotal:.3f}")
            if sliceTotal == 0.0:
                self.log.verbose("Slice usage is 0, skeeping slice")
                continue
            maxSliceCorrection = self.__slices[timeSlice][self._SLICE_MAX_CORRECTION]
            minSliceCorrection = 1.0 / maxSliceCorrection
            for entity in entitiesExpectedShare:
                if entity in sliceHistory:
                    normalizedSliceUsage = sliceHistory[entity] / sliceTotal
                    self.log.verbose(
                        "Entity %s is present in slice %s (normalized usage %.2f)"
                        % (entity, timeSlice, normalizedSliceUsage)
                    )
                    sliceCorrectionFactor = normalizedShares[entity] / normalizedSliceUsage
                    sliceCorrectionFactor = min(sliceCorrectionFactor, maxSliceCorrection)
                    sliceCorrectionFactor = max(sliceCorrectionFactor, minSliceCorrection)
                    sliceCorrectionFactor *= self.__slices[timeSlice][self._SLICE_WEIGHT]
                else:
                    self.log.verbose(f"Entity {entity} is not present in slice {timeSlice}")
                    sliceCorrectionFactor = maxSliceCorrection
                self.log.verbose(f"Slice correction factor for entity {entity} is {sliceCorrectionFactor:.3f}")
                entitiesSliceCorrections[entity].append(sliceCorrectionFactor)

        correctedEntityShare = {}
        maxGlobalCorrectionFactor = self.__globalCorrectionFactor
        minGlobalCorrectionFactor = 1.0 / maxGlobalCorrectionFactor
        for entity in entitiesSliceCorrections:
            entityCorrectionFactor = 0.0
            slicesCorrections = entitiesSliceCorrections[entity]
            if not slicesCorrections:
                self.log.verbose(f"Entity does not have any correction {entity}")
                correctedEntityShare[entity] = entitiesExpectedShare[entity]
            else:
                for cF in entitiesSliceCorrections[entity]:
                    entityCorrectionFactor += cF
                entityCorrectionFactor = min(entityCorrectionFactor, maxGlobalCorrectionFactor)
                entityCorrectionFactor = max(entityCorrectionFactor, minGlobalCorrectionFactor)
                correctedShare = entitiesExpectedShare[entity] * entityCorrectionFactor
                correctedEntityShare[entity] = correctedShare
                self.log.verbose(
                    "Final correction factor for entity %s is %.3f\n Final share is %.3f"
                    % (entity, entityCorrectionFactor, correctedShare)
                )
        self.log.verbose(
            "Initial shares:\n  %s"
            % "\n  ".join([f"{en} : {entitiesExpectedShare[en]:.2f}" for en in entitiesExpectedShare])
        )
        self.log.verbose(
            "Corrected shares:\n  %s"
            % "\n  ".join([f"{en} : {correctedEntityShare[en]:.2f}" for en in correctedEntityShare])
        )
        return correctedEntityShare
