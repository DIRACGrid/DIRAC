""" Accounting reporter
"""
import hashlib
import re

from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.AccountingSystem.private.Policies import gPoliciesList
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
from DIRAC.Core.Utilities.ObjectLoader import loadObjects


class PlottersList:
    def __init__(self):
        objectsLoaded = loadObjects(
            "AccountingSystem/private/Plotters", re.compile(r".*[a-z1-9]Plotter\.py$"), BaseReporter
        )
        self.__plotters = {}
        for objName in objectsLoaded:
            self.__plotters[objName[:-7]] = objectsLoaded[objName]

    def getPlotterClass(self, typeName):
        try:
            return self.__plotters[typeName]
        except KeyError:
            return None


class MainReporter:
    def __init__(self, db):
        self._db = db
        self.csSection = getServiceSection("Accounting/ReportGenerator")
        self.plotterList = PlottersList()

    def __calculateReportHash(self, reportRequest):
        requestToHash = dict(reportRequest)
        granularity = gConfig.getValue(f"{self.csSection}/CacheTimeGranularity", 300)
        granularity *= 1000
        for key in ("startTime", "endTime"):
            epoch = requestToHash[key]
            requestToHash[key] = epoch - epoch % granularity
        md5Hash = hashlib.md5()
        md5Hash.update(repr(requestToHash).encode())
        return md5Hash.hexdigest()

    def generate(self, reportRequest, credDict):
        typeName = reportRequest["typeName"]
        plotterClass = self.plotterList.getPlotterClass(typeName)
        if not plotterClass:
            return S_ERROR(f"There's no reporter registered for type {typeName}")
        if typeName in gPoliciesList:
            retVal = gPoliciesList[typeName].checkRequest(
                reportRequest["reportName"], credDict, reportRequest["condDict"], reportRequest["grouping"]
            )
            if not retVal["OK"]:
                return retVal
        reportRequest["hash"] = self.__calculateReportHash(reportRequest)
        plotter = plotterClass(self._db, reportRequest["extraArgs"])
        return plotter.generate(reportRequest)

    def list(self, typeName):
        plotterClass = self.plotterList.getPlotterClass(typeName)
        if not plotterClass:
            return S_ERROR(f"There's no plotter registered for type {typeName}")
        plotter = plotterClass(self._db)
        return S_OK(plotter.plotsList())
