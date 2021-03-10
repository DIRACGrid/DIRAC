""" Accounting Cache
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os.path
import time
import threading

from DIRAC import S_OK, S_ERROR, gLogger, rootPath, gConfig
from DIRAC.Core.Utilities.DictCache import DictCache


class DataCache(object):

  def __init__(self, dirName='accountingPlots'):
    self.graphsLocation = os.path.join(gConfig.getValue('/LocalSite/InstancePath', rootPath), 'data', dirName)
    self.cachedGraphs = {}
    self.alive = True
    self.purgeThread = threading.Thread(target=self.purgeExpired)
    self.purgeThread.setDaemon(1)
    self.purgeThread.start()
    self.__dataCache = DictCache()
    self.__graphCache = DictCache(deleteFunction=self._deleteGraph)
    self.__dataLifeTime = 600
    self.__graphLifeTime = 3600

  def setGraphsLocation(self, graphsDir):
    self.graphsLocation = graphsDir
    for graphName in os.listdir(self.graphsLocation):
      if graphName.find(".png") > 0:
        graphLocation = "%s/%s" % (self.graphsLocation, graphName)
        gLogger.verbose("Purging %s" % graphLocation)
        os.unlink(graphLocation)

  def purgeExpired(self):
    while self.alive:
      time.sleep(600)
      self.__graphCache.purgeExpired()
      self.__dataCache.purgeExpired()

  def getReportData(self, reportRequest, reportHash, dataFunc):
    """
    Get report data from cache if exists, else generate it
    """
    reportData = self.__dataCache.get(reportHash)
    if not reportData:
      retVal = dataFunc(reportRequest)
      if not retVal['OK']:
        return retVal
      reportData = retVal['Value']
      self.__dataCache.add(reportHash, self.__dataLifeTime, reportData)
    return S_OK(reportData)

  def getReportPlot(self, reportRequest, reportHash, reportData, plotFunc):
    """
    Get report data from cache if exists, else generate it
    """
    plotDict = self.__graphCache.get(reportHash)
    if not plotDict:
      basePlotFileName = "%s/%s" % (self.graphsLocation, reportHash)
      retVal = plotFunc(reportRequest, reportData, basePlotFileName)
      if not retVal['OK']:
        return retVal
      plotDict = retVal['Value']
      if plotDict['plot']:
        plotDict['plot'] = "%s.png" % reportHash
      if plotDict['thumbnail']:
        plotDict['thumbnail'] = "%s.thb.png" % reportHash
      self.__graphCache.add(reportHash, self.__graphLifeTime, plotDict)
    return S_OK(plotDict)

  def getPlotData(self, plotFileName):
    filename = "%s/%s" % (self.graphsLocation, plotFileName)
    try:
      fd = open(filename, "rb")
      data = fd.read()
      fd.close()
    except Exception as e:
      return S_ERROR("Can't open file %s: %s" % (plotFileName, str(e)))
    return S_OK(data)

  def _deleteGraph(self, plotDict):
    try:
      for key in plotDict:
        value = plotDict[key]
        if value:
          fPath = os.path.join(self.graphsLocation, str(value))
          if os.path.isfile(fPath):
            gLogger.info("Deleting plot from cache", value)
            os.unlink(fPath)
          else:
            gLogger.info("Plot has already been deleted", value)
    except Exception:
      pass
