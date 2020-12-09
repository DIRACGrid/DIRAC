""" Cache for the Plotting service plots
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os.path
import time
import threading

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities.Graphs import graph


class PlotCache(object):

  def __init__(self, plotsLocation=False):
    self.plotsLocation = plotsLocation
    self.alive = True
    self.__graphCache = DictCache(deleteFunction=_deleteGraph)
    self.__graphLifeTime = 600
    self.purgeThread = threading.Thread(target=self.purgeExpired)
    self.purgeThread.start()

  def setPlotsLocation(self, plotsDir):
    self.plotsLocation = plotsDir
    for plot in os.listdir(self.plotsLocation):
      if plot.find(".png") > 0:
        plotLocation = "%s/%s" % (self.plotsLocation, plot)
        gLogger.verbose("Purging %s" % plotLocation)
        os.unlink(plotLocation)

  def purgeExpired(self):
    while self.alive:
      time.sleep(self.__graphLifeTime)
      self.__graphCache.purgeExpired()

  def getPlot(self, plotHash, plotData, plotMetadata, subplotMetadata):
    """
    Get plot from the cache if exists, else generate it
    """

    plotDict = self.__graphCache.get(plotHash)
    if plotDict is None:
      basePlotFileName = "%s/%s.png" % (self.plotsLocation, plotHash)
      if subplotMetadata:
        retVal = graph(plotData, basePlotFileName, plotMetadata, metadata=subplotMetadata)
      else:
        retVal = graph(plotData, basePlotFileName, plotMetadata)
      if not retVal['OK']:
        return retVal
      plotDict = retVal['Value']
      if plotDict['plot']:
        plotDict['plot'] = os.path.basename(basePlotFileName)
      self.__graphCache.add(plotHash, self.__graphLifeTime, plotDict)
    return S_OK(plotDict)

  def getPlotData(self, plotFileName):
    filename = "%s/%s" % (self.plotsLocation, plotFileName)
    try:
      with open(filename, "rb") as fd:
        data = fd.read()
    except Exception as v:
      return S_ERROR("Can't open file %s: %s" % (plotFileName, str(v)))
    return S_OK(data)


def _deleteGraph(plotDict):
  try:
    for key in plotDict:
      value = plotDict[key]
      if value and os.path.isfile(value):
        os.unlink(value)
  except BaseException:
    pass


gPlotCache = PlotCache()
