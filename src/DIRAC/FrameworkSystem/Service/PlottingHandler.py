""" Plotting Service generates graphs according to the client specifications
    and data
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import hashlib

from DIRAC import S_OK, S_ERROR, rootPath, gConfig, gLogger
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.FrameworkSystem.Service.PlotCache import gPlotCache


def initializePlottingHandler(serviceInfo):

  # Get data location
  plottingSection = PathFinder.getServiceSection("Framework/Plotting")
  dataPath = gConfig.getValue("%s/DataLocation" % plottingSection, "data/graphs")
  dataPath = dataPath.strip()
  if "/" != dataPath[0]:
    dataPath = os.path.realpath("%s/%s" % (gConfig.getValue('/LocalSite/InstancePath', rootPath), dataPath))
  gLogger.info("Data will be written into %s" % dataPath)
  try:
    os.makedirs(dataPath)
  except Exception:
    pass
  try:
    testFile = "%s/plot__.test" % dataPath
    with open(testFile, "w"):
      pass
    os.unlink(testFile)
  except IOError:
    gLogger.fatal("Can't write to %s" % dataPath)
    return S_ERROR("Data location is not writable")

  gPlotCache.setPlotsLocation(dataPath)
  gMonitor.registerActivity("plotsDrawn", "Drawn plot images", "Plotting requests", "plots", gMonitor.OP_SUM)
  return S_OK()


class PlottingHandler(RequestHandler):

  def __calculatePlotHash(self, data, metadata, subplotMetadata):
    m = hashlib.md5()
    m.update(repr({
        'Data': data,
        'PlotMetadata': metadata,
        'SubplotMetadata': subplotMetadata
    }).encode())
    return m.hexdigest()

  types_generatePlot = [[dict, list], dict]

  def export_generatePlot(self, data, plotMetadata, subplotMetadata={}):
    """ Create a plot according to the client specification and return its name
    """
    plotHash = self.__calculatePlotHash(data, plotMetadata, subplotMetadata)
    result = gPlotCache.getPlot(plotHash, data, plotMetadata, subplotMetadata)
    if not result['OK']:
      return result
    return S_OK(result['Value']['plot'])

  def transfer_toClient(self, fileId, token, fileHelper):
    """
    Get graphs data
    """
    retVal = gPlotCache.getPlotData(fileId)
    if not retVal['OK']:
      return retVal
    retVal = fileHelper.sendData(retVal['Value'])
    if not retVal['OK']:
      return retVal
    fileHelper.sendEOF()
    return S_OK()
