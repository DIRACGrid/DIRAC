""" PlottingClient is a client of the Plotting Service
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import tempfile
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient


class PlottingClient(object):

  def __init__(self, rpcClient=None, transferClient=None):
    self.serviceName = "Framework/Plotting"
    self.rpcClient = rpcClient
    self.transferClient = transferClient

  def __getRPCClient(self):
    if self.rpcClient:
      return self.rpcClient
    return RPCClient(self.serviceName)

  def __getTransferClient(self):
    if self.transferClient:
      return self.transferClient
    return TransferClient(self.serviceName)

  def getPlotToMemory(self, plotName):
    """ Get the prefabricated plot from the service and return it as a string
    """
    transferClient = self.__getTransferClient()
    tmpFile = tempfile.TemporaryFile()
    retVal = transferClient.receiveFile(tmpFile, plotName)
    if not retVal['OK']:
      return retVal
    tmpFile.seek(0)
    data = tmpFile.read()
    tmpFile.close()
    return S_OK(data)

  def getPlotToFile(self, plotName, fileName):
    """ Get the prefabricated plot from the service and store it in a file
    """
    transferClient = self.__getTransferClient()
    try:
      with open(fileName, "wb") as destFile:
        retVal = transferClient.receiveFile(destFile, plotName)
    except Exception as e:
      return S_ERROR("Can't open file %s for writing: %s" % (fileName, str(e)))
    if not retVal['OK']:
      return retVal
    return S_OK(fileName)

  def graph(self, data, fname=False, *args, **kw):
    """ Generic method to obtain graphs from the Plotting service. The requested
        graphs are completely described by their data and metadata
    """

    client = self.__getRPCClient()
    plotMetadata = {}
    for arg in args:
      if isinstance(arg, dict):
        plotMetadata.update(arg)
      else:
        return S_ERROR('Non-dictionary non-keyed argument')
    plotMetadata.update(kw)
    result = client.generatePlot(data, plotMetadata)
    if not result['OK']:
      return result

    plotName = result['Value']
    if fname and fname != 'Memory':
      result = self.getPlotToFile(plotName, fname)
    else:
      result = self.getPlotToMemory(plotName)

    return result

  def barGraph(self, data, fileName, *args, **kw):
    return self.graph(data, fileName, plot_type='BarGraph', statistics_line=True, *args, **kw)

  def lineGraph(self, data, fileName, *args, **kw):
    return self.graph(data, fileName, plot_type='LineGraph', statistics_line=True, *args, **kw)

  def curveGraph(self, data, fileName, *args, **kw):
    return self.graph(data, fileName, plot_type='CurveGraph', statistics_line=True, *args, **kw)

  def cumulativeGraph(self, data, fileName, *args, **kw):
    return self.graph(data, fileName, plot_type='LineGraph', cumulate_data=True, *args, **kw)

  def pieGraph(self, data, fileName, *args, **kw):
    prefs = {'xticks': False, 'yticks': False, 'legend_position': 'right'}
    return self.graph(data, fileName, prefs, plot_type='PieGraph', *args, **kw)

  def qualityGraph(self, data, fileName, *args, **kw):
    prefs = {'plot_axis_grid': False}
    return self.graph(data, fileName, prefs, plot_type='QualityMapGraph', *args, **kw)

  def textGraph(self, text, fileName, *args, **kw):
    prefs = {'text_image': text}
    return self.graph({}, fileName, prefs, *args, **kw)

  def histogram(self, data, fileName, bins, *args, **kw):
    try:
      from pylab import hist
    except Exception:
      return S_ERROR("No pylab module available")
    values, vbins, patches = hist(data, bins)
    histo = dict(zip(vbins, values))
    span = (max(data) - min(data)) / float(bins) * 0.98
    return self.graph(histo, fileName, plot_type='BarGraph', span=span, statistics_line=True, *args, **kw)
