"""
It is a helper module which contains the available reports
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import re

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
from DIRAC.MonitoringSystem.private.Plotters.BasePlotter import BasePlotter as myBasePlotter
from DIRAC.Core.Utilities.ObjectLoader import loadObjects

__RCSID__ = "$Id$"


class PlottersList(object):

  """
  Used to determine all available plotters used to create the plots

  :param dict __plotters: stores the available plotters
  """

  def __init__(self):
    """ c'tor

    :param self: self reference
    """

    objectsLoaded = loadObjects('MonitoringSystem/private/Plotters',
                                re.compile(r".*[a-z1-9]Plotter\.py$"),
                                myBasePlotter)
    self.__plotters = {}
    for objName in objectsLoaded:
      self.__plotters[objName[:-7]] = objectsLoaded[objName]

  def getPlotterClass(self, typeName):
    """
    It returns the plotter class for a given monitoring type
    """
    try:
      return S_OK(self.__plotters[typeName])
    except KeyError:
      return S_ERROR()


class MainReporter(object):
  """
  :param object __db: database object
  :param str __setup: DIRAC setup
  :param str __csSection: CS section used to configure some parameters.
  :param list __plotterList: available plotters
  """

  def __init__(self, db, setup):
    """ c'tor

    :param self: self reference
    :param object: the database module
    :param str setup: DIRAC setup
    """
    self.__db = db
    self.__setup = setup
    self.__csSection = getServiceSection("Monitoring/Monitoring", setup=setup)
    self.__plotterList = PlottersList()

  def __calculateReportHash(self, reportRequest):
    """
    It creates an unique identifier

    :param dict reportRequest: plot attributes used to create the plot
    :return: an unique value
    :rtype: str
    """
    requestToHash = dict(reportRequest)
    granularity = gConfig.getValue("%s/CacheTimeGranularity" % self.__csSection, 300)
    for key in ('startTime', 'endTime'):
      epoch = requestToHash[key]
      requestToHash[key] = epoch - epoch % granularity
    md5Hash = hashlib.md5()
    md5Hash.update(repr(requestToHash).encode())
    md5Hash.update(self.__setup.encode())
    return md5Hash.hexdigest()

  def generate(self, reportRequest, credDict):
    """
    It is used to create a plot.

    :param dict reportRequest: plot attributes used to create the plot

    .. note:: I know credDict is not used, but if we plan to add some policy, we need to use it!

    :return: dict S_OK/S_ERROR the values used to create the plot
    """
    typeName = reportRequest['typeName']
    plotterClass = self.__plotterList.getPlotterClass(typeName)
    if not plotterClass['OK']:
      return S_ERROR("There's no reporter registered for type %s" % typeName)

    reportRequest['hash'] = self.__calculateReportHash(reportRequest)
    plotter = plotterClass['Value'](self.__db, self.__setup, reportRequest['extraArgs'])
    return plotter.generate(reportRequest)

  def list(self, typeName):
    """
    It returns the available plots

    :param str typeName: monitoring type
    :return: dict S_OK/S_ERROR list of available reports (plots)
    """
    plotterClass = self.__plotterList.getPlotterClass(typeName)
    if not plotterClass['OK']:
      return S_ERROR("There's no plotter registered for type %s" % typeName)
    plotter = plotterClass['Value'](self.__db, self.__setup)
    return S_OK(plotter.plotsList())
