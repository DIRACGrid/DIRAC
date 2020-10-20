"""
  Add-on to make the MonitoringClient works with an IOLoop from a Tornado Server
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import tornado.ioloop
from DIRAC import gLogger


class MonitoringFlusherTornado(object):
  """
  This class flushes all monitoring clients registered
  Works with the Tornado IOLoop
  """

  def __init__(self):
    self.__mcList = []
    gLogger.info("Using MonitoringClient in IOLoop mode")
    # Here we don't need to use IOLoop.current(), tornado will attach periodic callback to the current IOLoop himself
    # We set callback every 5 minnutes
    tornado.ioloop.PeriodicCallback(self.flush, 300000).start()

  def flush(self, allData=False):
    gLogger.info('Flushing monitoring')
    for mc in self.__mcList:
      mc.flush(allData)

  def registerMonitoringClient(self, mc):
    if mc not in self.__mcList:
      self.__mcList.append(mc)
