"""
  Service interface adapted to work with tornado, must be used only by tornado service handlers
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from tornado import gen
from tornado.ioloop import IOLoop
from DIRAC.ConfigurationSystem.private.ServiceInterfaceBase import ServiceInterfaceBase
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC import gLogger


class ServiceInterfaceTornado(ServiceInterfaceBase):
  """
    Service interface adapted to work with tornado
  """

  def __init__(self, sURL):
    ServiceInterfaceBase.__init__(self, sURL)

  def _launchCheckSlaves(self):
    """
      Start loop to check if slaves are alive
    """
    IOLoop.current().spawn_callback(self.run)
    gLogger.info("Starting purge slaves thread")

  def run(self):
    """
      Check if slaves are alive
    """
    while True:
      yield gen.sleep(gConfigurationData.getSlavesGraceTime())
      self._checkSlavesStatus()

  def _updateServiceConfiguration(self, urlSet, fromMaster=False):
    """
    Update configuration in a set of service in parallel

    :param set urlSet: a set of service URLs
    :param fromMaster: flag to force updating from the master CS
    :return: Nothing
    """

    for url in urlSet:
      IOLoop.current().spawn_callback(self._forceServiceUpdate, [url, fromMaster])
