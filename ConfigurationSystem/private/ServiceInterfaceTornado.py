"""
  Service interface adapted to work with tornado, must be used only by tornado service handlers
"""
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
    self.__launchCheckSlaves()

  def __launchCheckSlaves(self):
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
