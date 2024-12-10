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

    def _launchCheckWorkers(self):
        """
        Start loop to check if workers are alive
        """
        IOLoop.current().spawn_callback(self.run)
        gLogger.info("Starting purge workers thread")

    def run(self):
        """
        Check if workers are alive
        """
        while True:
            yield gen.sleep(gConfigurationData.getSlavesGraceTime())
            self._checkWorkersStatus()

    def _updateServiceConfiguration(self, urlSet, fromController=False):
        """
        Update configuration in a set of service in parallel

        :param set urlSet: a set of service URLs
        :param fromController: flag to force updating from the controller CS
        :return: Nothing
        """

        for url in urlSet:
            IOLoop.current().spawn_callback(self._forceServiceUpdate, [url, fromController])
