""" Threaded implementation of service interface
"""
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from DIRAC import gLogger

from DIRAC.ConfigurationSystem.private.ServiceInterfaceBase import ServiceInterfaceBase
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData


class ServiceInterface(ServiceInterfaceBase, threading.Thread):
    """
    Service interface, manage Worker/Controller server for CS
    Thread components
    """

    def __init__(self, sURL):
        threading.Thread.__init__(self)
        ServiceInterfaceBase.__init__(self, sURL)

    def _launchCheckWorkers(self):
        """
        Start loop which check if Workers are alive
        """
        gLogger.info("Starting purge Workers thread")
        self.daemon = True
        self.start()

    def run(self):
        while True:
            iWaitTime = gConfigurationData.getSlavesGraceTime()
            time.sleep(iWaitTime)
            self._checkWorkersStatus()

    def _updateServiceConfiguration(self, urlSet, fromController=False):
        """
        Update configuration of a set of Worker services in parallel

        :param set urlSet: a set of service URLs
        :param fromController: flag to force updating from the master CS
        :return: Nothing
        """
        if not urlSet:
            return
        with ThreadPoolExecutor(max_workers=len(urlSet)) as executor:
            futureUpdate = {executor.submit(self._forceServiceUpdate, url, fromController): url for url in urlSet}
            for future in as_completed(futureUpdate):
                url = futureUpdate[future]
                result = future.result()
                if result["OK"]:
                    gLogger.info("Successfully updated Worker configuration", url)
                else:
                    gLogger.error("Failed to update Worker configuration", url)
