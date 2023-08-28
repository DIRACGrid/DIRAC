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
    Service interface, manage Slave/Master server for CS
    Thread components
    """

    def __init__(self, sURL):
        threading.Thread.__init__(self)
        ServiceInterfaceBase.__init__(self, sURL)

    def _launchCheckSlaves(self):
        """
        Start loop which check if slaves are alive
        """
        gLogger.info("Starting purge slaves thread")
        self.daemon = True
        self.start()

    def run(self):
        while True:
            iWaitTime = gConfigurationData.getSlavesGraceTime()
            time.sleep(iWaitTime)
            self._checkSlavesStatus()

    def _updateServiceConfiguration(self, urlSet, fromMaster=False):
        """
        Update configuration of a set of slave services in parallel

        :param set urlSet: a set of service URLs
        :param fromMaster: flag to force updating from the master CS
        :return: Nothing
        """
        if not urlSet:
            return
        with ThreadPoolExecutor(max_workers=len(urlSet)) as executor:
            futureUpdate = {executor.submit(self._forceServiceUpdate, url, fromMaster): url for url in urlSet}
            for future in as_completed(futureUpdate):
                url = futureUpdate[future]
                result = future.result()
                if result["OK"]:
                    gLogger.info("Successfully updated slave configuration", url)
                else:
                    gLogger.error("Failed to update slave configuration", url)
