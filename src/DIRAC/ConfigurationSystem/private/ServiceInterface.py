""" Threaded implementation of service interface
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import threading
from DIRAC import gLogger, S_OK

from DIRAC.ConfigurationSystem.private.ServiceInterfaceBase import ServiceInterfaceBase
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.Utilities.ThreadPool import ThreadPool

__RCSID__ = "$Id$"


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
    self.setDaemon(1)
    self.start()

  def run(self):
    while True:
      iWaitTime = gConfigurationData.getSlavesGraceTime()
      time.sleep(iWaitTime)
      self._checkSlavesStatus()

  def _updateServiceConfiguration(self, urlSet, fromMaster=False):
    """
    Update configuration in a set of service in parallel

    :param set urlSet: a set of service URLs
    :param fromMaster: flag to force updating from the master CS
    :return: Nothing
    """
    pool = ThreadPool(len(urlSet))
    for url in urlSet:
      pool.generateJobAndQueueIt(self._forceServiceUpdate,
                                 args=[url, fromMaster],
                                 kwargs={},
                                 oCallback=self.__processResults)
    pool.processAllResults()

  def __processResults(self, _id, result):
    if not result['OK']:
      gLogger.warn("Failed to update configuration on", result['URL'] + ':' + result['Message'])
