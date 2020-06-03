""" Threaded implementation of service interface
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import threading
from DIRAC import gLogger

from DIRAC.ConfigurationSystem.private.ServiceInterfaceBase import ServiceInterfaceBase
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

__RCSID__ = "$Id$"


class ServiceInterface(ServiceInterfaceBase, threading.Thread):
  """
    Service interface, manage Slave/Master server for CS
    Thread components
  """

  def __init__(self, sURL):
    threading.Thread.__init__(self)
    ServiceInterfaceBase.__init__(self, sURL)
    self.__launchCheckSlaves()

  def __launchCheckSlaves(self):
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

