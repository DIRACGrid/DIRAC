""" Refresh local CS (if needed)
Used each time you call gConfig. It keep your configuration up-to-date with the configuration server
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import threading
import time
import os

from six.moves import _thread as thread

from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.RefresherBase import RefresherBase
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import LockRing


class Refresher(RefresherBase, threading.Thread):
  """
    The refresher
    A long time ago, in a code away, far away...
    A guy do the code to autorefresh the configuration
    To prepare transition to HTTPS we have done separation
    between the logic and the implementation of background
    tasks, it's the original version, for diset, using thread.

  """

  def __init__(self):
    threading.Thread.__init__(self)
    RefresherBase.__init__(self)
    self._triggeredRefreshLock = LockRing.LockRing().getLock()

  def _refreshInThread(self):
    """
      Refreshing configuration in the background. By default it uses a thread but it can be
      run also in the IOLoop
    """
    retVal = self._refresh()
    if not retVal['OK']:
      gLogger.error("Error while updating the configuration", retVal['Message'])

  def refreshConfigurationIfNeeded(self):
    """
      Refresh the configuration if automatic updates are disabled, refresher is enabled and servers are defined
    """
    if not self._refreshEnabled or self._automaticUpdate or not gConfigurationData.getServers():
      return
    # To improve performance, skip acquiring the lock if possible
    if not self._lastRefreshExpired():
      return
    self._triggeredRefreshLock.acquire()
    try:
      if not self._lastRefreshExpired():
        return
      self._lastUpdateTime = time.time()
    finally:
      try:
        self._triggeredRefreshLock.release()
      except thread.error:
        pass
    # Launch the refresh
    thd = threading.Thread(target=self._refreshInThread)
    thd.setDaemon(1)
    thd.start()

  def autoRefreshAndPublish(self, sURL):
    """
      Start the autorefresh background task

      :param str sURL: URL of the configuration server
    """
    gLogger.debug("Setting configuration refresh as automatic")
    if not gConfigurationData.getAutoPublish():
      gLogger.debug("Slave server won't auto publish itself")
    if not gConfigurationData.getName():
      import DIRAC
      DIRAC.abort(10, "Missing configuration name!")
    self._url = sURL
    self._automaticUpdate = True
    self.setDaemon(1)
    self.start()

  def run(self):
    while self._automaticUpdate:
      time.sleep(gConfigurationData.getPropagationTime())
      if self._refreshEnabled:
        if not self._refreshAndPublish():
          gLogger.error("Can't refresh configuration from any source")

  def daemonize(self):
    """
      Daemonize the background tasks
    """

    self.setDaemon(1)
    self.start()


# Here we define the refresher which should be used.
# By default we use the original refresher.

# Be careful, if you never start the IOLoop (with a TornadoServer for example)
# the TornadoRefresher will not work. IOLoop can be started after refresher
# but background tasks will be delayed until IOLoop start.
# DIRAC_USE_TORNADO_IOLOOP is defined by starting scripts
if os.environ.get('DIRAC_USE_TORNADO_IOLOOP', 'false').lower() in ('yes', 'true'):
  from DIRAC.ConfigurationSystem.private.TornadoRefresher import TornadoRefresher
  gRefresher = TornadoRefresher()
else:
  gRefresher = Refresher()


if __name__ == "__main__":
  time.sleep(0.1)
  gRefresher.daemonize()
