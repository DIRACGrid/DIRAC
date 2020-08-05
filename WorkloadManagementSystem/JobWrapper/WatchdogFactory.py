"""  The Watchdog Factory instantiates a given Watchdog based on a quick
     determination of the local operating system.
"""

__RCSID__ = "$Id$"

import re
import platform

from DIRAC import S_OK, S_ERROR, gLogger


class WatchdogFactory(object):

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.version = platform.uname()
    self.log = gLogger.getSubLogger('WatchdogFactory')
    self.watchDogsLocation = 'DIRAC.WorkloadManagementSystem.JobWrapper'

  #############################################################################
  def getWatchdog(self, pid, exeThread, spObject, jobCPUTime, memoryLimit, processors=1, jobArgs={}):
    """ This method returns the CE instance corresponding to the local OS. The Linux watchdog is returned by default.
    """
    if re.search('Darwin', self.version[0]):
      localOS = 'Mac'
      self.log.info('WatchdogFactory will create Watchdog%s instance' % (localOS))
    else:
      localOS = 'Linux'
      self.log.info('WatchdogFactory will create Watchdog%s instance' % (localOS))

    subClassName = "Watchdog%s" % (localOS)

    try:
      wdModule = __import__(self.watchDogsLocation + '.%s' % subClassName, globals(), locals(), [subClassName])
    except ImportError as e:
      self.log.exception(
          "Failed to import module" + self.watchDogsLocation + '.%s' %
          subClassName + '.%s' %
          subClassName + ': ' + str(e))
      return S_ERROR("Failed to import module")
    try:
      wd_o = getattr(wdModule, subClassName)(pid=pid,
                                             exeThread=exeThread,
                                             spObject=spObject,
                                             jobCPUTime=jobCPUTime,
                                             memoryLimit=memoryLimit,
                                             processors=processors,
                                             jobArgs=jobArgs)
      return S_OK(wd_o)
    except AttributeError as e:
      self.log.exception("Failed to create %s(): %s." % (subClassName, e))
      return S_ERROR("Failed to create object")

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
