########################################################################
# $HeadURL$
# File :   WatchdogFactory.py
# Author : Stuart Paterson
########################################################################

"""  The Watchdog Factory instantiates a given Watchdog based on a quick
     determination of the local operating system.
"""

from DIRAC                                               import S_OK, S_ERROR, gLogger

__RCSID__ = "$Id$"

import re, platform

class WatchdogFactory(object):

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.version = platform.uname()
    self.log = gLogger

  #############################################################################
  def getWatchdog(self, pid, thread, spObject, jobcputime):
    """This method returns the CE instance corresponding to the local OS,
       the Linux watchdog is returned by default.
    """
    localOS = None

    if re.search('Darwin', self.version[0]):
      localOS = 'Mac'
      self.log.info('WatchdogFactory will create Watchdog%s instance' % (localOS))
    elif re.search('Windows', self.version[0]):
      localOS = 'Windows'
      self.log.info('WatchdogFactory will create Watchdog%s instance' % (localOS))
    else:
      localOS = 'Linux'
      self.log.info('WatchdogFactory will create Watchdog%s instance' % (localOS))

    try:
      subClassName = "Watchdog%s" % (localOS)
      ceSubClass = __import__('DIRAC.WorkloadManagementSystem.JobWrapper.%s' % subClassName,
                              globals(), locals(), [subClassName])
    except Exception, x:
      msg = 'WatchdogFactory could not import DIRAC.WorkloadManagementSystem.JobWrapper.%s' % (subClassName)
      self.log.error(msg, x)
      return S_ERROR(msg)

    try:
      ceStr = 'ceSubClass.%s(pid, thread, spObject, jobcputime)' % (subClassName)
      watchdogInstance = eval(ceStr)
    except Exception, x:
      msg = 'WatchdogFactory could not instantiate %s()' % (subClassName)
      self.log.error(msg, x)
      return S_ERROR(msg)

    return S_OK(watchdogInstance)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
