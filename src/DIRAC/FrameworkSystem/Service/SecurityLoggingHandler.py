""" ProxyManager is the implementation of the ProxyManagement service
    in the DISET framework
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from DIRAC import gLogger, S_OK, S_ERROR, gConfig, rootPath
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.FrameworkSystem.private.SecurityFileLog import SecurityFileLog
from DIRAC.FrameworkSystem.Client.SecurityLogClient import SecurityLogClient

__RCSID__ = "$Id$"

gSecurityFileLog = False


def initializeSecurityLoggingHandler(serviceInfo):
  global gSecurityFileLog

  serviceCS = serviceInfo['serviceSectionPath']
  dataPath = gConfig.getValue("%s/DataLocation" % serviceCS, "data/securityLog")
  dataPath = dataPath.strip()
  if "/" != dataPath[0]:
    dataPath = os.path.realpath("%s/%s" % (gConfig.getValue('/LocalSite/InstancePath', rootPath), dataPath))
  gLogger.info("Data will be written into %s" % dataPath)
  mkDir(dataPath)

  try:
    testFile = "%s/seclog.jarl.test" % dataPath
    with open(testFile, "w"):
      pass
    os.unlink(testFile)
  except IOError:
    gLogger.fatal("Can't write to %s" % dataPath)
    return S_ERROR("Data location is not writable")
  # Define globals
  gSecurityFileLog = SecurityFileLog(dataPath)
  SecurityLogClient().setLogStore(gSecurityFileLog)
  return S_OK()


class SecurityLoggingHandler(RequestHandler):

  types_logAction = [(list, tuple)]

  def export_logAction(self, secMsg):
    """ Log a single action
    """
    result = gSecurityFileLog.logAction(secMsg)
    if not result['OK']:
      return S_OK([(secMsg, result['Message'])])
    return S_OK()

  types_logActionBundle = [(list, tuple)]

  def export_logActionBundle(self, secMsgList):
    """ Log a list of actions
    """
    errorList = []
    for secMsg in secMsgList:
      result = gSecurityFileLog.logAction(secMsg)
      if not result['OK']:
        errorList.append((secMsg, result['Message']))
    if errorList:
      return S_OK(errorList)
    return S_OK()
