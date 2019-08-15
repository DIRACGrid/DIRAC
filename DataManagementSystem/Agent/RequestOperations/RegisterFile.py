########################################################################
# File: RegisterOperation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/19 13:55:14
########################################################################
""" :mod: RegisterFile

    ==================

    .. module: RegisterFile

    :synopsis: register operation handler

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    RegisterFile operation handler
"""

__RCSID__ = "$Id $"

# #
# @file RegisterOperation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/19 13:55:24
# @brief Definition of RegisterOperation class.

# # imports
import time
import datetime
import socket
from DIRAC import S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

########################################################################


class RegisterFile(OperationHandlerBase):
  """
  .. class:: RegisterOperation

  RegisterFile operation handler

  :param self: self reference
  :param ~DIRAC.RequestManagementSystem.Client.Operation.Operation operation: Operation instance
  :param str csPath: CS path for this handler

  """

  def __init__(self, operation=None, csPath=None):
    """c'tor

    """
    OperationHandlerBase.__init__(self, operation, csPath)

    # Check whether the ES flag is enabled so we can send the data accordingly.
    self.rmsMonitoring = Operations().getValue("EnableActivityMonitoring", False)

    if self.rmsMonitoring:
      self.rmsMonitoringReporter = MonitoringReporter(monitoringType="RMSMonitoring")
    else:
      # # RegisterFile specific monitor info
      gMonitor.registerActivity("RegisterAtt", "Attempted file registrations",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
      gMonitor.registerActivity("RegisterOK", "Successful file registrations",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
      gMonitor.registerActivity("RegisterFail", "Failed file registrations",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)

  def __call__(self):
    """ call me maybe """
    # # counter for failed files
    failedFiles = 0
    # # catalog(s) to use
    catalogs = self.operation.Catalog
    if catalogs:
      catalogs = [cat.strip() for cat in catalogs.split(',')]
    dm = DataManager(catalogs=catalogs)
    # # get waiting files
    waitingFiles = self.getWaitingFilesList()
    # # loop over files
    for opFile in waitingFiles:

      if self.rmsMonitoring:
        self.rmsMonitoringReporter.addRecord({
            "timestamp": time.mktime(datetime.datetime.utcnow().timetuple()),
            "host": socket.getfqdn(),
            "objectType": "File",
            "operationType": self.operation.Type,
            "objectID": opFile.FileID,
            "parentID": self.operation.OperationID,
            "status": "FileAttempted",
            "nbObject": 1
        })
      else:
        gMonitor.addMark("RegisterAtt", 1)

      # # get LFN
      lfn = opFile.LFN
      # # and others
      fileTuple = (lfn, opFile.PFN, opFile.Size, self.operation.targetSEList[0], opFile.GUID, opFile.Checksum)
      # # call DataManager
      registerFile = dm.registerFile(fileTuple)
      # # check results
      if not registerFile["OK"] or lfn in registerFile["Value"]["Failed"]:

        if self.rmsMonitoring:
          self.rmsMonitoringReporter.addRecord({
              "timestamp": time.mktime(datetime.datetime.utcnow().timetuple()),
              "host": socket.getfqdn(),
              "objectType": "File",
              "operationType": self.operation.Type,
              "objectID": opFile.FileID,
              "parentID": self.operation.OperationID,
              "status": "FileFailed",
              "nbObject": 1
          })
        else:
          gMonitor.addMark("RegisterFail", 1)
#        self.dataLoggingClient().addFileRecord( lfn, "RegisterFail", ','.join( catalogs ) if catalogs else "all catalogs", "", "RegisterFile" )

        reason = str(registerFile.get("Message", registerFile.get("Value", {}).get("Failed", {}).get(lfn, 'Unknown')))
        errorStr = "failed to register LFN"
        opFile.Error = "%s: %s" % (errorStr, reason)
        if 'GUID already registered' in reason:
          opFile.Status = 'Failed'
          self.log.error(errorStr, "%s: %s" % (lfn, reason))
        elif 'File already registered with no replicas' in reason:
          self.log.warn(errorStr, "%s: %s, will remove it and retry" % (lfn, reason))
          dm.removeFile(lfn)
        else:
          self.log.warn(errorStr, "%s: %s" % (lfn, reason))
        failedFiles += 1

      else:

        if self.rmsMonitoring:
          self.rmsMonitoringReporter.addRecord({
              "timestamp": time.mktime(datetime.datetime.utcnow().timetuple()),
              "host": socket.getfqdn(),
              "objectType": "File",
              "operationType": self.operation.Type,
              "objectID": opFile.FileID,
              "parentID": self.operation.OperationID,
              "status": "FileSuccessful",
              "nbObject": 1
          })
        else:
          gMonitor.addMark("RegisterOK", 1)
#        self.dataLoggingClient().addFileRecord( lfn, "Register", ','.join( catalogs ) if catalogs else "all catalogs", "", "RegisterFile" )

        self.log.verbose("file %s has been registered at %s" %
                         (lfn, ','.join(catalogs) if catalogs else "all catalogs"))
        opFile.Status = "Done"

    if self.rmsMonitoring:
      self.rmsMonitoringReporter.commit()

    # # final check
    if failedFiles:
      self.log.warn("all files processed, %s files failed to register" % failedFiles)
      self.operation.Error = "some files failed to register"
      return S_ERROR(self.operation.Error)

    return S_OK()
