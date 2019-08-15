########################################################################
# $HeadURL $
# File: ReTransfer.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/02 14:24:21
########################################################################
""" :mod: ReTransfer

    ================


    .. module: ReTransfer

    :synopsis: ReTransfer Operation handler

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    ReTransfer Operation handler
"""

__RCSID__ = "$Id $"

# #
# @file ReTransfer.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/02 14:24:31
# @brief Definition of ReTransfer class.

# # imports
import time
import datetime
import socket
from DIRAC import S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase import DMSRequestOperationsBase
from DIRAC.Resources.Storage.StorageElement import StorageElement

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

########################################################################


class ReTransfer(DMSRequestOperationsBase):
  """
  .. class:: ReTransfer


  online ReTransfer operation handler

  :param self: self reference
  :param ~DIRAC.RequestManagementSystem.Client.Operation.Operation operation: Operation instance
  :param str csPath: CS path for this handler

  """

  def __init__(self, operation=None, csPath=None):
    """c'tor

    """
    # # base class ctor
    DMSRequestOperationsBase.__init__(self, operation, csPath)

    # Check whether the ES flag is enabled so we can send the data accordingly.
    self.rmsMonitoring = Operations().getValue("EnableActivityMonitoring", False)

    if self.rmsMonitoring:
      self.rmsMonitoringReporter = MonitoringReporter(monitoringType="RMSMonitoring")
    else:
      # # gMonitor stuff
      gMonitor.registerActivity("FileReTransferAtt", "File retransfers attempted",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
      gMonitor.registerActivity("FileReTransferOK", "File retransfers successful",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
      gMonitor.registerActivity("FileReTransferFail", "File retransfers failed",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)

  def __call__(self):
    """ reTransfer operation execution """
    # # list of targetSEs
    targetSEs = self.operation.targetSEList
    # # check targetSEs for removal
    targetSE = targetSEs[0]
    bannedTargets = self.checkSEsRSS(targetSE)
    if not bannedTargets['OK']:
      if self.rmsMonitoring:
        for opFile in self.operation:
          for status in ["FileAttempted", "FileFailed"]:
            self.rmsMonitoringReporter.addRecord({
                "timestamp": time.mktime(datetime.datetime.utcnow().timetuple()),
                "host": socket.getfqdn(),
                "objectType": "File",
                "operationType": self.operation.Type,
                "objectID": opFile.FileID,
                "parentID": self.operation.OperationID,
                "status": status,
                "nbObject": 1
            })
        self.rmsMonitoringReporter.commit()
      else:
        gMonitor.addMark("FileReTransferAtt")
        gMonitor.addMark("FileReTransferFail")
      return bannedTargets

    if bannedTargets['Value']:
      return S_OK("%s targets are banned for writing" % ",".join(bannedTargets['Value']))

    # # get waiting files
    waitingFiles = self.getWaitingFilesList()
    # # prepare waiting files
    toRetransfer = dict([(opFile.PFN, opFile) for opFile in waitingFiles])

    if self.rmsMonitoring:
      for opFile in toRetransfer.values():
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
      gMonitor.addMark("FileReTransferAtt", len(toRetransfer))

    if len(targetSEs) != 1:
      error = "only one TargetSE allowed, got %d" % len(targetSEs)
      for opFile in toRetransfer.values():
        opFile.Error = error
        opFile.Status = "Failed"
      self.operation.Error = error

      if self.rmsMonitoring:
        for opFile in toRetransfer.values():
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
        self.rmsMonitoringReporter.commit()
      else:
        gMonitor.addMark("FileReTransferFail", len(toRetransfer))

      return S_ERROR(error)

    se = StorageElement(targetSE)
    for opFile in toRetransfer.values():
      # template for file object
      if self.rmsMonitoring:
        fileObj = {
            "host": socket.getfqdn(),
            "objectType": "File",
            "operationType": self.operation.Type,
            "objectID": opFile.FileID,
            "parentID": self.operation.OperationID,
            "nbObject": 1
        }

      reTransfer = se.retransferOnlineFile(opFile.LFN)
      if not reTransfer["OK"]:
        opFile.Error = reTransfer["Message"]
        self.log.error("Retransfer failed", opFile.Error)

        if self.rmsMonitoring:
          fileObj["timestamp"] = time.mktime(datetime.datetime.utcnow().timetuple())
          fileObj["status"] = "FileFailed"
          self.rmsMonitoringReporter.addRecord(fileObj)
        else:
          gMonitor.addMark("FileReTransferFail", 1)

        continue
      reTransfer = reTransfer["Value"]
      if opFile.LFN in reTransfer["Failed"]:
        opFile.Error = reTransfer["Failed"][opFile.LFN]
        self.log.error("Retransfer failed", opFile.Error)

        if self.rmsMonitoring:
          fileObj["timestamp"] = time.mktime(datetime.datetime.utcnow().timetuple())
          fileObj["status"] = "FileFailed"
          self.rmsMonitoringReporter.addRecord(fileObj)
        else:
          gMonitor.addMark("FileReTransferFail", 1)

        continue
      opFile.Status = "Done"
      self.log.info("%s retransfer done" % opFile.LFN)

      if self.rmsMonitoring:
        fileObj["timestamp"] = time.mktime(datetime.datetime.utcnow().timetuple())
        fileObj["status"] = "FileSuccessful"
        self.rmsMonitoringReporter.addRecord(fileObj)
      else:
        gMonitor.addMark("FileReTransferOK", 1)

    if self.rmsMonitoring:
      self.rmsMonitoringReporter.commit()

    return S_OK()
