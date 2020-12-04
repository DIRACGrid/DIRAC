########################################################################
# $HeadURL $
# File: PutAndRegister.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 07:43:24
########################################################################

""" :mod: PutAndRegister

    ====================

    .. module: PutAndRegister

    :synopsis: putAndRegister operation handler

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    PutAndRegister operation handler
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id $"

# #
# @file PutAndRegister.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:43:34
# @brief Definition of PutAndRegister class.

# # imports
from DIRAC import S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase import DMSRequestOperationsBase
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

########################################################################


class PutAndRegister(DMSRequestOperationsBase):
  """
  .. class:: PutAndRegister

  PutAndRegister operation handler.

  This takes a local file and put it on a StorageElement before registering it in a catalog

  """

  def __init__(self, operation=None, csPath=None):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    :param str csPath: CS path for this handler
    """
    # # base classes ctor
    super(PutAndRegister, self).__init__(operation, csPath)

    self.dm = DataManager()

  def __call__(self):
    """ PutAndRegister operation processing """

    # The flag  'rmsMonitoring' is set by the RequestTask and is False by default.
    # Here we use 'createRMSRecord' to create the ES record which is defined inside OperationHandlerBase.
    if self.rmsMonitoring:
      self.rmsMonitoringReporter = MonitoringReporter(monitoringType="RMSMonitoring")
    else:
      # # gMonitor stuff
      gMonitor.registerActivity("PutAtt", "File put attempts",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
      gMonitor.registerActivity("PutFail", "Failed file puts",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
      gMonitor.registerActivity("PutOK", "Successful file puts",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
      gMonitor.registerActivity("RegisterOK", "Successful file registrations",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
      gMonitor.registerActivity("RegisterFail", "Failed file registrations",
                                "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)

    # # list of targetSEs
    targetSEs = self.operation.targetSEList

    if len(targetSEs) != 1:
      self.log.error("Wrong value for TargetSE list, should contain only one target!", "%s" % targetSEs)
      self.operation.Error = "Wrong parameters: TargetSE should contain only one targetSE"
      for opFile in self.operation:

        opFile.Status = "Failed"
        opFile.Error = "Wrong parameters: TargetSE should contain only one targetSE"

        if not self.rmsMonitoring:
          gMonitor.addMark("PutAtt", 1)
          gMonitor.addMark("PutFail", 1)

      if self.rmsMonitoring:
        for status in ["Attempted", "Failed"]:
          self.rmsMonitoringReporter.addRecord(
              self.createRMSRecord(status, len(self.operation))
          )
          self.rmsMonitoringReporter.commit()

      return S_ERROR("TargetSE should contain only one target, got %s" % targetSEs)

    targetSE = targetSEs[0]
    bannedTargets = self.checkSEsRSS(targetSE)
    if not bannedTargets['OK']:
      if self.rmsMonitoring:
        for status in ["Attempted", "Failed"]:
          self.rmsMonitoringReporter.addRecord(
              self.createRMSRecord(status, len(self.operation))
          )
        self.rmsMonitoringReporter.commit()
      else:
        gMonitor.addMark("PutAtt")
        gMonitor.addMark("PutFail")
      return bannedTargets

    if bannedTargets['Value']:
      return S_OK("%s targets are banned for writing" % ",".join(bannedTargets['Value']))

    # # get waiting files
    waitingFiles = self.getWaitingFilesList()

    if self.rmsMonitoring:
      self.rmsMonitoringReporter.addRecord(
          self.createRMSRecord("Attempted", len(waitingFiles))
      )

    # # loop over files
    for opFile in waitingFiles:
      # # get LFN
      lfn = opFile.LFN
      self.log.info("processing file %s" % lfn)

      if not self.rmsMonitoring:
        gMonitor.addMark("PutAtt", 1)

      pfn = opFile.PFN
      guid = opFile.GUID
      checksum = opFile.Checksum

      # # call DataManager passing a list of requested catalogs
      catalogs = self.operation.Catalog
      if catalogs:
        catalogs = [cat.strip() for cat in catalogs.split(',')]
      putAndRegister = DataManager(catalogs=catalogs).putAndRegister(lfn,
                                                                     pfn,
                                                                     targetSE,
                                                                     guid=guid,
                                                                     checksum=checksum)
      if not putAndRegister["OK"]:
        if self.rmsMonitoring:
          self.rmsMonitoringReporter.addRecord(
              self.createRMSRecord("Failed", 1)
          )
        else:
          gMonitor.addMark("PutFail", 1)
#         self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "PutAndRegister" )
        self.log.error("Completely failed to put and register file", putAndRegister["Message"])
        opFile.Error = str(putAndRegister["Message"])
        self.operation.Error = str(putAndRegister["Message"])
        continue

      putAndRegister = putAndRegister["Value"]

      if lfn in putAndRegister["Failed"]:
        if self.rmsMonitoring:
          self.rmsMonitoringReporter.addRecord(
              self.createRMSRecord("Failed", 1)
          )
        else:
          gMonitor.addMark("PutFail", 1)
#         self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "PutAndRegister" )

        reason = putAndRegister["Failed"][lfn]
        self.log.error("Failed to put and register file", " %s at %s: %s" % (lfn, targetSE, reason))
        opFile.Error = str(reason)
        self.operation.Error = str(reason)
        continue

      putAndRegister = putAndRegister["Successful"]
      if lfn in putAndRegister:

        if "put" not in putAndRegister[lfn]:

          if self.rmsMonitoring:
            self.rmsMonitoringReporter.addRecord(
                self.createRMSRecord("Failed", 1)
            )
          else:
            gMonitor.addMark("PutFail", 1)
#           self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "PutAndRegister" )

          self.log.info("failed to put %s to %s" % (lfn, targetSE))

          opFile.Error = "put failed"
          self.operation.Error = "put failed"
          continue

        if "register" not in putAndRegister[lfn]:

          if self.rmsMonitoring:
            self.rmsMonitoringReporter.addRecord(
                self.createRMSRecord("Failed", 1)
            )
          else:
            gMonitor.addMark("PutOK", 1)
            gMonitor.addMark("RegisterFail", 1)

#           self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "PutAndRegister" )
#           self.dataLoggingClient().addFileRecord( lfn, "RegisterFail", targetSE, "", "PutAndRegister" )

          self.log.info("put of %s to %s took %s seconds" % (lfn, targetSE, putAndRegister[lfn]["put"]))
          self.log.error("Register of lfn to SE failed", "%s to %s" % (lfn, targetSE))

          opFile.Error = "failed to register %s at %s" % (lfn, targetSE)
          opFile.Status = "Failed"

          self.log.info(opFile.Error)
          registerOperation = self.getRegisterOperation(opFile, targetSE)
          self.request.insertAfter(registerOperation, self.operation)
          continue

        if self.rmsMonitoring:
          self.rmsMonitoringReporter.addRecord(
              self.createRMSRecord("Successful", 1)
          )
        else:
          gMonitor.addMark("PutOK", 1)
          gMonitor.addMark("RegisterOK", 1)

#         self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "PutAndRegister" )
#         self.dataLoggingClient().addFileRecord( lfn, "Register", targetSE, "", "PutAndRegister" )

        opFile.Status = "Done"
        for op in ("put", "register"):
          self.log.info("%s of %s to %s took %s seconds" % (op, lfn, targetSE, putAndRegister[lfn][op]))

    if self.rmsMonitoring:
      self.rmsMonitoringReporter.commit()

    return S_OK()
