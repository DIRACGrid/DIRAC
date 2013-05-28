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

__RCSID__ = "$Id $"

# #
# @file PutAndRegister.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:43:34
# @brief Definition of PutAndRegister class.

# # imports
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

########################################################################
class PutAndRegister( OperationHandlerBase ):
  """
  .. class:: PutAndRegister

  PutAndRegister operation handler
  """

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    :param str csPath: CS path for this handler
    """
    # # base class ctor
    OperationHandlerBase.__init__( self, operation, csPath )
    # # gMonitor stuff
    gMonitor.registerActivity( "PutAtt", "File put attempts",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "PutFail", "Failed file puts",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "PutOK", "Successful file puts",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterOK", "Successful file registrations",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterFail", "Failed file registrations",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )

  def __call__( self ):
    """ PutAndRegister operation processing """
    # # list of targetSEs

    targetSEs = self.operation.targetSEList

    if len( targetSEs ) != 1:
      self.log.error( "wrong value for TargetSE list = %s, should contain only one target!" % targetSEs )
      self.operation.Error = "Wrong parameters: TargetSE should contain only one targetSE"
      for opFile in self.operation:

        opFile.Status = "Failed"
        opFile.Error = "Wrong parameters: TargetSE should contain only one targetSE"

        gMonitor.addMark( "PutAtt", 1 )
        gMonitor.addMark( "PutFail", 1 )

      return S_ERROR( "TargetSE should contain only one target, got %s" % targetSEs )

    targetSE = targetSEs[0]
    targetWrite = self.rssSEStatus( targetSE, "WriteAccess" )
    if not targetWrite["OK"]:
      self.log.error( targetWrite["Message"] )
      for opFile in self.operation:
        opFile.Status = "Failed"
        opFile.Error = "Wrong parameters: %s" % targetWrite["Message"]
        gMonitor.addMark( "PutAtt", 1 )
        gMonitor.addMark( "PutFail", 1 )
      self.operation.Error = targetWrite["Message"]
      return S_OK()

    if not targetWrite["Value"]:
      self.operation.Error = "TargetSE %s is banned for writing"
      return S_ERROR( self.operation.Error )

    # # get waiting files
    waitingFiles = self.getWaitingFilesList()

    # # loop over files
    for opFile in waitingFiles:
      # # get LFN
      lfn = opFile.LFN
      self.log.info( "processing file %s" % lfn )
      gMonitor.addMark( "PutAtt", 1 )

      pfn = opFile.PFN
      guid = opFile.GUID
      checksum = opFile.Checksum

      # # call RM at least
      putAndRegister = self.replicaManager().putAndRegister( lfn,
                                                             pfn,
                                                             targetSE,
                                                             guid = guid,
                                                             checksum = checksum,
                                                             catalog = self.operation.Catalog )
      if not putAndRegister["OK"]:
        gMonitor.addMark( "PutFail", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "PutAndRegister" )
        self.log.error( "completely failed to put and register file: %s" % putAndRegister["Message"] )
        opFile.Error = str(putAndRegister["Message"])
        self.operation.Error = str(putAndRegister["Message"])
        continue

      putAndRegister = putAndRegister["Value"]

      if lfn in putAndRegister["Failed"]:
        gMonitor.addMark( "PutFail", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "PutAndRegister" )

        reason = putAndRegister["Failed"][lfn]
        self.log.error( "failed to put and register file %s at %s: %s" % ( lfn, targetSE, reason ) )
        opFile.Error = str( reason )
        self.operation.Error = str( reason )
        continue

      putAndRegister = putAndRegister["Successful"]
      if lfn in putAndRegister:

        if "put" not in putAndRegister[lfn]:

          gMonitor.addMark( "PutFail", 1 )
          self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "PutAndRegister" )

          self.log.info( "failed to put %s to %s" % ( lfn, targetSE ) )

          opFile.Error = "put failed"
          self.operation.Error = "put failed"
          continue

        if "register" not in putAndRegister[lfn]:

          gMonitor.addMark( "PutOK", 1 )
          gMonitor.addMark( "RegisterFail", 1 )

          self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "PutAndRegister" )
          self.dataLoggingClient().addFileRecord( lfn, "RegisterFail", targetSE, "", "PutAndRegister" )

          self.log.info( "put of %s to %s took %s seconds" % ( lfn, targetSE, putAndRegister[lfn]["put"] ) )
          self.log.error( "register of %s to %s failed" % ( lfn, targetSE ) )

          opFile.Error = "failed to register %s at %s" % ( lfn, targetSE )
          opFile.Status = "Failed"

          self.log.info( opFile.Error )
          self.addRegisterReplica( opFile, targetSE )
          continue

        gMonitor.addMark( "PutOK", 1 )
        gMonitor.addMark( "RegisterOK", 1 )

        self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "PutAndRegister" )
        self.dataLoggingClient().addFileRecord( lfn, "Register", targetSE, "", "PutAndRegister" )

        opFile.Status = "Done"
        for op in ( "put", "register" ):
          self.log.info( "%s of %s to %s took %s seconds" % ( op, lfn, targetSE, putAndRegister[lfn][op] ) )

    return S_OK()

  def addRegisterReplica( self, opFile, targetSE ):
    """ add RegisterReplica operation for file

    :param File opFile: operation file
    :param str targetSE: target SE
    """
    # # add RegisterReplica operation
    registerOperation = Operation()
    registerOperation.Type = "RegisterFile"
    registerOperation.TargetSE = targetSE

    registerFile = File()
    registerFile.LFN = opFile.LFN
    registerFile.PFN = opFile.PFN
    registerFile.GUID = opFile.GUID
    registerFile.Checksum = opFile.Checksum
    registerFile.ChecksumType = opFile.ChecksumType
    registerFile.Size = opFile.Size

    registerOperation.addFile( registerFile )
    self.request.insertAfter( registerOperation, self.operation )
    return S_OK()
