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
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase  import DMSRequestOperationsBase
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

########################################################################
class PutAndRegister( DMSRequestOperationsBase ):
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
    # # base classes ctor
    super( PutAndRegister, self ).__init__( operation, csPath )
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

    self.dm = DataManager()

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
    bannedTargets = self.checkSEsRSS( targetSE )
    if not bannedTargets['OK']:
      gMonitor.addMark( "PutAtt" )
      gMonitor.addMark( "PutFail" )
      return bannedTargets

    if bannedTargets['Value']:
      return S_OK( "%s targets are banned for writing" % ",".join( bannedTargets['Value'] ) )

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
      putAndRegister = DataManager( catalogs = self.operation.Catalog ).putAndRegister( lfn,
                                                             pfn,
                                                             targetSE,
                                                             guid = guid,
                                                             checksum = checksum )
      if not putAndRegister["OK"]:
        gMonitor.addMark( "PutFail", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "PutAndRegister" )
        self.log.error( "completely failed to put and register file: %s" % putAndRegister["Message"] )
        opFile.Error = str( putAndRegister["Message"] )
        self.operation.Error = str( putAndRegister["Message"] )
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
          registerOperation = self.getRegisterOperation( opFile, targetSE )
          self.request.insertAfter( registerOperation, self.operation )
          continue

        gMonitor.addMark( "PutOK", 1 )
        gMonitor.addMark( "RegisterOK", 1 )

        self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "PutAndRegister" )
        self.dataLoggingClient().addFileRecord( lfn, "Register", targetSE, "", "PutAndRegister" )

        opFile.Status = "Done"
        for op in ( "put", "register" ):
          self.log.info( "%s of %s to %s took %s seconds" % ( op, lfn, targetSE, putAndRegister[lfn][op] ) )

    return S_OK()
