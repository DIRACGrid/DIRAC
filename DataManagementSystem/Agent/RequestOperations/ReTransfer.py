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
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation

########################################################################
class ReTransfer( BaseOperation ):
  """
  .. class:: ReTransfer


  online ReTransfer operation handler
  """

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: operation to execute
    :param str csPath: CS path for this handler
    """
    # # base class ctor
    BaseOperation.__init__( self, operation, csPath )
    # # gMonitor stuff
    gMonitor.registerActivity( "FileReTransferAtt", "File retransfers attempted",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FileReTransferOK", "File retransfers successful",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FileReTransferFail", "File retransfers failed",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )

  def __call__( self ):
    """ reTransfer operation execution """
    # # list of targetSEs
    targetSEs = self.operation.targetSEList
    # # get waiting files
    waitingFiles = self.getWaitingFilesList()
    # # prepare waiting files
    toRetransfer = dict( [ ( opFile.PFN, opFile ) for opFile in waitingFiles ] )

    gMonitor.addMark( "FileReTransferAtt", len( toRetransfer ) )

    if len( targetSEs ) != 1:
      error = "only one TargetSE allowed, got %s" % len( targetSEs )
      for opFile in toRetransfer.values():
        opFile.Error = error
        opFile.Status = "Failed"
      self.operation.Error = error
      gMonitor.addMark( "FileReTransferFail", len( toRetransfer ) )
      return S_ERROR( error )

    # # check targetSEs for removal
    targetSE = targetSEs[0]
    writeStatus = self.rssSEStatus( targetSE, "Write" )
    if not writeStatus["OK"]:
      self.log.error( writeStatus["Message"] )
      for opFile in self.operation:
        opFile.Error = "unknown targetSE: %s" % targetSE
        opFile.Status = "Failed"
      self.operation.Error = "unknown targetSE: %s" % targetSE
      gMonitor.addMark( "FileReTransferFail", len( toRetransfer ) )
      return S_ERROR( self.operation.Error )

    if not writeStatus["Value"]:
      self.log.error( "TargetSE %s in banned for writing right now" % targetSE )
      self.operation.Error += "banned targetSE: %s;" % targetSE
      gMonitor.addMark( "FileReTransferFail", len( toRetransfer ) )
      return S_ERROR( self.operation.Error )

    for opFile in toRetransfer.values():
      reTransfer = self.replicaManager().onlineRetransfer( targetSE, opFile.PFN )
      if not reTransfer["OK"]:
        opFile.Error = reTransfer["Message"]
        self.log.error( "%s retransfer failed: %s" % opFile.Error )
        gMonitor.addMark( "FileReTransferFail", 1 )
        continue
      reTransfer = reTransfer["Value"]
      if opFile.PFN in reTransfer["Failed"]:
        opFile.Error = reTransfer["Failed"][opFile.PFN]
        self.log.error( "%s retransfer failed: %s" % opFile.Error )
        gMonitor.addMark( "FileReTransferFail", 1 )
        continue
      opFile.Status = "Done"
      self.log.info( "%s retransfer done" % opFile.LFN )
      gMonitor.addMark( "FileReTransferOK", 1 )

    return S_OK()
