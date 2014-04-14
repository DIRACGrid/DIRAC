########################################################################
# $HeadURL $
# File: RemoveReplica.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 07:45:06
########################################################################

""" :mod: RemoveReplica
    ===================

    .. module: RemoveReplica
    :synopsis: removeReplica operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    RemoveReplica operation handler
"""

__RCSID__ = "$Id $"

# #
# @file RemoveReplica.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:45:17
# @brief Definition of RemoveReplica class.

# # imports
import os
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase   import DMSRequestOperationsBase

########################################################################
class RemoveReplica( DMSRequestOperationsBase ):
  """
  .. class:: RemoveReplica

  """

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: operation to execute
    :param str csPath: CS path for this handler
    """
    # # base class ctor
    DMSRequestOperationsBase.__init__( self, operation, csPath )
    # # gMonitor stuff
    gMonitor.registerActivity( "RemoveReplicaAtt", "Replica removals attempted",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveReplicaeOK", "Successful replica removals",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveReplicaFail", "Failed replica removals",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )

  def __call__( self ):
    """ remove replicas """
    # # prepare list of targetSEs
    targetSEs = self.operation.targetSEList
    # # get waiting files
    waitingFiles = self.getWaitingFilesList()
    # # and prepare dict
    toRemoveDict = dict( [ ( opFile.LFN, opFile ) for opFile in waitingFiles ] )

    self.log.info( "todo: %s replicas to delete from %s sites" % ( len( toRemoveDict ), len( targetSEs ) ) )
    gMonitor.addMark( "RemoveReplicaAtt", len( toRemoveDict ) * len( targetSEs ) )

    # # check targetSEs for removal
    bannedTargets = self.checkSEsRSS( access = 'RemoveAccess' )
    if not bannedTargets['OK']:
      gMonitor.addMark( "RemoveReplicaAtt" )
      gMonitor.addMark( "RemoveReplicaFail" )
      return bannedTargets

    if bannedTargets['Value']:
      return S_OK( "%s targets are banned for removal" % ",".join( bannedTargets['Value'] ) )

    # # keep status for each targetSE
    removalStatus = dict.fromkeys( toRemoveDict.keys(), None )
    for lfn in removalStatus:
      removalStatus[lfn] = dict.fromkeys( targetSEs, None )

    # # loop over targetSEs
    for targetSE in targetSEs:

      self.log.info( "removing replicas at %s" % targetSE )

      # # 1st step - bulk removal
      bulkRemoval = self.bulkRemoval( toRemoveDict, targetSE )
      if not bulkRemoval["OK"]:
        self.log.error( bulkRemoval["Message"] )
        return bulkRemoval
      bulkRemoval = bulkRemoval["Value"]

      # # update removal status for successful files
      removalOK = [ opFile for opFile in bulkRemoval.values() if not opFile.Error ]
      for opFile in removalOK:
        removalStatus[opFile.LFN][targetSE] = ""
      gMonitor.addMark( "RemoveReplicaOK", len( removalOK ) )

      # # 2nd step - process the rest again
      toRetry = dict( [ ( lfn, opFile ) for lfn, opFile in bulkRemoval.items() if opFile.Error ] )
      for lfn, opFile in toRetry.items():
        self.singleRemoval( opFile, targetSE )
        if not opFile.Error:
          gMonitor.addMark( "RemoveReplicaOK", 1 )
          removalStatus[lfn][targetSE] = ""
        else:
          gMonitor.addMark( "RemoveReplicaFail", 1 )
          removalStatus[lfn][targetSE] = opFile.Error

    # # update file status for waiting files
    failed = 0
    for opFile in self.operation:
      if opFile.Status == "Waiting":
        errors = list( set( [ error for error in removalStatus[lfn].values() if error ] ) )
        if errors:
          opFile.Error = ",".join( errors )
          # This seems to be the only offending error
          if "Write access not permitted for this credential" in opFile.Error:
            failed += 1
            continue
        opFile.Status = "Done"

    if failed:
      self.operation.Error = "failed to remove %s replicas" % failed

    return S_OK()

  def bulkRemoval( self, toRemoveDict, targetSE ):
    """ remove replicas :toRemoveDict: at :targetSE:

    :param dict toRemoveDict: { lfn: opFile, ... }
    :param str targetSE: target SE name
    :return: toRemoveDict with updated errors
    """
    removeReplicas = self.dm.removeReplica( targetSE, toRemoveDict.keys() )
    if not removeReplicas["OK"]:
      for opFile in toRemoveDict.values():
        opFile.Error = removeReplicas["Message"]
      return S_ERROR( removeReplicas["Message"] )
    removeReplicas = removeReplicas["Value"]
    # # filter out failed
    for lfn, opFile in toRemoveDict.items():
      if lfn in removeReplicas["Failed"]:
        opFile.Error = str( removeReplicas["Failed"][lfn] )
    return S_OK( toRemoveDict )

  def singleRemoval( self, opFile, targetSE ):
    """ remove opFile replica from targetSE

    :param File opFile: File instance
    :param str targetSE: target SE name
    """
    proxyFile = None
    if "Write access not permitted for this credential" in opFile.Error:
      # # not a DataManger? set status to failed and return
      if "DataManager" in self.shifter:
        # #  you're a data manager - save current proxy and get a new one for LFN and retry
        saveProxy = os.environ["X509_USER_PROXY"]
        try:
          proxyFile = self.getProxyForLFN( opFile.LFN )
          if not proxyFile["OK"]:
            opFile.Error = proxyFile["Message"]
          else:
            proxyFile = proxyFile["Value"]
            removeReplica = self.dm.removeReplica( targetSE, opFile.LFN )
            if not removeReplica["OK"]:
              opFile.Error = removeReplica["Message"]
            else:
              removeReplica = removeReplica["Value"]
              if opFile.LFN in removeReplica["Failed"]:
                opFile.Error = removeReplica["Failed"][opFile.LFN]
              else:
                # # reset error - replica has been removed this time
                opFile.Error = ""
        finally:
          if proxyFile:
            os.unlink( proxyFile )
          # # put back request owner proxy to env
          os.environ["X509_USER_PROXY"] = saveProxy
    return S_OK( opFile )
