########################################################################
# $HeadURL $
# File: PhysicalRemoval.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/02 11:56:10
########################################################################
""" :mod: PhysicalRemoval
    =====================

    .. module: PhysicalRemoval
    :synopsis: PhysicalRemoval operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    PhysicalRemoval operation handler
"""

__RCSID__ = "$Id $"

# #
# @file PhysicalRemoval.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/02 11:56:22
# @brief Definition of PhysicalRemoval class.

# # imports
import os
# # from DIRAC
from DIRAC import S_OK, gMonitor
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase  import DMSRequestOperationsBase
from DIRAC.Resources.Storage.StorageElement import StorageElement

########################################################################
class PhysicalRemoval( DMSRequestOperationsBase ):
  """
  .. class:: PhysicalRemoval

  """

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    :param str csPath: cs config path
    """
    DMSRequestOperationsBase.__init__( self, operation, csPath )
    # # gMonitor stuff
    gMonitor.registerActivity( "PhysicalRemovalAtt", "Physical file removals attempted",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "PhysicalRemovalOK", "Successful file physical removals",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "PhysicalRemovalFail", "Failed file physical removals",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "PhysicalRemovalSize", "Physically removed size",
                               "RequestExecutingAgent", "Bytes", gMonitor.OP_ACUM )

  def __call__( self ):
    """ perform physical removal operation """
    bannedTargets = self.checkSEsRSS( access = 'RemoveAccess' )
    if not bannedTargets['OK']:
      gMonitor.addMark( "PhysicalRemovalAtt" )
      gMonitor.addMark( "PhysicalRemovalFail" )
      return bannedTargets

    if bannedTargets['Value']:
      return S_OK( "%s targets are banned for removal" % ",".join( bannedTargets['Value'] ) )

    # # get waiting files
    waitingFiles = self.getWaitingFilesList()
    # # prepare pfn dict
    toRemoveDict = dict( [ ( opFile.PFN, opFile ) for opFile in waitingFiles ] )

    targetSEs = self.operation.targetSEList
    gMonitor.addMark( "PhysicalRemovalAtt", len( toRemoveDict ) * len( targetSEs ) )

    # # keep errors dict
    removalStatus = dict.fromkeys( toRemoveDict.keys(), None )
    for pfn in removalStatus:
      removalStatus[pfn] = dict.fromkeys( targetSEs, "" )

    for targetSE in targetSEs:

      self.log.info( "removing files from %s" % targetSE )

      # # 1st - bulk removal
      bulkRemoval = self.bulkRemoval( toRemoveDict, targetSE )
      if not bulkRemoval["OK"]:
        self.log.error( bulkRemoval["Message"] )
        self.operation.Error = bulkRemoval["Message"]
        return bulkRemoval

      bulkRemoval = bulkRemoval["Value"]

      for pfn, opFile in toRemoveDict.items():
        removalStatus[pfn][targetSE] = bulkRemoval["Failed"].get( pfn, "" )
        opFile.Error = removalStatus[pfn][targetSE]

      # # 2nd - single file removal
      toRetry = dict( [ ( pfn, opFile ) for pfn, opFile in toRemoveDict.items() if pfn in bulkRemoval["Failed"] ] )
      for pfn, opFile in toRetry.items():
        self.singleRemoval( opFile, targetSE )
        if not opFile.Error:
          removalStatus[pfn][targetSE] = ""
        else:
          gMonitor.addMark( "PhysicalRemovalFail", 1 )
          removalStatus[pfn][targetSE] = opFile.Error

    # # update file status for waiting files
    failed = 0
    for opFile in self.operation:
      if opFile.Status == "Waiting":
        errors = [ error for error in removalStatus[opFile.PFN].values() if error.strip() ]
        if errors:
          failed += 1
          opFile.Error = ",".join( errors )
          if "Write access not permitted for this credential" in opFile.Error:
            opFile.Status = "Failed"
            gMonitor.addMark( "PhysicalRemovalFail", len( errors ) )
            continue
        gMonitor.addMark( "PhysicalRemovalOK", len( targetSEs ) )
        gMonitor.addMark( "PhysicalRemovalSize", opFile.Size * len( targetSEs ) )
        opFile.Status = "Done"

    if failed:
      self.operation.Error = "failed to remove %s files" % failed

    return S_OK()

  def bulkRemoval( self, toRemoveDict, targetSE ):
    """ bulk removal of pfns from :targetSE:

    :param dict toRemoveDict: { pfn : opFile, ... }
    :param str targetSE: target SE name
    """


    bulkRemoval = StorageElement( targetSE ).removeFile( toRemoveDict )
    return bulkRemoval

  def singleRemoval( self, opFile, targetSE ):
    """ remove single file from :targetSE: """
    proxyFile = None
    if "Write access not permitted for this credential" in opFile.Error:
      # # not a DataManger? set status to failed and return
      if "DataManager" not in self.shifter:
        opFile.Status = "Failed"
      elif not opFile.LFN:
        opFile.Error = "LFN not set"
        opFile.Status = "Failed"
      else:
        # #  you're a data manager - save current proxy and get a new one for LFN and retry
        saveProxy = os.environ["X509_USER_PROXY"]
        try:
          proxyFile = self.getProxyForLFN( opFile.LFN )
          if not proxyFile["OK"]:
            opFile.Error = proxyFile["Message"]
          else:
            proxyFile = proxyFile["Value"]
            removeFile = StorageElement( targetSE ).removeFile( opFile.PFN )
            if not removeFile["OK"]:
              opFile.Error = removeFile["Message"]
            else:
              removeFile = removeFile["Value"]
              if opFile.LFN in removeFile["Failed"]:
                opFile.Error = removeFile["Failed"][opFile.LFN]
              else:
                # # reset error - replica has been removed this time
                opFile.Error = ""
        finally:
          if proxyFile:
            os.unlink( proxyFile )
          # # put back request owner proxy to env
          os.environ["X509_USER_PROXY"] = saveProxy
    return S_OK( opFile )
