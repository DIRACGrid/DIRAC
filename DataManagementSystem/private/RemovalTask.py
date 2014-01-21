########################################################################
# $HeadURL $
# File: RemovalTask.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/10/25 07:52:37
########################################################################
""" :mod: RemovalTask
    =======================

    .. module: RemovalTask
    :synopsis: removal requests processing
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    removal requests processing

    :deprecated:
"""
__RCSID__ = "$Id $"
# #
# @file RemoveTask.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/10/25 07:52:50
# @brief Definition of RemoveTask class.

# # imports
import re
import os
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Resources.Utilities import Utils
from DIRAC.Resources.Storage.StorageElement import StorageElement

########################################################################
class RemovalTask( RequestTask ):
  """
  .. class:: RemovalTask

  supported operations are:
  - physicalRemoval - remove PFN from TargetSE
  - removeFile - completely remove LFNs from catalogues and SEs
  - replicaRemoval - remove replicas at specified TargetSEs
  - reTransfer - delete file at source and issue a transfer request using DataManager::onlineRetransfer
  """
  def __init__( self, *args, **kwargs ):
    """c'tor

    :param self: self reference
    :param tuple args: anonymouse args tuple
    :param dict kwagrs: named args dict
    """
    RequestTask.__init__( self, *args, **kwargs )
    self.setRequestType( "removal" )
    # # operation handlers physicalRemoval
    self.addOperationAction( "physicalRemoval", self.physicalRemoval )
    self.addOperationAction( "removeFile", self.removeFile )
    self.addOperationAction( "replicaRemoval", self.replicaRemoval )
    self.addOperationAction( "reTransfer", self.reTransfer )

  def getProxyForLFN( self, lfn ):
    """ get proxy for LFN

    :param self: self reference
    :param str lfn: LFN
    """
    dirMeta = Utils.executeSingleFileOrDirWrapper( self.fc.getDirectoryMetadata( lfn ) )
    if not dirMeta["OK"]:
      return dirMeta
    dirMeta = dirMeta["Value"]

    ownerRole = "/%s" % dirMeta["OwnerRole"] if not dirMeta["OwnerRole"].startswith( "/" ) else dirMeta["OwnerRole"]
    ownerDN = dirMeta["OwnerDN"]

    ownerProxy = None
    # Execute with server certificate
    gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'true' )
    for ownerGroup in getGroupsWithVOMSAttribute( ownerRole ):
      vomsProxy = gProxyManager.downloadVOMSProxy( ownerDN, ownerGroup, limited = True,
                                                   requiredVOMSAttribute = ownerRole )
      if not vomsProxy["OK"]:
        self.debug( "getProxyForLFN: failed to get VOMS proxy for %s role=%s: %s" % ( ownerDN,
                                                                                      ownerRole,
                                                                                      vomsProxy["Message"] ) )
        continue
      ownerProxy = vomsProxy["Value"]
      self.debug( "getProxyForLFN: got proxy for %s@%s [%s]" % ( ownerDN, ownerGroup, ownerRole ) )
      break

    gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'false' )
    if not ownerProxy:
      return S_ERROR( "Unable to get owner proxy" )

    dumpToFile = ownerProxy.dumpAllToFile()
    if not dumpToFile["OK"]:
      self.error( "getProxyForLFN: error dumping proxy to file: %s" % dumpToFile["Message"] )
      return dumpToFile
    dumpToFile = dumpToFile["Value"]
    os.environ["X509_USER_PROXY"] = dumpToFile

    return S_OK()

  def removeFile( self, index, requestObj, subRequestAttrs, subRequestFiles ):
    """ action for 'removeFile' operation

    :param self: self reference
    :param int index: subRequest index in execution order
    :param RequestContainer requestObj: request
    :param dict subRequestAttrs: subRequest's attributes
    :param dict subRequestFiles: subRequest's files
    """
    self.info( "removeFile: processing subrequest %s" % index )
    if requestObj.isSubRequestEmpty( index, "removal" )["Value"]:
      self.info( "removeFile: subrequest %s is empty, setting its status to 'Done'" % index )
      requestObj.setSubRequestStatus( index, "removal", "Done" )
      return S_OK( requestObj )

    lfns = [ str( subRequestFile["LFN"] ) for subRequestFile in subRequestFiles
             if subRequestFile["Status"] == "Waiting" and  str( subRequestFile["LFN"] ) ]
    self.debug( "removeFile: about to remove %d files" % len( lfns ) )
    # # keep removal status for each file
    removalStatus = dict.fromkeys( lfns, "" )
    self.addMark( "RemoveFileAtt", len( lfns ) )

    # # bulk removal 1st
    exists = self.fc.exists( lfns )
    if not exists['OK']:
      self.error( "removeFile: unable to check existence of files", exists['Message'] )
      return exists
    exists = exists['Value']['Successful']
    lfns = [lfn for lfn in exists if exists[lfn]]
    toRemove = []
    if lfns:
      bulkRemoval = self.dm.removeFile( lfns )
      if not bulkRemoval["OK"]:
        bulkRemoval = { 'Failed' : dict.fromkeys( lfns, bulkRemoval['Message'] )}
      else:
        bulkRemoval = bulkRemoval["Value"]
      failedLfns = bulkRemoval["Failed"] if "Failed" in bulkRemoval else []
      for lfn in removalStatus:
        if lfn in failedLfns and "no such file or directory" in str( bulkRemoval["Failed"][lfn] ).lower():
          removalStatus[lfn] = bulkRemoval["Failed"][lfn]
          removeCatalog = Utils.executeSingleFileOrDirWrapper(self.fc.removeFile( lfn ) )
          if not removeCatalog["OK"]:
            removalStatus[lfn] = removeCatalog["Message"]
            continue
        else:
          toRemove.append( lfn )

    # # loop over LFNs to remove
    for lfn in toRemove:
      self.debug( "removeFile: processing file %s" % lfn )
      try:
        # # try to remove using proxy already defined in os.environ
        removal = self.dm.removeFile( lfn )
        # # file is not existing?
        if not removal["OK"] and "no such file or directory" in str( removal["Message"] ).lower():
          removalStatus[lfn] = removal["Message"]
          continue
        # # not OK but request belongs to DataManager?
        if not self.requestOwnerDN and \
           ( not removal["OK"] and "Write access not permitted for this credential." in removal["Message"] ) or \
           ( removal["OK"] and "Failed" in removal["Value"] and
             lfn in removal["Value"]["Failed"] and
             "permission denied" in str( removal["Value"]["Failed"][lfn] ).lower() ):
          self.debug( "removeFile: retrieving proxy for %s" % lfn )
          getProxyForLFN = self.getProxyForLFN( lfn )
          # # can't get correct proxy? continue...
          if not getProxyForLFN["OK"]:
            self.warn( "removeFile: unable to get proxy for file %s: %s" % ( lfn, getProxyForLFN["Message"] ) )
            removal = getProxyForLFN
          else:
            # # you're a DataManager, retry with the new one proxy
            removal = self.dm.removeFile( lfn )
      finally:
        # # make sure DataManager proxy is set back in place
        if not self.requestOwnerDN and self.dataManagerProxy():
          # # remove temp proxy
          if os.environ["X509_USER_PROXY"] != self.dataManagerProxy():
            os.unlink( os.environ["X509_USER_PROXY"] )
          # # put back DataManager proxy
          os.environ["X509_USER_PROXY"] = self.dataManagerProxy()

      # # save error
      if not removal["OK"]:
        removalStatus[lfn] = removal["Message"]
        continue
      # # check fail reason, filter out missing files
      removal = removal["Value"]
      if lfn in removal["Failed"]:
        removalStatus[lfn] = removal["Failed"][lfn]

    # # counters
    filesRemoved = 0
    filesFailed = 0
    subRequestError = []
    # # update File statuses and errors
    for lfn, error in removalStatus.items():

      # # set file error if any
      if error:
        self.debug( "removeFile: %s: %s" % ( lfn, str( error ) ) )
        fileError = str( error ).replace( "'", "" )[:255]
        fileError = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn,
                                                                "Error", fileError )
        if not fileError["OK"]:
          self.error( "removeFile: unable to set Error for %s: %s" % ( lfn, fileError["Message"] ) )
      # # no error? file not exists? - we are able to recover
      if not error or "no such file or directory" in str( error ).lower() or \
            "file does not exist" in str( error ).lower():
        filesRemoved += 1
        self.info( "removeFile: successfully removed %s" % lfn )
        updateStatus = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )
        if not updateStatus["OK"]:
          self.error( "removeFile: unable to change status to 'Done' for %s" % lfn )
      else:
        filesFailed += 1
        self.warn( "removeFile: unable to remove file %s : %s" % ( lfn, error ) )
        errorStr = str( error )
        if type( error ) == type( {} ):
          errorStr = ";".join( [ "%s:%s" % ( key, value ) for key, value in error.items() ] )
        errorStr = errorStr.replace( "'", "" )
        subRequestError.append( "%s:%s" % ( lfn, errorStr ) )

    self.addMark( "RemoveFileDone", filesRemoved )
    self.addMark( "RemoveFileFail", filesFailed )

    # # all 'Done'?
    if requestObj.isSubRequestDone( index, "removal" )["Value"]:
      self.info( "removeFile: all files processed, setting subrequest status to 'Done'" )
      requestObj.setSubRequestStatus( index, "removal", "Done" )
    elif filesFailed:
      self.info( "removeFile: all files processed, %s files failed to remove" % filesFailed )
      subRequestError = ";".join( subRequestError )[:255]
      subRequestError = requestObj.setSubRequestAttributeValue( index, "removal", "Error", subRequestError )
    return S_OK( requestObj )

  def replicaRemoval( self, index, requestObj, subRequestAttrs, subRequestFiles ):
    """ action for 'replicaRemoval' operation

    :param self: self reference
    :param int index: subRequest index in execution order
    :param RequestContainer requestObj: request
    :param dict subRequestAttrs: subRequest's attributes
    :param dict subRequestFiles: subRequest's files

    TODO: add bulk removal first
    """
    self.info( "replicaRemoval: processing subrequest %s" % index )
    if requestObj.isSubRequestEmpty( index, "removal" )["Value"]:
      self.info( "replicaRemoval: subrequest %s is empty, setting its status to 'Done'" % index )
      requestObj.setSubRequestStatus( index, "removal", "Done" )
      return S_OK( requestObj )

    targetSEs = list( set( [ targetSE.strip() for targetSE in subRequestAttrs["TargetSE"].split( "," )
                            if targetSE.strip() ] ) )
    lfns = [ str( subRequestFile["LFN"] ) for subRequestFile in subRequestFiles
              if subRequestFile["Status"] == "Waiting" and str( subRequestFile["LFN"] ) ]

    self.debug( "replicaRemoval: found %s lfns to delete from %s sites (%s replicas)" % ( len( lfns ),
                                                                                          len( targetSEs ),
                                                                                          len( lfns ) * len( targetSEs ) ) )
    self.addMark( "ReplicaRemovalAtt", len( lfns ) * len( targetSEs ) )
    removalStatus = {}

    # # loop over LFNs
    for lfn in lfns:
      self.info( "replicaRemoval: processing file %s" % lfn )
      # # prepare status dict
      removalStatus[lfn] = dict.fromkeys( targetSEs, "" )
      # # loop over targetSEs
      try:
        for targetSE in targetSEs:
          # # try to remove using current proxy
          removeReplica = self.dm.removeReplica( targetSE, lfn )
          # # file is not existing?
          if not removeReplica["OK"] and "no such file or directory" in str( removeReplica["Message"] ).lower():
            removalStatus[lfn][targetSE] = removeReplica["Message"]
            continue

          # # not OK but request belongs to DataManager?
          if not self.requestOwnerDN and \
                ( not removeReplica["OK"] and
                  "Write access not permitted for this credential." in removeReplica["Message"] ) or \
                ( removeReplica["OK"] and "Failed" in removeReplica["Value"] and
                  lfn in removeReplica["Value"]["Failed"] and
                  "permission denied" in str( removeReplica["Value"]["Failed"][lfn] ).lower() ):
            # # get proxy for LFN
            getProxyForLFN = self.getProxyForLFN( lfn )
            # # can't get correct proxy?
            if not getProxyForLFN["OK"]:
              self.warn( "replicaRemoval: unable to get proxy for file %s: %s" % ( lfn, getProxyForLFN["Message"] ) )
              removeReplica = getProxyForLFN
            else:
              # # got correct proxy? try to remove again
              removeReplica = self.dm.removeReplica( targetSE, lfn )

          if not removeReplica["OK"]:
            removalStatus[lfn][targetSE] = removeReplica["Message"]
            continue
          removeReplica = removeReplica["Value"]
          # # check failed status for missing files
          if lfn in removeReplica["Failed"]:
            removalStatus[lfn][targetSE] = removeReplica["Failed"][lfn]
      finally:
        # # make sure DataManager proxy is set back in place
        if not self.requestOwnerDN and self.dataManagerProxy():
          # # remove temp proxy
          if os.environ["X509_USER_PROXY"] != self.dataManagerProxy():
            os.unlink( os.environ["X509_USER_PROXY"] )
          # # put back DataManager proxy
          os.environ["X509_USER_PROXY"] = self.dataManagerProxy()

    replicasRemoved = 0
    replicasFailed = 0
    subRequestError = []

    # # filter out missing files
    for lfn, pTargetSEs in removalStatus.items():
      for targetSE, error in pTargetSEs.items():
        if "no such file or directory" in str( error ).lower():
          removalStatus[lfn][targetSE] = ""
    # # loop over statuses and errors
    for lfn, pTargetSEs in removalStatus.items():

      failed = [ ( targetSE, error ) for targetSE, error in pTargetSEs.items() if error != "" ]
      successful = [ ( targetSE, error ) for targetSE, error in pTargetSEs.items() if error == "" ]

      replicasRemoved += len( successful )
      replicasFailed += len( failed )

      if not failed:
        self.info( "replicaRemoval: successfully removed %s from %s" % ( lfn, str( targetSEs ) ) )
        updateStatus = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )
        if not updateStatus["OK"]:
          self.error( "replicaRemoval: error setting status to 'Done' for %s" % lfn )
        continue

      for targetSE, error in failed:
        self.warn( "replicaRemoval: failed to remove %s from %s: %s" % ( lfn, targetSE, error ) )

      fileError = ";".join( [ "%s:%s" % ( targetSE, str( error ).replace( "'", "" ) ) for targetSE, error in failed ] )[:255]
      subRequestError.append( fileError )
      fileError = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Error", fileError )
      if not fileError["OK"]:
        self.error( "replicaRemoval: unable to set Error for %s: %s" % ( lfn, fileError["Message"] ) )

    self.addMark( "ReplicaRemovalDone", replicasRemoved )
    self.addMark( "ReplicaRemovalFail", replicasFailed )

    # # no 'Waiting' files or all 'Done'
    if requestObj.isSubRequestDone( index, "removal" )["Value"]:
      self.info( "replicaRemoval: all files processed, setting subrequest status to 'Done'" )
      requestObj.setSubRequestStatus( index, "removal", "Done" )
    elif replicasFailed:
      self.info( "replicaRemoval: all files processed, failed to remove %s replicas" % replicasFailed )
      subRequestError = ";".join( subRequestError ).replace( "'", "" )[:255]
      subRequestError = requestObj.setSubRequestAttributeValue( index, "removal", "Error", subRequestError )

    # # return requestObj at least
    return S_OK( requestObj )

  def reTransfer( self, index, requestObj, subRequestAttrs, subRequestFiles ):
    """ action for 'reTransfer' operation

    :param self: self reference
    :param int index: subRequest index in execution order
    :param RequestContainer requestObj: request
    :param dict subRequestAttrs: subRequest's attributes
    :param dict subRequestFiles: subRequest's files
    """
    self.info( "reTransfer: processing subrequest %s" % index )
    if requestObj.isSubRequestEmpty( index, "removal" )["Value"]:
      self.info( "reTransfer: subrequest %s is empty, setting its status to 'Done'" % index )
      requestObj.setSubRequestStatus( index, "removal", "Done" )
      return S_OK( requestObj )
    subRequestError = []

    targetSEs = list( set( [ targetSE.strip() for targetSE in subRequestAttrs["TargetSE"].split( "," )
                             if targetSE.strip() ] ) )
    lfnsPfns = [ ( subFile["LFN"], subFile["PFN"], subFile["Status"] ) for subFile in subRequestFiles ]

    failed = {}
    for lfn, pfn, status in lfnsPfns:
      self.info( "reTransfer: processing file %s" % lfn )
      if status != "Waiting":
        self.info( "reTransfer: skipping file %s, status is %s" % ( lfn, status ) )
        continue
      failed.setdefault( lfn, {} )
      for targetSE in targetSEs:
        reTransfer = StorageElement( targetSE ).retransferOnlineFile( pfn )
        if reTransfer["OK"]:
          if pfn in reTransfer["Value"]["Successful"]:
            self.info( "reTransfer: succesfully requested retransfer of %s" % pfn )
          else:
            reason = reTransfer["Value"]["Failed"][pfn]
            self.error( "reTransfer: failed to set retransfer request for %s at %s: %s" % ( pfn, targetSE, reason ) )
            failed[lfn][targetSE] = reason
            subRequestError.append( "%s:%s:%s" % ( lfn, targetSE, reason ) )
        else:
          self.error( "reTransfer: completely failed to retransfer: %s" % reTransfer["Message"] )
          failed[lfn][targetSE] = reTransfer["Message"]
          subRequestError.append( "%s:%s:%s" % ( lfn, targetSE, reTransfer["Message"] ) )
      if not failed[lfn]:
        self.info( "reTransfer: file %s sucessfully processed at all targetSEs" % lfn )
        requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )

    # # subrequest empty or all Files done?
    if requestObj.isSubRequestDone( index, "removal" )["Value"]:
      self.info( "reTransfer: all files processed, setting subrequest status to 'Done'" )
      requestObj.setSubRequestStatus( index, "removal", "Done" )
    else:
      subRequestError = requestObj.setSubRequestAttributeValue( index, "removal",
                                                                "Error", ";".join( subRequestError )[:255] )
    return S_OK( requestObj )

  def physicalRemoval( self, index, requestObj, subRequestAttrs, subRequestFiles ):
    """ action for 'physicalRemoval' operation

    :param self: self reference
    :param int index: subRequest index in execution order
    :param RequestContainer requestObj: request
    :param dict subRequestAttrs: subRequest's attributes
    :param dict subRequestFiles: subRequest's files
    """
    self.info( "physicalRemoval: processing subrequest %s" % index )
    if requestObj.isSubRequestEmpty( index, "removal" )["Value"]:
      self.info( "physicalRemoval: subrequest %s is empty, setting its status to 'Done'" % index )
      requestObj.setSubRequestStatus( index, "removal", "Done" )
      return S_OK( requestObj )

    targetSEs = list( set( [ targetSE.strip() for targetSE in subRequestAttrs["TargetSE"].split( "," )
                           if targetSE.strip() ] ) )
    pfns = []
    pfnToLfn = {}
    for subRequestFile in subRequestFiles:
      if subRequestFile["Status"] == "Waiting":
        pfn = subRequestFile["PFN"]
        lfn = subRequestFile["LFN"]
        pfnToLfn[pfn] = lfn
        pfns.append( pfn )
    failed = {}
    errors = {}
    self.addMark( 'PhysicalRemovalAtt', len( pfns ) )
    for targetSE in targetSEs:
      remove = StorageElement( targetSE ).removeFile( pfns )
      if remove["OK"]:
        for pfn in remove["Value"]["Failed"]:
          if pfn not in failed:
            failed[pfn] = {}
          failed[pfn][targetSE] = remove["Value"]["Failed"][pfn]
      else:
        errors[targetSE] = remove["Message"]
        for pfn in pfns:
          if pfn not in failed:
            failed[pfn] = {}
          failed[pfn][targetSE] = "Completely"
    failedPFNs = failed.keys()
    pfnsOK = [ pfn for pfn in pfns if pfn not in failedPFNs ]
    self.addMark( "PhysicalRemovalDone", len( pfnsOK ) )
    for pfn in pfnsOK:
      self.info( "physicalRemoval: succesfully removed %s from %s" % ( pfn, str( targetSEs ) ) )
      res = requestObj.setSubRequestFileAttributeValue( index, "removal", pfnToLfn[pfn], "Status", "Done" )
      if not res["OK"]:
        self.error( "physicalRemoval: error setting status to 'Done' for %s" % pfnToLfn[pfn] )

    if failed:
      self.addMark( "PhysicalRemovalFail", len( failedPFNs ) )
      for pfn in failed:
        for targetSE in failed[pfn]:
          if type( failed[pfn][targetSE] ) in StringTypes:
            if re.search( "no such file or directory", failed[pfn][targetSE].lower() ):
              self.info( "physicalRemoval: file %s did not exist" % pfn )
              res = requestObj.setSubRequestFileAttributeValue( index, "removal", pfnToLfn[pfn], "Status", "Done" )
              if not res["OK"]:
                self.error( "physicalRemoval: error setting status to 'Done' for %s" % pfnToLfn[pfn] )

    if errors:
      for targetSE in errors:
        self.warn( "physicalRemoval: completely failed to remove files at %s" % targetSE )

    # # subrequest empty or all Files done?
    if requestObj.isSubRequestDone( index, "removal" )["Value"]:
      self.info( "physicalRemoval: all files processed, setting subrequest status to 'Done'" )
      requestObj.setSubRequestStatus( index, "removal", "Done" )

    return S_OK( requestObj )
