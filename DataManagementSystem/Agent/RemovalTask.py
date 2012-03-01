########################################################################
# $HeadURL $
# File: RemoveTask.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/10/25 07:52:37
########################################################################

""" :mod: RemoveTask 
    =======================
 
    .. module: RemovalTask
    :synopsis: removal requests processing 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    removal requests processing 
"""

__RCSID__ = "$Id $"

##
# @file RemoveTask.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/10/25 07:52:50
# @brief Definition of RemoveTask class.

## imports 
import re
import os
## from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager 

########################################################################
class RemovalTask( RequestTask ):
  """
  .. class:: RemovalTask
  
  """

  def __init__( self, *args, **kwargs ):
    """c'tor

    :param self: self reference
    :param tuple args: anonymouse args tuple 
    :param dict kwagrs: named args dict
    """
    RequestTask.__init__( self, *args, **kwargs )
    self.setRequestType( "removal" )
    ## operation handlers
    self.addOperationAction( "removeFile", self.removeFile )
    self.addOperationAction( "replicaRemoval", self.replicaRemoval )
    self.addOperationAction( "reTransfer", self.reTransfer ) 

  def getProxyForLFN( self, lfn ):
    """ get proxy for LFN

    :param self: self reference
    :param str lfn: LFN
    """
    dirMeta = self.replicaManager().getCatalogDirectoryMetadata( lfn, singleFile = True )
    if not dirMeta["OK"]:
      return dirMeta
    dirMeta = dirMeta["Value"]
    ownerRole = "/%s" % dirMeta["OwnerRole"] if not dirMeta["OwnerRole"].startswith("/") else dirMeta["OwnerRole"]
    ownerDN = dirMeta["OwnerDN"]

    ownerProxy = None
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

    if not ownerProxy:
      return S_ERROR("Unable to get owner proxy")

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
    #if requestObj.isSubRequestEmpty( index, "removal" ):
    #  self.info("removeFile: subrequest %s is empty, setting its status to 'Done'" % index )
    #  requestObj.setSubRequestStatus( index, "removal", "Done" )
    #  return S_OK( requestObj )

    lfns = [ str( subRequestFile["LFN"] ) for subRequestFile in subRequestFiles 
             if subRequestFile["Status"] == "Waiting" and  str( subRequestFile["LFN"] ) ]
    self.debug( "removeFile: about to remove %d files" % len(lfns) )
    ## keep removal status for each file
    removalStatus = {} 
    self.addMark( "RemoveFileAtt", len( lfns ) )
    for lfn in lfns:
      self.debug("removeFile: processing file %s" % lfn )
      try:
        ## are you DataManager?
        if not self.requestOwnerDN:
          getProxyForLFN = self.getProxyForLFN( lfn )
          if not getProxyForLFN["OK"]:
            if re.search( "no such file or directory", getProxyForLFN["Message"].lower() ):
              removalStatus[lfn] = ""
            else:
              ## this LFN will be 'Waiting' unless proxy will be uploaded and/or retrieved
              self.error("removeFile: unable to get proxy for file %s: %s" % ( lfn, getProxyForLFN["Message"] ) )
            continue
        removal = self.replicaManager().removeFile( lfn )
      finally:
        ## make sure DataManager proxy is set back in place
        if not self.requestOwnerDN and self.dataManagerProxy():
          os.environ["X509_USER_PROXY"] = self.dataManagerProxy()

      ## a priori OK
      removalStatus[lfn] = "" 
      if not removal["OK"]:
        removalStatus[lfn] = removal["Message"]
        continue
      ## check fail reason
      removal = removal["Value"]  
      if lfn in removal["Failed"]:
        error = str(removal["Failed"][lfn])
        missingFile = re.search( "no such file or directory", error.lower() )
        removalStatus[lfn] = "" if missingFile else str(removal["Failed"][lfn])

    ## counters 
    filesRemoved = 0
    filesFailed = 0

    ## update File statuses and errors
    for lfn, error in removalStatus.items():
      if not error:
        filesRemoved += 1
        self.info("removeFile: successfully removed %s" % lfn )
        updateStatus = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )
        if not updateStatus["OK"]:
          self.error("removeFile: unable to change status to 'Done' for %s" % lfn )
      else:
        filesFailed += 1
        self.error("removeFile: unable to remove file %s : %s" % ( lfn, error ) )
        fileError = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Error", error[:255] )
        if not fileError["OK"]:
          self.error("removeFile: unable to set Error for %s: %s" % ( lfn, fileError["Message"] ) )
        updateStatus = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Failed" )
        if not updateStatus["OK"]:
          self.error("removeFile: unable to change status to 'Failed' for %s" % lfn )
          
    self.addMark( "RemoveFileDone", filesRemoved )
    self.addMark( "RemoveFileFail", filesFailed )
    
    ## no 'Waiting' or all 'Done'?
    if requestObj.isSubRequestDone( index, "removal" ) or requestObj.isSubRequestEmpty( index, "removal" ):
      self.info("removeFile: all files processed, setting subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "removal", "Done" )
      
    return S_OK( requestObj )

  def replicaRemoval( self, index, requestObj, subRequestAttrs, subRequestFiles ):
    """ action for 'replicaRemoval' operation

    :param self: self reference
    :param int index: subRequest index in execution order 
    :param RequestContainer requestObj: request 
    :param dict subRequestAttrs: subRequest's attributes
    :param dict subRequestFiles: subRequest's files
    """
    self.info( "replicaRemoval: processing subrequest %s" % index )
    #if requestObj.isSubRequestEmpty( index, "removal" ):
    #  self.info("replicaRemoval: subrequest %s is empty, setting its status to 'Done'" % index )
    #  requestObj.setSubRequestStatus( index, "removal", "Done" )
    #  return S_OK( requestObj )

    targetSEs = list( set( [ targetSE.strip() for targetSE in subRequestAttrs["TargetSE"].split(",") 
                            if targetSE.strip() ] ) )
    lfns =  [ str( subRequestFile["LFN"] ) for subRequestFile in subRequestFiles 
              if subRequestFile["Status"] == "Waiting" and str(subRequestFile["LFN"]) ]

    self.debug( "replicaRemoval: found %s lfns to delete from %s sites (%s replicas)" % ( len(lfns), 
                                                                                          len(targetSEs), 
                                                                                          len(lfns)*len(targetSEs) ) )
    self.addMark( "ReplicaRemovalAtt", len(lfns)*len(targetSEs) )
    removalStatus = {}

    ## loop over LFNs
    for lfn in lfns:
      self.info("replicaRemoval: processing file %s" % lfn )
      try:
        ## are you DataManager?
        if not self.requestOwnerDN:
          getProxyForLFN = self.getProxyForLFN( lfn )
          if not getProxyForLFN["OK"]:
            if re.search( "no such file or directory", getProxyForLFN["Message"].lower() ):
              removalStatus[lfn] = dict.fromkeys( targetSEs, "" )
            else:
              ## this LFN will be 'Waiting' unless proxy will be uploaded and/or retrieved
              self.error("removeFile: unable to get proxy for file %s: %s" % ( lfn, getProxyForLFN["Message"] ) )
            continue
        ## loop over targetSEs
        for targetSE in targetSEs:
          ## prepare status dict
          if lfn not in removalStatus:
            removalStatus.setdefault( lfn, {} )
          ## a priori OK
          removalStatus[lfn][targetSE] = ""
          ## call ReplicaManager
          removeReplica = self.replicaManager().removeReplica( targetSE, lfn )
          if not removeReplica["OK"]:
            removalStatus[lfn][targetSE] = removeReplica["Message"]
            continue
          removeReplica = removeReplica["Value"]
          ## check failed status
          if lfn in removeReplica["Failed"]:
            error = str( removeReplica["Failed"][lfn] )
            missingFile = re.search( "no such file or directory", error.lower() ) 
            removalStatus[lfn][targetSE] = "" if missingFile else error
      finally:
        ## make sure DataManager proxy is set back in place
        if not self.requestOwnerDN and self.dataManagerProxy():
          os.environ["X509_USER_PROXY"] = self.dataManagerProxy()

    replicasRemoved = 0
    replicasFailed = 0
    ## loop over statuses and errors
    for lfn, pTargetSEs in removalStatus.items():

      failed = [ ( targetSE, error ) for targetSE, error in pTargetSEs.items() if error != "" ]
      successful = [ ( targetSE, error ) for targetSE, error in pTargetSEs.items() if error == "" ]
      
      replicasRemoved += len( successful )
      replicasFailed += len( failed )

      if not failed:
        self.info("replicaRemoval: successfully removed %s from %s" % ( lfn, str(targetSEs) ) )
        updateStatus = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )
        if not updateStatus["OK"]:
          self.error( "replicaRemoval: error setting status to 'Done' for %s" % lfn )
        continue

      for targetSE, error in failed:
        self.error("replicaRemoval: failed to remove %s from %s: %s" % ( lfn, targetSE, error ) )

      fileError = ";".join( ["%s:%s" % error for error in failed ] )[:255]
      fileError = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Error", fileError )
      if not fileError["OK"]:
        self.error("removeFile: unable to set Error for %s: %s" % ( lfn, fileError["Message"] ) )
      updateStatus = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Failed" )
      if not updateStatus["OK"]:
        self.error( "replicaRemoval: error setting status to 'Failed' for %s" % lfn )

    self.addMark( "ReplicaRemovalDone", replicasRemoved )
    self.addMark( "ReplicaRemovalFail", replicasFailed )

    ## no 'Waiting' files or all 'Done' 
    if requestObj.isSubRequestDone( index, "removal" ) or requestObj.isSubRequestEmpty( index, "removal" ):
      self.info("replicaRemoval: all files processed, setting subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "removal", "Done" )
      
    ## return requestObj at least
    return S_OK( requestObj )

  def reTransfer( self, index, requestObj, subRequestAttrs, subRequestFiles ):
    """ action for 'reTransfer' operation

    :param self: self reference
    :param int index: subRequest index in execution order 
    :param RequestContainer requestObj: request 
    :param dict subRequestAttrs: subRequest's attributes
    :param dict subRequestFiles: subRequest's files    
    """
    self.info("reTransfer: processing subrequest %s" % index )
    #if requestObj.isSubRequestEmpty( index, "removal" ):
    #  self.info("reTransfer: subrequest %s is empty, setting its status to 'Done'" % index )
    #  requestObj.setSubRequestStatus( index, "removal", "Done" )
    #  return S_OK( requestObj )

    targetSEs = list( set( [ targetSE.strip() for targetSE in subRequestAttrs["TargetSE"].split(",") 
                             if targetSE.strip() ] ) )
    lfnsPfns = [ ( subFile["LFN"], subFile["PFN"], subFile["Status"] ) for subFile in subRequestFiles ]
 
    failed = {}
    for lfn, pfn, status in lfnsPfns:
      self.info("reTransfer: processing file %s" % lfn )
      if status != "Waiting":
        self.info("reTransfer: skipping file %s, status is %s" % ( lfn, status ) )
        continue 
      failed.setdefault( lfn, {} )
      for targetSE in targetSEs:
        reTransfer = self.replicaManager().onlineRetransfer( targetSE, pfn )
        if reTransfer["OK"]:
          if pfn in reTransfer["Value"]["Successful"]:
            self.info("reTransfer: succesfully requested retransfer of %s" % pfn )
          else:
            reason = reTransfer["Value"]["Failed"][pfn]
            self.error( "reTransfer: failed to set retransfer request for %s at %s: %s" % ( pfn, targetSE, reason ) )
            failed[lfn][targetSE] = reason
        else:
          self.error( "reTransfer: completely failed to retransfer: %s" % reTransfer["Message"] )
          failed[lfn][targetSE] = reTransfer["Message"]
      if not failed[lfn]:
        self.info("reTransfer: file %s sucessfully processed at all targetSEs" % lfn )
        requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )
       
    ## subrequest empty or all Files done?
    if requestObj.isSubRequestDone( index, "removal" ) or requestObj.isSubRequestEmpty( index, "removal" ):
      self.info("reTransfer: all files processed, setting subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "removal", "Done" )

    return S_OK( requestObj )
  
