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
from types import StringTypes
## from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask

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

  def removeFile( self, index, requestObj, subRequestAttrs, subRequestFiles ):
    """ action for 'removeFile' operation

    :param self: self reference
    :param int index: subRequest index in execution order 
    :param RequestContainer requestObj: request 
    :param dict subRequestAttrs: subRequest's attributes
    :param dict subRequestFiles: subRequest's files
    """
    self.info( "removeFile: processing subrequest %s" % index  )
    lfns = [ str( subRequestFile["LFN"] ) for subRequestFile in subRequestFiles 
             if subRequestFile["Status"] == "Waiting" ]
    self.debug( "removeFile: about to remove %d files" % len(lfns) )

    self.addMark( "RemoveFileAtt", len( lfns ) )
    if lfns:
      removal = self.replicaManager().removeFile( lfns )
      self.addMark( "RemoveFileDone", len( removal["Value"]["Successful"] ) )
      self.addMark( "RemoveFileFail", len( removal["Value"]["Failed"] ) )
      if removal["OK"]:
        for lfn in removal["Value"]["Successful"]:
          self.info("removeFile: successfully removed %s" % lfn )
          updateStatus = requestObj.setSubRequestFileAttributeValue( index, "removal", 
                                                                     lfn, "Status", "Done" )
          if not updateStatus["OK"]:
            self.error("removeFile: unable to change status to 'Done' for %s" % lfn )
    
        for lfn, error in removal["Value"]["Failed"].items():  
          if type(error) in StringTypes:
            if re.search( "no such file or directory", error.lower() ):
              self.info("removeFile: file %s didn't exist, setting its status to 'Done'" % lfn )
              updateStatus = requestObj.setSubRequestFileAttributeValue( index, "removal", 
                                                                         lfn, "Status", "Done")
              if not updateStatus["OK"]:
                self.error("removeFile: unable to change status to 'Done' for %s" % lfn )
            else:
              self.error("removeFile: unable to remove file %s : %s" % ( lfn, error ) )
      else:
        self.addMark( 'RemoveFileFail', len( lfns ) )
        self.error("removeFile: completely failed to remove files: %s" % removal["Message"] )
    
    if requestObj.isSubRequestEmpty( index, "removal" ) or requestObj.isSubRequestDone( index, "removal" ):
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
    targetSEs = list( set( [ targetSE.strip() for targetSE in subRequestAttrs["TargetSE"].split(",") 
                            if targetSE.strip() ] ) )
    lfns =  [ str( subRequestFile["LFN"] ) for subRequestFile in subRequestFiles 
              if subRequestFile["Status"] == "Waiting" ]
    failed = {}
    errMsg = {}

    self.addMark( 'ReplicaRemovalAtt', len( lfns ) )
    self.debug( "replicaRemoval: found %s replicas to delete from %s sites" % ( len(lfns), len(targetSEs) ) )

    for targetSE in targetSEs:
      self.debug( "replicaRemoval: deleting files from %s" % targetSE )
      removeReplica = self.replicaManager().removeReplica( targetSE, lfns )
      if removeReplica["OK"]:
        for lfn, error in removeReplica["Value"]["Failed"].items():
          if lfn not in failed:
            failed.setdefault( lfn, {} )
          failed[lfn][targetSE] = error
      else:
        errMsg[targetSE] = removeReplica["Message"]
        for lfn in lfns:
          if lfn not in failed:
            failed.setdefault( lfn, {} )
          failed[lfn][targetSE] = "Completely"

    removedLFNs = [ lfn for lfn in lfns if lfn not in failed.keys() ]
    self.addMark( 'ReplicaRemovalDone', len( removedLFNs ) )

    ## set status for removed to Done
    for lfn in removedLFNs:
      self.info("replicaRemoval: successfully removed %s from %s" % ( lfn, str(targetSEs) ) )
      res = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )
      if not res["OK"]:
        self.error( "replicaRemoval: error setting status to 'Done' for %s" % lfn )

    ## check failed
    if failed:
      self.addMark( 'ReplicaRemovalFail', len( failed ) )
      for lfn, errors in failed.items():
        for targetSE, error in errors.items(): 
          if type( error ) in StringTypes:
            if re.search( "no such file or directory", error.lower() ):
              self.info( "replicaRemoval: file %s doesn't exist at %s" % ( lfn, targetSE ) )
              res = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )
              if not res["OK"]:
                self.error( "replicaRemoval: error setting status to 'Done' for %s" % lfn )
            else:
              self.error( "replicaRemoval: failed to remove file %s at %s" % ( lfn, targetSE ) )

    ## check errMsg
    for targetSE, error in errMsg.items():
      self.error("replicaRemoval: failed to remove replicas at %s: %s" % ( targetSE, error ) )

    if requestObj.isSubRequestEmpty( index, "removal" ) or requestObj.isSubRequestDone( index, "removal" ):
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
    targetSEs = list( set( [ targetSE.strip() for targetSE in subRequestAttrs["TargetSE"].split(",") 
                             if targetSE.strip() ] ) )
    lfnsPfns = [ ( subFile["LFN"], subFile["PFN"], subFile["Status"] ) for subFile in subRequestFiles ]
    subRequestError = False
    failed = {}
    for lfn, pfn, status in lfnsPfns:
      self.info("reTransfer: processing file %s" % lfn )
      failed.setdefault( lfn, {} )
      if status != "Waiting":
        self.info("reTransfer: skipping file %s, status is %s" % ( lfn, status ) )
        continue 

      for targetSE in targetSEs:
        reTransfer = self.replicaManager().onlineRetransfer( targetSE, pfn )
        if reTransfer["OK"]:
          if pfn in reTransfer["Value"]["Successful"]:
            self.info("reTransfer: succesfully requested retransfer of %s" % pfn )
          else:
            reason = reTransfer["Value"]["Failed"][pfn]
            self.error( "reTransfer: failed to set retransfer request for %s at %s: %s" % ( pfn, targetSE, reason ) )
            failed[lfn][targetSE] = reTransfer["Value"]["Failed"][pfn]
            subRequestError = True 
        else:
          self.error( "reTransfer: completely failed to retransfer: %s" % reTransfer["Message"] )
          failed[lfn][targetSE] = reTransfer["Message"]
          subRequestError = True

      if not failed[lfn]:
        self.info("reTransfer: file %s sucessfully processed at all targetSEs" % lfn )
        requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )

    if not subRequestError:
      self.info("reTransfer: all files processed successfully, setting subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "removal", "Done" )
      
    return S_OK( requestObj )
  
