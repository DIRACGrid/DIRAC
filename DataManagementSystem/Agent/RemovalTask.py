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
    self.addOperationAction( "physicalRemoval" , self.physicalRemoval )
    self.addOperationAction( "removeFile", self.removeFile )
    self.addOperationAction( "replicaRemoval", self.replicaRemoval )
    self.addOperationAction( "reTransfer", self.reTransfer )
      
  def physicalRemoval( self, index, requestObj, subRequestAttrs, subRequestFiles ):
    """ action for 'physicalRemoval' operation  

    :param self: self reference
    :param int index: subRequest index in execution order 
    :param RequestContainer requestObj: request 
    :param dict subRequestAttrs: subRequest's attributes
    :param dict subRequestFiles: subRequest's files
    """
    targetSEs = list(set([ targetSE.strip() for targetSE in subRequestAttrs["TargetSE"].split(",") ]))
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
      remove = self.replicaManager().removeStorageFiles( pfns, targetSE )
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
    self.addMark( 'PhysicalRemovalDone', len( pfnsOK ) )
    for pfn in pfnsOK:
      self.info("Succesfully removed %s from %s" % ( pfn, str(targetSEs) ) )
      res = requestObj.setSubRequestFileAttributeValue( index, "removal", pfnToLfn[pfn], "Status", "Done" )
      if not res["OK"]:
        self.error("Error setting status to 'Done' for %s" % pfnToLfn[pfn])

    if failed:
      self.addMark( 'PhysicalRemovalFail', len( failedPFNs ) )
      for pfn in failed:
        for targetSE in failed[pfn]:
          if type( failed[pfn][targetSE] ) in StringTypes:
            if re.search("no such file or directory", failed[pfn][targetSE].lower()):
              self.info("File %s did not exist" % pfn)
              res = requestObj.setSubRequestFileAttributeValue( index, "removal", pfnToLfn[pfn], "Status", "Done" )
              if not res["OK"]:
                self.error("Error setting status to 'Done' for %s" % pfnToLfn[pfn] )

    if errors:
      for targetSE in errors:
        self.error("Completely failed to remove files at %s" % targetSE )

    return S_OK( requestObj )

  def removeFile( self, index, requestObj, subRequestAttrs, subRequestFiles ):
    """ action for 'removeFile' operation

    :param self: self reference
    :param int index: subRequest index in execution order 
    :param RequestContainer requestObj: request 
    :param dict subRequestAttrs: subRequest's attributes
    :param dict subRequestFiles: subRequest's files
    """
    lfns = [ str( subRequestFile["LFN"] ) for subRequestFile in subRequestFiles 
             if subRequestFile["Status"] == "Waiting" ]
    self.addMark( 'RemoveFileAtt', len( lfns ) )
    if lfns:
      removal = self.replicaManager().removeFile( lfns )
      self.addMark( 'RemoveFileDone', len( removal["Value"]["Successful"] ) )
      self.addMark( 'RemoveFileFail', len( removal["Value"]["Failed"] ) )
      if removal["OK"]:
        for lfn in removal["Value"]["Successful"]:
          self.info("Successfully removed %s" % lfn )
          updateStatus = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )
          if not updateStatus["OK"]:
            self.error("Unable to change status to 'Done' for %s" % lfn )
        for lfn, error in removal["Value"]["Failed"].items():
          if type(error) in StringTypes:
            if re.search( "no such file or directory", error.lower() ):
              self.info("File %s didn't exist, setting its status to 'Done'." % lfn )
              updateStatus = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done")
              if not updateStatus["OK"]:
                self.error("Unable to change status to 'Done' for %s" % lfn )
            else:
              self.error("Unable to remove file '%s' : %s" % ( lfn, error ) )
      else:
        self.addMark( 'RemoveFileFail', len( lfns ) )
        self.error("Completely failed to remove files: %s" % removal["Message"] )
        return removal

    return S_OK( requestObj )

  def replicaRemoval( self, index, requestObj, subRequestAttrs, subRequestFiles ):
    """ action for 'replicaRemoval' operation

    :param self: self reference
    :param int index: subRequest index in execution order 
    :param RequestContainer requestObj: request 
    :param dict subRequestAttrs: subRequest's attributes
    :param dict subRequestFiles: subRequest's files
    """
    diracSEs = list(set([targetSE.strip() for targetSE in subRequestAttrs["TargetSE"].split(",")]))
    lfns =  [ str( subRequestFile["LFN"] ) for subRequestFile in subRequestFiles 
              if subRequestFile["Status"] == "Waiting" ]
    failed = {}
    errMsg = {}
    timeOutCounter = 0
    self.addMark( 'ReplicaRemovalAtt', len( lfns ) )
    for diracSE in diracSEs:
      removeReplica = self.replicaManager().removeReplica( diracSE, lfns )
      if removeReplica["OK"]:
        for lfn, error in removeReplica["Value"]["Failed"].items():
          if error.find( 'seconds timeout for "__gfal_wrapper" call' ) != -1:
            timeOutCounter += 1
          if lfn not in failed:
            failed.setdefault( lfn, {} )
          failed[lfn][diracSE] = error
      else:
        errMsg[diracSE] = removeReplica["Message"]
        for lfn in lfns:
          if lfn not in failed:
            failed.setdefault( lfn, {} )
          failed[lfn][diracSE] = "Completely"
    removedLFNs = [ lfn for lfn in lfns if lfn not in failed.keys() ]
    self.addMark( 'ReplicaRemovalDone', len( removedLFNs ) )
    ## set status for removed to Done
    for lfn in removedLFNs:
      self.info("Succefully removed %s at %s" % ( lfn, str(diracSEs) ) )
      res = requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )
      if not res["OK"]:
        self.error( "Error setting status to 'Done' for %s" % lfn )
    ## check failed
    if failed:
      self.addMark( 'PhysicalRemovalFail', len( failed ) )
      for lfn, diracSE in failed.iteritems():
        error = failed[lfn][diracSE]
        if type( error ) in StringTypes:
          if re.search( "no such file or directory", error.lower() ):
            self.info( "File %s didn't exist at %s" % ( lfn, diracSE ) )
            ## set the status to 'Done' anyway
            res = requestObj.setSUbRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )
            if not res["OK"]:
              self.error( "Error setting status to 'Done' for %s" % lfn )
          else:
            self.error( "Failed to remove file %s at %s" % ( lfn, diracSE ) )

    if errMsg:
      for diracSE, error in errMsg.iteritems():
        self.error("Completely failed to remove replicas at %s" % diracSE )

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
    targetSEs = list( set( [ targetSE.strip() for targetSE in subRequestAttrs["TargetSE"].split(",") ]))
    lfnsPfns = [ ( subFile["LFN"], subFile["PFN"], subFile["Status"] ) for subFile in subRequestFiles ]
    subRequestError = False
    failed = {}
    for lfn, pfn, status in lfnsPfns:
      self.info("Processing file %s" % lfn )
      failed.setdefault( lfn, {} )
      if status != "Waiting":
        self.info("Skipping file %s, status is %s" % ( lfn, status ) )
        continue 

      for targetSE in targetSEs:
        reTransfer = self.replicaManager().onlineRetransfer( targetSE, pfn )
        if reTransfer["OK"]:
          if pfn in reTransfer["Value"]["Successful"]:
            self.info("Succesfully requested retransfer of %s" % pfn )
          else:
            self.error( "Failed to retransfer request for %s at %s: %s" % ( pfn, 
                                                                            targetSE, 
                                                                            reTransfer["Value"]["Failed"][pfn] ) )
            failed[lfn][targetSE] = reTransfer["Value"]["Failed"][pfn]
            subRequestError = True 
        else:
          self.error( "Completely failed to retransfer: %s" % reTransfer["Message"] )
          failed[lfn][targetSE] = reTransfer["Message"]
          subRequestError = True
      if not failed[lfn]:
        self.info("File %s sucessfully processed at all targetSEs" % lfn )
        requestObj.setSubRequestFileAttributeValue( index, "removal", lfn, "Status", "Done" )

    if not subRequestError:
      self.info("All files processed successfully, setting subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "removal", "Done" )
      
    return S_OK( requestObj )
  
