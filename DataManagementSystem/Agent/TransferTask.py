########################################################################
# $HeadURL $
# File: TransferTask.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/11/08 15:32:07
########################################################################

""" :mod: TransferTask 
    =======================
 
    .. module: TransferTask
    :synopsis: transfer request processing
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    transfer request processing
"""

__RCSID__ = "$Id $"

##
# @file TransferTask.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/11/08 15:32:30
# @brief Definition of TransferTask class.

## imports 
from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask

########################################################################
class TransferTask( RequestTask ):
  """
  .. class:: TransferTask
  
  """

  def __init__( self, *args, **kwargs ):
    """c'tor

    :param self: self reference
    :param list args: args list
    :param dict kwargs: args dict
    """
    ## parent class init
    RequestTask.__init__( self, *args, **kwargs )
    ## set request type
    self.setRequestType( "transfer" )
    ## operation handlers
    self.addOperationAction( "putAndRegister", self.putAndRegister )
    self.addOperationAction( "replicateAndRegister", self.replicateAndRegister )

  def putAndRegister( self, index, requestObj, subAttrs, subFiles ):
    """ putAndRegister operation processing

    :param self: self reference
    :param int index: execution order
    :param RequestContainer requestObj: request object
    :param dict subAttrs: sub-request attributes 
    :param dict subFiles: sub-request files
    :return: S_OK( requestObj ) or S_ERROR
    """
    ## holder for error
    subRequestError = ""
    ## list of targetSEs
    targetSEs = list(set([ targetSE.strip() for targetSE in subAttrs["TargetSE"].split(",")]))
    if len(targetSEs) != 1:
      self.error("putAndRegister: wrong value for TargetSE list = %s, should contain one target!" % targetSEs )
      ## TODO set Failed status!
      return S_ERROR( "putAndRegister: TargetSE should contain one target, got %s" % targetSEs )

    targetSE = targetSEs[0]
    ## dict for failed LFNs
    failed = {}

    catalog = ""
    if "Catalogue" in subAttrs and subAttrs["Catalogue"]:
      catalog = subAttrs["Catalogue"]

    for subRequestFile in subFiles:
      lfn = subRequestFile["LFN"]
      failed.setdefault( lfn, {} )

      self.info("putAndRegister: processing file %s" % lfn )
      if subRequestFile["Status"] != "Waiting":
        self.info("putAndRegister: skipping file %s, status is %s" % ( lfn,  subRequestFile["Status"] ) )
        continue
      
      self.addMark( "Put and register", 1 )

      pfn = subRequestFile["PFN"] if subRequestFile["PFN"] else ""
      guid = subRequestFile["GUID"] if subRequestFile["GUID"] else ""
      addler = subRequestFile["Addler"] if subRequestFile["Addler"] else ""

      ## missing parameters
      if "" in [ lfn, pfn, guid, addler ]:
        self.error( "putAndRegister: missing parameters %s" % ( ", ".join( [ k for k, v in {"PFN" : pfn, 
                                                                                            "GUID" : guid, 
                                                                                            "Addler" : addler, 
                                                                                            "LFN" : lfn  }.items() 
                                                                             if v in ( "", None ) ] ) ) )
        self.error( "putAndRegister: setting file status to 'WrongParams'")
        requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "WrongParams" )
        continue

      ## call RM at least
      putAndRegister = self.replicaManager().putAndRegister( lfn, 
                                                             pfn, 
                                                             targetSE, 
                                                             guid = guid, 
                                                             checksum = addler, 
                                                             catalog = catalog )
      if putAndRegister["OK"]:

        if lfn in putAndRegister["Value"]["Successful"]:
          
          if "put" not in putAndRegister["Value"]["Successful"][lfn]:

            self.addMark( "Put failed", 1 )
            self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "TransferAgent" )
            self.info( "putAndRegister: failed to put %s to %s." % ( lfn, targetSE ) )
            subRequestError = "Put operation failed for %s to %s" % ( lfn, targetSE )
            failed[lfn][targetSE] = "put failed"
            requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "FailedToPut" )

          elif "register" not in putAndRegister["Value"]["Successful"][lfn]:
              
            self.addMark( "Put successful", 1 )
            self.addMark( "File registration failed", 1 )
            
            self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "TransferAgent" )
            self.dataLoggingClient().addFileRecord( lfn, "RegisterFail", targetSE, "", "TransferAgent" )
            
            putTime = putAndRegister["Value"]["Successful"][lfn]["put"] 
            self.info( "putAndRegister: successfully put %s to %s in %s seconds" % ( lfn, targetSE, putTime ) )
            self.info( "putAndRegister: failed to register %s at %s" % ( lfn, targetSE ) )

            requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "FailedToRegister" )

            subRequestError = "Registration failed for %s at %s" % ( lfn, targetSE )
            fileDict = putAndRegister["Value"]["Failed"][lfn]["register"]
            registerRequestDict = { 
              "Attributes": { 
                "TargetSE": fileDict["TargetSE"], 
                "Operation" : "registerFile" }, 
              "Files" : [ { "LFN" : fileDict["LFN"], 
                            "PFN" : fileDict["PFN"], 
                            "Size" : fileDict["Size"], 
                            "Addler" : fileDict["Addler"], 
                            "GUID" : fileDict["GUID"] } ] }
            self.info( "putAndRegister: setting registration request for failed file" )
            requestObj.addSubRequest( registerRequestDict, "register" )

          else:

            self.addMark( "Put successful", 1 )
            self.addMark( "File registration successful", 1 )
            self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "TransferAgent" )
            self.dataLoggingClient().addFileRecord( lfn, "Register", targetSE, "", "TransferAgent" )
            putTime = putAndRegister["Value"]["Successful"][lfn]["put"]
            self.info( "putAndRegister: successfully put %s to %s in %s seconds" % ( lfn, targetSE, putTime ) )
            registerTime = putAndRegister["Value"]["Successful"][lfn]["register"] 
            self.info( "putAndRegister: successfully registered %s to %s in %s seconds" % ( lfn, 
                                                                                            targetSE, 
                                                                                            registerTime ) )
            
        else:

          self.addMark( "Put failed", 1 )
          self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "TransferAgent" )
          reason = putAndRegister["Value"]["Failed"][lfn]
          self.error( "putAndRegister: failed to put and register file %s at %s: %s" % ( lfn, 
                                                                                         targetSE, 
                                                                                         reason ) )
          requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "Complete file failure" )
          failed[lfn][targetSE] = "Complete file failure"
          subRequestError = "Failed to put and register file."

      else:

        self.addMark( "Put failed", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "TransferAgent" )
        self.error ( "putAndRegister: completely failed to put and register file: %s" % putAndRegister["Message"] )
        requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "RM call failure" )
        subRequestError = "RM call failed"
        failed[lfn][targetSE] = "RM call failed"
        
        if not failed[lfn]:
          self.info("putAndRegister: file %s processed successfully, setting its startus do 'Done'.")
          requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Status", "Done" )
 
    if not subRequestError:
      self.info("putAndRegister: all files processed successfully, setting subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "transfer", "Done" )
    else:
      self.error( subRequestError )
          
    return S_OK( requestObj )

  def replicateAndRegister( self, index, requestObj, subAttrs, subFiles ):
    """ replicateAndRegister operation processing

    :param self: self reference
    :param int index: execution order
    :param RequestContainer requestObj: request object
    :param dict subAttrs: sub-request attributes 
    :param list subFiles: sub-request files
    :param str operation: operation name
    :return: a tuple ( requestObj, modified, subRequestError )
    """    
    # holder for subrequest error
    subRequestError = ""
    ## list of targetSEs
    targetSEs = list(set([ targetSE.strip() for targetSE in subAttrs["TargetSE"].split(",") ]))
    ## dict for failed LFNs
    failed = {}
    sourceSE = subAttrs["SourceSE"] if subAttrs["SourceSE"] != "None" else ""

    for subRequestFile in subFiles:

      lfn = subRequestFile["LFN"]
      failed.setdefault( lfn, {} )
      self.info("replicateAndRegister: processing %s file" % lfn )  

      if subRequestFile["Status"] != "Waiting":
        self.info("replicateAndRegister: skipping %s file, status is %s" % ( lfn, subRequestFile["Status"] ) ) 
        continue
          
      for targetSE in targetSEs:
        self.addMark( "Replicate and register", 1 )
        res = self.replicaManager().replicateAndRegister( lfn, targetSE, sourceSE = sourceSE )
        if res["OK"]:
          if lfn in res["Value"]["Successful"]:
            if "replicate" in res["Value"]["Successful"][lfn]:
              copyTime = res["Value"]["Successful"][lfn]["replicate"] 
              self.info("replicateAndRegister: file %s replicated at %s in %s s." % ( lfn, 
                                                                                    targetSE, 
                                                                                    copyTime ) )
              self.addMark( "Replication successful", 1 )
              if "register" in res["Value"]["Successful"][lfn]:
                self.addMark( "Replica registration successful", 1 )                
                registerTime = res["Value"]["Successful"][lfn]["register"]
                self.info("replicateAndRegister: file %s registered at %s in %s s." % ( lfn, 
                                                                                        targetSE, 
                                                                                        registerTime ))
              else:
                self.addMark( "Replica registration failed", 1 )
                self.info( "replicateAndRegister: failed to register %s at %s." % ( lfn, targetSE ) )
                requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "FailedToRegister" )
                fileDict = res["Value"]["Failed"][lfn]["register"]
                registerRequestDict = {
                  "Attributes" : {
                    "TargetSE" : fileDict["TargetSE"], 
                    "Operation": "registerReplica" }, 
                  "Files": [ {
                      "LFN" : fileDict["LFN"], 
                      "PFN" : fileDict["PFN"] } ] }
                self.info( "replicateAndRegister: adding registration request for failed replica." )
                requestObj.addSubRequest( registerRequestDict, "register" )
            else:
              self.info( "replicateAndRegister: failed to replicate %s to %s." % ( lfn, targetSE ) )
              self.addMark( "Replication failed", 1 )
              requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "FailedToReplicate" )
              subRequestError = "Replication failed for %s at %s" % ( lfn, targetSE )
              failed[lfn][targetSE] = subRequestError
          else:
            self.addMark( "Replication failed", 1 )
            reason = res["Value"]["Failed"][lfn]
            self.error( "replicateAndRegister: failed to replicate and register file %s at %s: %s" % ( lfn, 
                                                                                                       targetSE, 
                                                                                                       reason ) )
            failed[lfn][targetSE] = res["Value"]["Failed"][lfn]
        else:
          self.addMark( "Replication failed", 1 )
          self.error( "replicateAndRegister: ReplicaManager error: %s" % res["Message"] )
          requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "ReplicaMgrFailure" )
          failed[lfn][targetSE] = res["Message"]
          subRequestError = res["Message"]

      if not failed[lfn]:
        self.info("replicateAndRegister: file has been %s successfully processed at all targetSEs" % lfn )
        requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Status", "Done" )

    if not subRequestError:
      self.info("replicateAndRegister: all files processed successfully, will set subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "transfer", "Done" )
    else:
      self.error( subRequestError )

    return S_OK( requestObj )
