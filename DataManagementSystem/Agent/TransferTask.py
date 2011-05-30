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
class TransferTask(RequestTask):
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
    RequestTask.__init__( self, args, *kwargs )
    ## set request type
    self.setRequestType( "transfer" )
    ## operation handlers
    self.addOperationAction( "put", self.put )
    self.addOperationAction( "putAndRegister", self.putAndRegister )
    self.addOperationAction( "putAndRegisterAndRemove", self.putAndRegister )
    self.addOperationAction( "replicate", self.replicate )
    self.addOperationAction( "replicateAndRegister", self.replicateAndRegister )
    self.addOperationAction( "replicateAndRegisterAndRemove", self.replicateAndRegister )
    self.addOperationAction( "get", self.get )

  def put( self, index, requestObj, subAttrs, subFiles ):
    """ put operation processing
    
    :param self: self reference
    :param int index: execution order
    :param RequestContainer requestObj: request object
    :param dict subAttrs: sub-request attributes 
    :param list subFiles: sub-request files
    :return: S_OK( requestObj ) 
    """
    ## holder for error
    subRequestError = ""    
    ## list ot targetSEs
    targetSEs = list(set([ targetSE.strip() for targetSE in subAttrs["TargetSE"].split(",")]))
    ## dict for failed LFNs
    failed = {}

    for subRequestFile in subFiles:
      lfn = subRequestFile["LFN"]
      failed.setdefault( lfn, {} )
      self.info("Processing file %s" % lfn )
        
      if subRequestFile["Status"] != "Waiting":
        self.info("Skipping file %s, status is %s" % ( lfn, subRequestFile["Status"] ) )
        continue
      self.addMark( "Put", 1 ) 

      for targetSE in targetSEs:
        pfn = subRequestFile["PFN"]
        put = self.replicaManager().put( lfn, pfn, targetSE )
        if put["OK"]:
          if lfn in put["Value"]["Successful"]:
            self.addMark( "Put successful", 1 )
            self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "TransferAgent" )
            putTime = put["Value"]["Successful"][lfn] 
            self.info( "Successfully put %s to %s in %s seconds." % ( lfn, targetSE, putTime ) )
          else:
            self.addMark( "Put failed", 1 )
            self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "TransferAgent" )
            self.error( "Failed to put file %s at %s: %s" % ( lfn, 
                                                              targetSE, 
                                                              put["Value"]["Failed"][lfn] ) )
            subRequestError = "Put operation failed for %s to %s" % ( lfn, targetSE )
            failed[lfn][targetSE] = "Put failed"
            requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "Put failed" )
        else:
          self.addMark( "Put failed", 1 )
          self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "TransferAgent" )
          self.error( "Completely failed to put file %s at %s: %s" % ( lfn, targetSE, put["Message"] ) )
          subRequestError = "Put RM call failed for %s to %s" % ( lfn, targetSE )
          requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "Put RM call failed" )
          failed[lfn][targetSE] = put["Message"]
          
      if not failed[lfn]:
        self.info("File %s has been processed sucessfully at all targetSEs." % lfn )
        requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Status", "Done" )

    if not subRequestError:
      self.info("All files processed successfully, setting subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "transfer", "Done" )
    else:
      self.error( subRequestError )

    return S_OK( requestObj )

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
    ## dict for failed LFNs
    failed = {}

    catalog = ""
    if "Catalogue" in subAttrs:
      catalog = subAttrs["Catalogue"]

    for subRequestFile in subFiles:
      lfn = subRequestFile["LFN"]
      failed.setdefault( lfn, {} )

      self.info("Processing file %s" % lfn )
      if subRequestFile["Status"] != "Waiting":
        self.info("Skipping file %s, status is %s" % ( lfn,  subRequestFile["Status"] ) )
        continue
      
      for targetSE in targetSEs:
        self.addMark( "Put and register", 1 )
        pfn = subRequestFile["PFN"]
        guid = subRequestFile["GUID"]
        addler = subRequestFile["Addler"]
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
              self.info( "Failed to put %s to %s." % ( lfn, targetSE ) )
              subRequestError = "Put operation failed for %s to %s" % ( lfn, targetSE )
              failed[lfn][targetSE] = "put failed"
              requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "Put failed" )

            elif "register" not in putAndRegister["Value"]["Successful"][lfn]:
              
              self.addMark( "Put successful", 1 )
              self.addMark( "File registration failed", 1 )

              self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "TransferAgent" )
              self.dataLoggingClient().addFileRecord( lfn, "RegisterFail", targetSE, "", "TransferAgent" )
      
              putTime = putAndRegister["Value"]["Successful"][lfn]["put"] 
              self.info( "Successfully put %s to %s in %s seconds." % ( lfn, targetSE, putTime ) )
              self.info( "Failed to register %s at %s." % ( lfn, targetSE ) )

              requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "Registration failed" )

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
              self.info( "Setting registration request for failed file." )
              requestObj.addSubRequest( registerRequestDict, "register" )
            
            else:

              self.addMark( "Put successful", 1 )
              self.addMark( "File registration successful", 1 )
              self.dataLoggingClient().addFileRecord( lfn, "Put", targetSE, "", "TransferAgent" )
              self.dataLoggingClient().addFileRecord( lfn, "Register", targetSE, "", "TransferAgent" )
              putTime = putAndRegister["Value"]["Successful"][lfn]["put"]
              self.info( "Successfully put %s to %s in %s seconds." % ( lfn, targetSE, putTime ) )
              registerTime = putAndRegister["Value"]["Successful"][lfn]["register"] 
              self.info( "Successfully registered %s to %s in %s seconds." % ( lfn, targetSE, registerTime ) )

              #requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Status", "Done" )
          else:

            self.addMark( "Put failed", 1 )
            self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "TransferAgent" )
            self.error( "Failed to put and register file %s at %s: %s" % ( lfn, 
                                                                           targetSE, 
                                                                           putAndRegister["Value"]["Failed"][lfn] ) )
            requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "Complete file failure" )
            failed[lfn][targetSE] = "Complete file failure"
            subRequestError = "Failed to put and register file."

        else:

          self.addMark( "Put failed", 1 )
          self.dataLoggingClient().addFileRecord( lfn, "PutFail", targetSE, "", "TransferAgent" )
          self.error ( "Completely failed to put and register file: %s" % putAndRegister["Message"] )
          requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "RM call failure" )
          subRequestError = "RM call file"

      if not failed[lfn]:
        self.info("File %s processed sucesfull at all targetSEs.")
        requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Status", "Done" )
 
    if not subRequestError:
      self.info("All files processed successfully, setting subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "transfer", "Done" )
    else:
      self.error( subRequestError )
          
    return S_OK( requestObj )

  def replicate( self, index, requestObj, subAttrs, subFiles ):
    """ replicate operation processing

    :param self: self reference
    :param int index: execution order
    :param RequestContainer requestObj: request object
    :param dict subAttrs: sub-request attributes 
    :param list subFiles: sub-request files
    :return: S_OK ( requestObj )
    """
    ## holder for subrequest error
    subRequestError = ""
    ## list of targetSEs
    targetSEs = list(set([ targetSE.strip() for targetSE in subAttrs["TargetSE"].split(",")]))
    ## dict for failed LFNs
    failed = {}

    sourceSE = subAttrs["SourceSE"]

    for subRequestFile in subFiles:
      lfn = subRequestFile["LFN"]
      failed.setdefault( lfn, {} )
      self.info( "Processing file %s" % lfn )

      if subRequestFile["Status"] != "Waiting":
        self.info( "Skipping file %s, status is '%s'" % ( lfn, subRequestFile["Status"] ) )
        continue
      
      for targetSE in targetSEs:
        self.addMark( "Replicate", 1 )
        res = self.replicaManager().replicate( lfn, targetSE, sourceSE = sourceSE )
        if res["OK"]:
          if lfn in res["Value"]["Successful"]:
            self.addMark( "Replication successful", 1 )
            replicaTime = res['Value']['Successful'][lfn]
            self.info( "Successfully replicated %s to %s in %s seconds." % ( lfn, targetSE, replicaTime ) )
          else:            
            self.addMark( "Replication failed", 1 )
            self.error( "Failed to replicate file %s at %s: %s" % ( lfn, 
                                                                    targetSE, 
                                                                    res["Value"]["Failed"][lfn] ) )
            requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "Replicate failed" )
            failed[lfn][targetSE] = "Replicate failed"
            subRequestError = "Replicate failed"
        else:
          self.addMark( "Replication failed", 1 )
          self.error( "Completely failed to replicate file.", res["Message"] ) 
          failed[lfn][targetSE] = res["Message"]
          requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "Replicate RM call failed" )
          subRequestError = "RM call failed"
      
      ## all targetSEs processed, set status to 'Done' if not failed
      if not failed[lfn]:
        self.info( "File %s processed sucessfully at all targetSEs, setting its status to 'Done'" % lfn )
        requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Status", "Done" )
        
    if not subRequestError:
      self.info("All files processed successfully, will set subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "replicate", "Done" )
    else:
      self.error( subRequestError )

      #self.info("Some replications have failed, will set subrequest status to 'Error'")
      #requestObj.setSubRequestStatus( index, "replicate", "Error" )
      
    return S_OK( requestObj )

  def replicateAndRegister( self, index, requestObj, subAttrs, subFiles ):
    """ replicateAndRegsiter operation processing

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
      self.info("Processing %s file" % lfn )  

      if subRequestFile["Status"] != "Waiting":
        self.info("Skipping %s file, status is %s" % ( lfn, subRequestFile["Status"] ) ) 
        continue
          
      for targetSE in targetSEs:
        self.addMark( "Replicate and register", 1 )
        res = self.replicaManager().replicateAndRegister( lfn, targetSE, sourceSE = sourceSE )
        if res["OK"]:
          if lfn in res["Value"]["Successful"]:
            if "replicate" in res["Value"]["Successful"][lfn]:
              self.info("File %s replicated at %s in %s s." % ( lfn, 
                                                                targetSE, 
                                                                res["Value"]["Successful"][lfn]["replicate"] ) )
              self.addMark( "Replication successful", 1 )
              if "register" in res["Value"]["Successful"][lfn]:
                self.addMark( "Replica registration successful", 1 )                
                self.info("File %s registered at %s in %s s." % ( lfn, 
                                                                  targetSE, 
                                                                  res["Value"]["Successful"][lfn]["register"] ))
              else:
                self.addMark( "Replica registration failed", 1 )
                self.info( "Failed to register %s at %s." % ( lfn, targetSE ) )
                requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "Registration failed" )
                fileDict = res["Value"]["Failed"][lfn]["register"]
                registerRequestDict = {
                  "Attributes" : {
                    "TargetSE" : fileDict["TargetSE"], 
                    "Operation": "registerReplica" }, 
                  "Files": [ {
                      "LFN" : fileDict["LFN"], 
                      "PFN" : fileDict["PFN"] } ] }
                self.info( "Setting registration request for failed replica." )
                requestObj.addSubRequest( registerRequestDict, "register" )
            else:
              self.info( "Failed to replicate %s to %s." % ( lfn, targetSE ) )
              self.addMark( "Replication failed", 1 )
              requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "Replication failed" )
              subRequestError = "Replication failed for %s at %s" % ( lfn, targetSE )
              failed[lfn][targetSE] = subRequestError
          else:
            self.addMark( "Replication failed", 1 )
            self.error( "Failed to replicate and register file %s at %s: %s" % ( lfn, 
                                                                                 targetSE, 
                                                                                 res["Value"]["Failed"][lfn] ) )
            failed[lfn][targetSE] = res["Value"]["Failed"][lfn]
        else:
          self.addMark( "Replication failed", 1 )
          self.error( "Completely failed to replicate and register file: %s" % res["Message"] )
          requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", "RM call failure" )
          failed[lfn][targetSE] = res["Message"]
          subRequestError = res["Message"]

      if not failed[lfn]:
        self.info("File %s successfully processed at all targetSEs.")
        requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Status", "Done" )

    if not subRequestError:
      self.info("All files processed successfully, will set subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "transfer", "Done" )
    else:
      self.error( subRequestError )

      
    return S_OK( requestObj )
  
  def get( self, index, requestObj, subAttrs, subFiles ):
    """ get operation processing

    :param self: self reference
    :param int index: current execution order index
    :param RequestContainer requestObj: request object
    :param dict subAttrs: sub-request attributes 
    :param list subFiles: sub-request files
    :return: S_ERROR or S_OK( requestObj )
    """
    ## holder for subrequest error
    subRequestError = ""
    ## list of targetSEs
    targetSEs = list(set([targetSE.strip() for targetSE in subAttrs["TargetSE"].split(",")]))
    # dict for failed LFNs
    failed = {}

    for subRequestFile in subFiles:
      lfn = subRequestFile["LFN"]
      failed.setdefault( lfn, {} )
      self.info("Processing file %s" % lfn )
      if subRequestFile["Status"] != "Waiting":
        self.info( "Skipping file %s, status is %s" % ( lfn, subRequestFile["Status"] ) )
        continue
      pfn = subRequestFile["PFN"]
      get = { "OK" : False, "Message" : "" }
      if pfn:
        for targetSE in targetSEs:
          get = self.replicaManager().getStorageFile( pfn, targetSE )
          if get["OK"] and pfn in get["Value"]["Successful"]:
            self.info("Sucessfully get %s at %s" % ( pfn, targetSE) )
          else:
            self.error( "Failed to get %s file at %s: %s" % ( lfn, targetSE, get["Message"] ) )
            failed[lfn][targetSE] = get["Message"]
      else:
        get = self.replicaManager().getFile( lfn )
        if get["OK"] and lfn in get["Value"]["Successful"]: 
          self.info( "Successfully got %s" % ( lfn ) )
        else:
          self.error( "Failed to get %s file: %s" % ( lfn, get["Message"] ) )
          failed[lfn][lfn] = get["Message"]
          subRequestError = get["Message"]

      if not failed[lfn]:
        self.info("File %s sucessfully processed at all targetSEs")
        requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Status", "Done" )

    if not subRequestError:
      self.info("All files processed successfully, seting subrequest status to 'Done'.")
      requestObj.setSubRequestStatus( index, "transfer", "Done" )
    else:
      self.error( subRequestError )
      
    return S_OK( requestObj )
