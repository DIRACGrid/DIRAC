########################################################################
# $HeadURL $
# File: FixSURLEqTURLPlugin.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/07/01 08:57:26
########################################################################
""" fix Requests and SubRequests for LFN with SourceSURL == TargetSURL
"""
__RCSID__ = "$Id $"

##
# @file FixSURLEqTURLPlugin.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/07/01 08:57:52
# @brief Definition of FixSURLEqTURLPlugin class.

## imports 
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.RequestManagementSystem.DB.RequestDBMySQL import RequestDBMySQL
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.private.FTSCurePlugin import FTSCurePlugin, injectFunction
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from datetime import datetime

########################################################################
class FixSURLEqTURLPlugin( FTSCurePlugin, RequestAgentBase ):
  """
  .. class:: FixSURLEqTURLPlugin
  
  """

  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    FTSCurePlugin.__init__( self )
    RequestAgentBase.__init__( self )
    
    self.log = gLogger.getSubLogger( self.__class__.__name__ )

 
    ## injecting few function to RequestDBMySQL instance
    injectFunction( self.selectFileSourceSURLEqTargetSURL, self.requestDBMySQL(), RequestDBMySQL )
    injectFunction( self.countLFNInFiles, self.requestDBMySQL(), RequestDBMySQL )
    injectFunction( self.deleteFileAndChannel, self.requestDBMySQL(), RequestDBMySQL )  
    injectFunction( self.getRequestForSubRequest, self.requestDBMySQL(), RequestDBMySQL )
    injectFunction( self.selectChannelIDSourceSURLTargetSURL, self.requestDBMySQL(), RequestDBMySQL )

  def selectChannelIDSourceSURLTargetSURL(self, itself, fileID, subRequestID ):
    """ select ChannelID, SourceSURL, TargetSURL, Channel.Status given SubRequestID and FileID and Status like 'Waiting%'
    
    :param self: self reference
    :param itself: patient reference
    :param fileID: Files.FileID
    :param subRequestID: SubRequests.SubRequestID
    
    :return: S_OK( { "SourceSE" : sourceSE, 
                     "TargetSE" : targetSE, 
                     "Status" : status, 
                     "SourceSURLEqTargetSURL" : bool }
    :warn: function has to be injected to RequestDBMySQL instance
    """
    query  = "SELECT ChannelID, SourceSE, SourceSURL, TargetSE, TargetSURL, Channel.Status FROM Channel "
    query += "LEFT JOIN Files ON Channel.FileID = Files.FileID "
    query += "WHERE Files.FileID=%s AND Files.SubRequestID = %s " % ( str(fileID), str(subRequestID ) )
    #query += "AND Channel.Status LIKE 'Waiting%'" 
    
    query = self._query( query )
    if not query["OK"]:
      return query
    ## build return dict
    fileDict = {}
    for channelID, sourceSE, sourceSURL, targetSE, targetSURL, status in query["Value"]:
      SrcEqTgt = bool(sourceSURL == targetSURL)
      fileDict[channelID] = { "SourceSE" : sourceSE, 
                              "TargetSE" : targetSE, 
                              "Status" : status, 
                              "SourceSURLEqTargetSURL" : SrcEqTgt }
        
    return S_OK( fileDict )

  def selectFileSourceSURLEqTargetSURL( self, itself ):
    """ 
    Select one record:
     ( Files.FileID, File.LFN, Channel.ChannelID, Channel.SourceSE,Channel.TargetSE, Files.SubRequestID )
    where Channel.SourceSURL = Channel.TargetSURL, Files.Status = 'Scheduled', Channel.Status = 'Waiting'

    :param self: plugin reference
    :param itself: patient reference for injection
    :warn: function has to be injected to RequestDBMySQL instance
    """
    fileQuery = " ".join( 
      [ "SELECT Files.FileID,Files.LFN,Channel.ChannelID,Channel.SourceSE,Channel.TargetSE,Files.SubRequestID",
        "FROM Files LEFT JOIN Channel ON Files.FileID = Channel.FileID",
        "WHERE Channel.SourceSURL = Channel.TargetSURL AND",
        "Files.Status = 'Scheduled' and Channel.Status = 'Waiting' LIMIT 1;" ] )
    return self._query( fileQuery )

  def getRequestForSubRequest(self, itself, subRequestID ):
    """ 
    Select Request given SubRequestID.

    :param self: plugin reference
    :param itself: patient reference for injection
    :param int subRequestID: SubRequests.SubRequestID
    :warn: function has to be injected to RequestDBMySQL instance

    """

    ## get RequestID
    requestID = "SELECT RequestID FROM SubRequests WHERE SubRequestID = %s;" % str(subRequestID)
    requestID = self._query( requestID )
    if not requestID["OK"]:
      return requestID
    requestID = requestID["Value"][0]

    ## create RequestContainer
    requestContainer = RequestContainer( init = False )
    requestContainer.setRequestID( requestID )
    
    ## put some basic infos in 
    requestInfo  = "SELECT RequestName, JobID, OwnerDN, OwnerGroup, DIRACSetup, SourceComponent, CreationTime, SubmissionTime, LastUpdate, Status "
    requestInfo += "FROM Requests WHERE RequestID = %d;" % requestID
    requestInfo = self._query( requestInfo )
    if not requestInfo["OK"]:
      return requestInfo

    requestName, jobID, ownerDN, ownerGroup, diracSetup, sourceComponent, creationTime, submissionTime, lastUpdate, status = requestInfo['Value'][0]
    requestContainer.setRequestName( requestName )
    requestContainer.setJobID( jobID )
    requestContainer.setOwnerDN( ownerDN )
    requestContainer.setOwnerGroup( ownerGroup )
    requestContainer.setDIRACSetup( diracSetup )
    requestContainer.setSourceComponent( sourceComponent )
    requestContainer.setCreationTime( str( creationTime ) )
    requestContainer.setLastUpdate( str( lastUpdate ) )
    requestContainer.setStatus( status )
    
    ## get sub-requests 
    subRequests = "SELECT SubRequestID, Status, RequestType, Operation, Arguments, ExecutionOrder, SourceSE, "
    subRequests += "TargetSE, Catalogue, CreationTime, SubmissionTime, LastUpdate FROM SubRequests WHERE RequestID=%s;" % requestID
    subRequests = self._query( subRequests )
    if not  subRequests["OK"]:
      return subRequests
    ## loop over sub requests
    for subRequestID, status, requestType, operation, arguments, executionOrder, sourceSE, targetSE, catalogue, creationTime, submissionTime, lastUpdate in  subRequests["Value"]:
      res = requestContainer.initiateSubRequest( requestType )
      ind = res["Value"]
      subRequestDict = { "Status" : status, "SubRequestID"  : subRequestID, "Operation" : operation, "Arguments" : arguments,
                         "ExecutionOrder" : int( executionOrder ), "SourceSE" : sourceSE, "TargetSE" : targetSE,
                         "Catalogue" : catalogue, "CreationTime" : creationTime, "SubmissionTime" : submissionTime,
                         "LastUpdate" : lastUpdate }
      res = requestContainer.setSubRequestAttributes( ind, requestType, subRequestDict )
      if not res["OK"]:
        return res
    
      ## get files for this subrequest
      req = "SELECT FileID, LFN, Size, PFN, GUID, Md5, Addler, Attempt, Status FROM Files WHERE SubRequestID = %s ORDER BY FileID;" % str(subRequestID)
      res = self._query( req )
      if not res["OK"]:
        return res
      files = []
      for fileID, lfn, size, pfn, guid, md5, addler, attempt, status in res["Value"]:
        fileDict = { "FileID" : fileID, "LFN" : lfn, "Size" : size, 
                     "PFN" : pfn, "GUID" : guid, "Md5" : md5, 
                     "Addler" : addler, "Attempt" : attempt, 
                     "Status" : status }
        files.append( fileDict )
      res = requestContainer.setSubRequestFiles( ind, requestType, files )
      if not res["OK"]:
        return res

    ## dump request to XML
    res = requestContainer.toXML()
    if not res["OK"]:
      return res
    requestString = res["Value"]
    
    ## return dictonary with all info in at least
    return S_OK( { 
      "RequestName" : requestName,
      "RequestString" : requestString,
      "JobID" : jobID,
      "RequestContainer" : requestContainer 
      } )
    
  def countLFNInFiles( self, itself, fileLFN ):
    """ sum up all enties in RequestDB.Files table for given LFN

    :param self: plugin reference
    :param itself: patient reference (for injection)
    :param str fileLFN: Files.LFN
    :warn: function has to be injected to RequestDBMySQL instance 
    """
    return self._query( "SELECT COUNT(*) FROM Files WHERE LFN = '%s';" % fileLFN )

  def deleteFileAndChannel( self, itself, fileID, channelID ):
    """ delete RequestDB.Files record given FileID together with RequestDB.Channel record given FileID and ChannelID

    :param self: plugin reference
    :param itself: patient reference for injection
    :param int fileID: Files.FileID
    :param int channelID: Channel.ChannelID

    :warn: function has to be injected to RequestDBMySQL instance 
    """
    deleteFiles = "DELETE FROM Files where FileID = %s; " % str(fileID)
    deleteChannel = "DELETE FROM Channel where FileID = %s and ChannelID = %s;" % ( str(fileID), str(channelID) )
    delete = self._query( deleteFiles + deleteChannel )
    return delete

  def selectSubRequestFiles( self, itself, subRequestID ):
    """
    not used, could be removed???
    
    :param self: self reference
    :param itself: patient reference for injection
    :param int subRequestID: SubRequests.SubRequestID
    :return: S_OK with  "Value" : ( ( Files.FileID, Files.LFN, Files.Status, 
    Channel.TargetSE, Channel.ChannelID, Channel.Status ), ...) ) or S_ERROR
    :warn: function has to be injected to RequestDBMySQL instance 
    """

    subRequestFiles = "SELECT Files.FileID,Files.LFN,Files.Status,Channel.ChanneID,Channel.TargetSE,Channel.Status FROM Files LEFT JOIN Channel ON Files.FileID = Channel.FileID WHERE SubRequestID = %d;" % subRequestID
    return self._query( subRequestFiles )



  def selectFileToFTSStatus( self, itself, fileIDList ):
    """ collect FileToFTS.Status for each file in SubRequest, given list of FileIDs

    not used, could be removed??

    :param self: self reference
    :param itself: patient reference for injection
    :param list fileIdList: Files.FileID
    :warn: function has to be injected to RequestDBMySQL instance 
    """
    ftsFileStatus = "SELECT FileID, Status, ChannelID FROM FileToFTS WHERE FileID IN ( %s );" % ",".join( [ str(fileID) for fileID in fileIDList ] ) 
    return self._query( ftsFileStatus )

  def execute( self ):
    """ main worker here, execute plugin
  
    :param self: self reference
    """

    ret = self.requestDBMySQL().selectFileSourceSURLEqTargetSURL()
    if not ret["OK"]:
      self.log.error("Unable to select Files with SourceSURL=TargetSURL: %s" % ret["Message"] )
      return ret

    if not ret["Value"]:
      self.log.info("No valid records found in this cycle.")
      return S_OK()

    ## unpack record
    self.log.info("Found FileID=%d LFN=%s ChannelId=%d SourceSE=%s TargetSE=%s SubRequestID=%d" % ret["Value"] )
    ## SourceURL = TargetURL file 
    pFileID, pFileLFN, pChannelID, pSourceSE, pTargetSE, pSubRequestID = ret["Value"]

    ## sub-request not set? 
    if pSubRequestID == 0:
      self.log.info("SubRequestID = 0, will count LFN entries...")
      ret = self.requestDBMySQL().countLFNInFiles( pFileLFN )
      if not ret["OK"]:
        self.log.error("Unable to count LFN entires in Files for LFN %s: %s" %( pFileLFN,  ret["Message"] ) )
        return ret
      ## only one entry? - delete Files and Channel records 
      if ret["Value"][0] == 1:
        self.log.info("Only one entry for LFN=%s found in Files, about to delete its Files and Channel records..." % pFileLFN )
        ret = self.requestDBMySQL().deleteFileAndChannel( pFileID, pChannelID )
        if not ret["OK"]:
          self.log.error("Unable to delete Files and Channel records: ", ret["Message"] )
        return ret

    ## we've got SubRequestID 
    ## get all files for this subRequest
    ## get Request

    requestDict = self.requestDBMySQL().getRequestForSubRequest( pSubRequestID )
    if not ret["OK"]:
      self.log.error("Unable to select Request for given SubRequest: %s" % ret["Message"] )
      return ret

    requestDict = requestDict["Value"]
    requestObj = requestDict["RequestContainer"]
    requestName = requestDict["RequestName"]
    requestString = requestDict["RequestString"]
    executionOrder = self.requestClient().getCurrentExecutionOrder( requestName )
    jobID = requestDict["JobID"] if requestDict["JobID"] else 0

    ## get nb of SubRequests
    nbSubRequest = requestObj.getNumSubRequests( "transfer" )
    if not nbSubRequest["OK"]:
      return nbSubRequest
    nbSubRequest = nbSubRequest["Value"]
    
    ## loop over SubRequests
    for index in range( nbSubRequest ):
      subRequestAttrs = requestObj.getSubRequestAttributes( index, "transfer" )["Value"]
      
      if subRequestAttrs["ExecutionOrder"]:
        subExecutionOrder = int( subRequestAttrs["ExecutionOrder"] )
      else:
        subExecutionOrder = 0
      subRequestStatus = subRequestAttrs["Status"]
      if subRequestStatus == "Waiting" and subExecutionOrder <= executionOrder:
        operation = subRequestAttrs["Operation"]
        
      ## get SubRequestID
      subRequestID = subRequestAttrs["SubRequestID"]

      ## is it problematic one? we didn't touch any others
      if subRequestID != pSubRequestID: 
        continue

      ## get SubRequest files
      subRequestFiles = requestObj.getSubRequestFiles( index, "transfer" )["Value"]      
      ## good fileIDs
      completedFileIDs = []
      ## bad fileIDs
      failedFileIDs = []
      ## loop over subRequest files
      for subRequestFile in subRequestFiles:
        
        ## get LFN, fileID, targetSE
        fileLFN = subRequestFile["LFN"]
        fileID = subRequestFile["FileID"]
        targetSEs = [ targetSE.strip() for targetSE in subRequestAttrs["TargetSE"].split(",") ]

        ## get all replicas
        replicas = self.replicaManager().getReplicas( [ fileLFN ] )        
        if not replicas["OK"]:
          return replicas

        replicas = replicas["Value"]
        if replicas["Failed"]:
          return S_ERROR("Failed to determine all replicas for LFN=%s" % fileLFN )
        replicas = replicas["Successful"] 
            
        ## get channel info 
        channelInfo = self.requestDBMySQL().selectChannelIDSourceSURLTargetSURL( fileID, subRequestID )
        if not channelInfo["OK"]:
          return channelInfo

        if not channelInfo["Value"]:
          return S_OK("No channel info available for FileID=%s in SubRequestID=%s" % ( str(fileID), str(subRequestID) ) )

        channelInfo = channelInfo["Value"]

        ## loop over channels
        for channelID in channelInfo:
          ## targetSE is in replicas?
          if channelInfo[channelID]["Status"] == "Done":
            requestObj.setSubRequestFileAttributeValue( index, "transfer", fileLFN, "Status", "Done" )
            #self.transferDB().setFileChannelStatus( channelID, fileID, "Done" )
            completedFileIDs.append( fileID )
          elif channelInfo[channelID]["Status"] == "Failed":
            requestObj.setSubRequestFileAttributeValue( index, "transfer", fileLFN, "Status", "Failed" )
            #self.transferDB().setFileChannelStatus( channelID, fileID, "Failed" )
            failedFileIDs.append( fileID )
          ## now only Waiting% are left, check if targetSE is in replicas?
          elif channelInfo[channelID]["TargetSE"] in replicas:
            self.log.info( "LFN=%s is already replicated in %s, setting it's status to 'Done'" % ( fileLFN, channelInfo["TargetSE"] ) )
            requestObj.setSubRequestFileAttributeValue( index, "transfer", fileLFN, "Status", "Done" )
            self.transferDB().setFileChannelStatus( channelID, fileID, "Done" )
            completedFileIDs.append( fileID )
          else:
            ## nope, it isn't, so maybe SourceSURL = TargeSURL for this file 
            if channelInfo[channelID]["SourceSURLEqTargetSURL"]:
              self.log.info( "LFN=%s has SourceSURL=TargetSURL, setting it's status to 'Done'" % fileLFN, str(targetSEs) )
              requestObj.setSubRequestFileAttributeValue( index, "transfer", fileLFN, "Status", "Done" )
              self.transferDB().setFileChannelStatus( channelID, fileID, "Done" )
              completedFileIDs.append( fileID )
            else:
              ## nope, we will write off this file (will set Status = 'Failed' everywhere)
              self.log.info("LFN=%s is not replicated at %s, will set its status to 'Failed'." % ( fileLFN, str(targetSEs) ) )
              requestObj.setSubRequestFileAttributeValue( index, "transfer", fileLFN, "Status", "Failed" )
              self.transferDB().setFileChannelStatus( channelID, fileID, "Failed" )
              failedFileIDs.append( fileID )
      
      ## all files processed succesfully? 
      if ( len(completedFileIDs) == len(subRequestFiles) ):
        requestObj.setSubRequestStatus( index, "transfer", "Done" )
        if  nbSubRequest == 1:
          requestObj.setStatus( "Done" ) 
      else:
      ## there are some failed files, so  SubRequest status is 'Failed'
        requestObj.setSubRequestStatus( index, "transfer", "Failed" )
        if  nbSubRequest == 1:
          requestObj.setStatus( "Failed" ) 

    ## update request if modified
    newRequestString = requestObj.toXML()['Value']
    if requestString != newRequestString:
      update = self.requestClient().updateRequest( requestName, newRequestString )
      if not update["OK"]:
        self.log.error("Failed to update request %s: %s" % ( requestName, update["Message"] ) )
        return update
      

      ## some files had been modified and only one SubRequest in Request: finalize Request 
      if nbSubRequest == 1 and jobID:
        finalize = self.requestClient().finalizeRequest( requestName, jobID )
        if not finalize["OK"]:
          self.log.error("Unable to finalize Request %s: %s"  %( requestName, finalize["Message"] ) )
          return finalize

    ## if we're here processing was OK
    return S_OK()
