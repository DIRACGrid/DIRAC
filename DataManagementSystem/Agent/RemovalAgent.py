########################################################################
# $HeadURL$
########################################################################
"""  RemovalAgent takes removal requests from the RequestDB and executes them
"""

from DIRAC  import gLogger, gMonitor, S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool, ThreadedJob
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.RequestManagementSystem.Agent.RequestAgentMixIn import RequestAgentMixIn
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

import re, os
from types import StringTypes

__RCSID__ = "$Id$"

AGENT_NAME = 'DataManagement/RemovalAgent'

class RemovalAgent( AgentModule, RequestAgentMixIn ):
  """
    This Agent takes care of executing "removal" request from the RequestManagement system
  """

  def __init__( self, *args ):
    """
    Initialize the base class and define some extra data members
    """
    AgentModule.__init__( self, *args )
    self.requestDBClient = None
    self.replicaManager = None
    self.maxNumberOfThreads = 4
    self.maxRequestsInQueue = 100
    self.threadPool = None

  def initialize( self ):
    """
      Called by the framework upon startup, before any cycle (execute method bellow)
    """
    self.requestDBClient = RequestClient()
    self.replicaManager = ReplicaManager()

    gMonitor.registerActivity( "Iteration", "Agent Loops", "RemovalAgent", "Loops/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Execute", "Request Processed", "RemovalAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Done", "Request Completed", "RemovalAgent", "Requests/min", gMonitor.OP_SUM )

    gMonitor.registerActivity( "PhysicalRemovalAtt", "Physical removals attempted",
                               "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "PhysicalRemovalDone", "Successful physical removals",
                               "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "PhysicalRemovalFail", "Failed physical removals",
                               "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "PhysicalRemovalSize", "Physically removed size",
                               "RemovalAgent", "Bytes", gMonitor.OP_ACUM )

    gMonitor.registerActivity( "ReplicaRemovalAtt", "Replica removal attempted",
                               "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "ReplicaRemovalDone", "Successful replica removals",
                               "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "ReplicaRemovalFail", "Failed replica removals",
                               "RemovalAgent", "Removal/min", gMonitor.OP_SUM )

    gMonitor.registerActivity( "RemoveFileAtt", "File removal attempted",
                               "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveFileDone", "File removal done",
                               "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveFileFail", "File removal failed",
                               "RemovalAgent", "Removal/min", gMonitor.OP_SUM )

    self.maxNumberOfThreads = self.am_getOption( 'NumberOfThreads', self.maxNumberOfThreads )
    self.maxRequestsInQueue = self.am_getOption( 'RequestsInQueue', self.maxRequestsInQueue )
    self.threadPool = ThreadPool( 1, self.maxNumberOfThreads, self.maxRequestsInQueue )

    # Set the ThreadPool in daemon mode to process new ThreadedJobs as they are inserted
    self.threadPool.daemonize()

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    return S_OK()

  def execute( self ):
    """
    Fill the TreadPool with ThreadJobs
    """
    while True:
      requestExecutor = ThreadedJob( self.executeRequest )
      ret = self.threadPool.queueJob( requestExecutor )
      if not ret['OK']:
        break

    return S_OK()

  def executeRequest( self ):
    """
    Do the actual work in the Thread
    """
    ################################################
    # Get a request from request DB
    gMonitor.addMark( "Iteration", 1 )
    res = self.requestDBClient.getRequest( 'removal' )
    if not res['OK']:
      gLogger.info( "RemovalAgent.execute: Failed to get request from database." )
      return S_OK()
    elif not res['Value']:
      gLogger.info( "RemovalAgent.execute: No requests to be executed found." )
      return S_OK()
    requestString = res['Value']['RequestString']
    requestName = res['Value']['RequestName']
    sourceServer = res['Value']['Server']
    try:
      jobID = int( res['Value']['JobID'] )
    except ValueError:
      jobID = 0
    gLogger.info( "RemovalAgent.execute: Obtained request %s" % requestName )

    try:

      result = self.requestDBClient.getCurrentExecutionOrder( requestName, sourceServer )
      if result['OK']:
        currentOrder = result['Value']
      else:
        gLogger.error( 'Can not get the request execution order' )
        self.requestDBClient.updateRequest( requestName, requestString, sourceServer )
        return S_OK( 'Can not get the request execution order' )

      oRequest = RequestContainer( request = requestString )

      ################################################
      # Find the number of sub-requests from the request
      res = oRequest.getNumSubRequests( 'removal' )
      if not res['OK']:
        errStr = "RemovalAgent.execute: Failed to obtain number of removal subrequests."
        gLogger.error( errStr, res['Message'] )
        return S_OK()
      gLogger.info( "RemovalAgent.execute: Found %s sub requests." % res['Value'] )

      ################################################
      # For all the sub-requests in the request
      modified = False
      for ind in range( res['Value'] ):
        gMonitor.addMark( "Execute", 1 )
        gLogger.info( "RemovalAgent.execute: Processing sub-request %s." % ind )
        subRequestAttributes = oRequest.getSubRequestAttributes( ind, 'removal' )['Value']
        subExecutionOrder = int( subRequestAttributes['ExecutionOrder'] )
        subStatus = subRequestAttributes['Status']
        if subStatus == 'Waiting' and subExecutionOrder <= currentOrder:
          subRequestFiles = oRequest.getSubRequestFiles( ind, 'removal' )['Value']
          operation = subRequestAttributes['Operation']

          ################################################
          #  If the sub-request is a physical removal operation
          if operation == 'physicalRemoval':
            gLogger.info( "RemovalAgent.execute: Attempting to execute %s sub-request." % operation )
            diracSEs = subRequestAttributes['TargetSE'].split( ',' )
            physicalFiles = []
            pfnToLfn = {}
            for subRequestFile in subRequestFiles:
              if subRequestFile['Status'] == 'Waiting':
                pfn = str( subRequestFile['PFN'] )
                lfn = str( subRequestFile['LFN'] )
                pfnToLfn[pfn] = lfn
                physicalFiles.append( pfn )
            gMonitor.addMark( 'PhysicalRemovalAtt', len( physicalFiles ) )
            failed = {}
            errMsg = {}
            for diracSE in diracSEs:
              res = self.replicaManager.removeStorageFile( physicalFiles, diracSE )
              if res['OK']:
                for pfn in res['Value']['Failed'].keys():
                  if not failed.has_key( pfn ):
                    failed[pfn] = {}
                  failed[pfn][diracSE] = res['Value']['Failed'][pfn]
              else:
                errMsg[diracSE] = res['Message']
                for pfn in physicalFiles:
                  if not failed.has_key( pfn ):
                    failed[pfn] = {}
                  failed[pfn][diracSE] = 'Completely'
            # Now analyse the results
            failedPFNs = failed.keys()
            pfnsOK = [pfn for pfn in physicalFiles if not pfn in failedPFNs]
            gMonitor.addMark( 'PhysicalRemovalDone', len( pfnsOK ) )
            for pfn in pfnsOK:
              gLogger.info( "RemovalAgent.execute: Successfully removed %s at %s" % ( pfn, str( diracSEs ) ) )
              res = oRequest.setSubRequestFileAttributeValue( ind, 'removal', pfnToLfn[pfn], 'Status', 'Done' )
              if not res['OK']:
                gLogger.error( "RemovalAgent.execute: Error setting status to %s for %s" % ( 'Done', pfnToLfn[pfn] ) )
              modified = True
            if failed:
              gMonitor.addMark( 'PhysicalRemovalFail', len( failedPFNs ) )
              for pfn in failedPFNs:
                for diracSE in failed[pfn].keys():
                  if type( failed[pfn][diracSE] ) in StringTypes:
                    if re.search( 'no such file or directory', failed[pfn][diracSE].lower() ):
                      gLogger.info( "RemovalAgent.execute: File did not exist.", pfn )
                      res = oRequest.setSubRequestFileAttributeValue( ind, 'removal', pfnToLfn[pfn], 'Status', 'Done' )
                      if not res['OK']:
                        gLogger.error( "RemovalAgent.execute: Error setting status to %s for %s" % ( 'Done', pfnToLfn[pfn] ) )
                      modified = True
                    else:
                      gLogger.info( "RemovalAgent.execute: Failed to remove file.", "%s at %s - %s" % ( pfn, diracSE, failed[pfn][diracSE] ) )
            if errMsg:
              for diracSE in errMsg.keys():
                errStr = "RemovalAgent.execute: Completely failed to remove replicas. At %s", diracSE
                gLogger.error( errStr, errMsg[diracSE] )


          ################################################
          #  If the sub-request is a physical removal operation
          elif operation == 'removeFile':
            gLogger.info( "RemovalAgent.execute: Attempting to execute %s sub-request." % operation )
            lfns = []
            for subRequestFile in subRequestFiles:
              if subRequestFile['Status'] == 'Waiting':
                lfn = str( subRequestFile['LFN'] )
                lfns.append( lfn )
            gMonitor.addMark( 'RemoveFileAtt', len( lfns ) )
            res = self.replicaManager.removeFile( lfns )
            if res['OK']:
              gMonitor.addMark( 'RemoveFileDone', len( res['Value']['Successful'].keys() ) )
              for lfn in res['Value']['Successful'].keys():
                gLogger.info( "RemovalAgent.execute: Successfully removed %s." % lfn )
                result = oRequest.setSubRequestFileAttributeValue( ind, 'removal', lfn, 'Status', 'Done' )
                if not result['OK']:
                  gLogger.error( "RemovalAgent.execute: Error setting status to %s for %s" % ( 'Done', lfn ) )
                modified = True
              gMonitor.addMark( 'RemoveFileFail', len( res['Value']['Failed'].keys() ) )
              for lfn in res['Value']['Failed'].keys():
                if type( res['Value']['Failed'][lfn] ) in StringTypes:
                  if re.search( 'no such file or directory', res['Value']['Failed'][lfn].lower() ):
                    gLogger.info( "RemovalAgent.execute: File did not exist.", lfn )
                    result = oRequest.setSubRequestFileAttributeValue( ind, 'removal', lfn, 'Status', 'Done' )
                    if not result['OK']:
                      gLogger.error( "RemovalAgent.execute: Error setting status to %s for %s" % ( 'Done', lfn ) )
                    modified = True
                  else:
                    gLogger.info( "RemovalAgent.execute: Failed to remove file:",
                                  "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
            else:
              gMonitor.addMark( 'RemoveFileFail', len( lfns ) )
              errStr = "RemovalAgent.execute: Completely failed to remove files files."
              gLogger.error( errStr, res['Message'] )

          ################################################
          #  If the sub-request is a physical removal operation
          elif operation == 'replicaRemoval':
            gLogger.info( "RemovalAgent.execute: Attempting to execute %s sub-request." % operation )
            diracSEs = subRequestAttributes['TargetSE'].split( ',' )
            lfns = []
            for subRequestFile in subRequestFiles:
              if subRequestFile['Status'] == 'Waiting':
                lfn = str( subRequestFile['LFN'] )
                lfns.append( lfn )
            gMonitor.addMark( 'ReplicaRemovalAtt', len( lfns ) )

            failed = {}
            errMsg = {}
            for diracSE in diracSEs:
              res = self.replicaManager.removeReplica( diracSE, lfns )
              if res['OK']:
                for lfn in res['Value']['Failed'].keys():
                  if res['Value']['Failed'][lfn].find( 'Write access not permitted for this credential.' ) != -1:
                    if self.__getProxyAndRemoveReplica( diracSE, lfn ):
                      continue
                  if not failed.has_key( lfn ):
                    failed[lfn] = {}
                  failed[lfn][diracSE] = res['Value']['Failed'][lfn]
              else:
                errMsg[diracSE] = res['Message']
                for lfn in lfns:
                  if not failed.has_key( lfn ):
                    failed[lfn] = {}
                  failed[lfn][diracSE] = 'Completely'
            # Now analyse the results
            failedLFNs = failed.keys()
            lfnsOK = [lfn for lfn in lfns if not lfn in failedLFNs]
            gMonitor.addMark( 'ReplicaRemovalDone', len( lfnsOK ) )
            for lfn in lfnsOK:
              gLogger.info( "RemovalAgent.execute: Successfully removed %s at %s" % ( lfn, str( diracSEs ) ) )
              res = oRequest.setSubRequestFileAttributeValue( ind, 'removal', lfn, 'Status', 'Done' )
              if not res['OK']:
                gLogger.error( "RemovalAgent.execute: Error setting status to %s for %s" % ( 'Done', lfn ) )
              modified = True
            if failed:
              gMonitor.addMark( 'PhysicalRemovalFail', len( failedLFNs ) )
              for lfn in failedLFNs:
                for diracSE in failed[lfn].keys():
                  if type( failed[lfn][diracSE] ) in StringTypes:
                    if re.search( 'no such file or directory', failed[lfn][diracSE].lower() ):
                      gLogger.info( "RemovalAgent.execute: File did not exist.", lfn )
                      res = oRequest.setSubRequestFileAttributeValue( ind, 'removal', lfn, 'Status', 'Done' )
                      if not res['OK']:
                        gLogger.error( "RemovalAgent.execute: Error setting status to %s for %s" % ( 'Done', lfn ) )
                      modified = True
                    else:
                      gLogger.info( "RemovalAgent.execute: Failed to remove file.", "%s at %s - %s" % ( lfn, diracSE, failed[lfn][diracSE] ) )
            if errMsg:
              for diracSE in errMsg.keys():
                errStr = "RemovalAgent.execute: Completely failed to remove replicas. At %s", diracSE
                gLogger.error( errStr, errMsg[diracSE] )

          ################################################
          #  If the sub-request is a request to the online system to retransfer
          elif operation == 'reTransfer':
            gLogger.info( "RemovalAgent.execute: Attempting to execute %s sub-request." % operation )
            diracSE = subRequestAttributes['TargetSE']
            for subRequestFile in subRequestFiles:
              if subRequestFile['Status'] == 'Waiting':
                pfn = str( subRequestFile['PFN'] )
                lfn = str( subRequestFile['LFN'] )
                res = self.replicaManager.onlineRetransfer( diracSE, pfn )
                if res['OK']:
                  if res['Value']['Successful'].has_key( pfn ):
                    gLogger.info( "RemovalAgent.execute: Successfully requested retransfer of %s." % pfn )
                    result = oRequest.setSubRequestFileAttributeValue( ind, 'removal', lfn, 'Status', 'Done' )
                    if not result['OK']:
                      gLogger.error( "RemovalAgent.execute: Error setting status to %s for %s" % ( 'Done', lfn ) )
                    modified = True
                  else:
                    errStr = "RemovalAgent.execute: Failed to request retransfer."
                    gLogger.error( errStr, "%s %s %s" % ( pfn, diracSE, res['Value']['Failed'][pfn] ) )
                else:
                  errStr = "RemovalAgent.execute: Completely failed to request retransfer."
                  gLogger.error( errStr, res['Message'] )
              else:
                gLogger.info( "RemovalAgent.execute: File already completed." )

          ################################################
          #  If the sub-request is none of the above types
          else:
            gLogger.error( "RemovalAgent.execute: Operation not supported.", operation )

          ################################################
          #  Determine whether there are any active files
          if oRequest.isSubRequestEmpty( ind, 'removal' )['Value']:
            oRequest.setSubRequestStatus( ind, 'removal', 'Done' )
            gMonitor.addMark( "Done", 1 )

        ################################################
        #  If the sub-request is already in terminal state
        else:
          gLogger.info( "RemovalAgent.execute:",
                        "Sub-request %s is status '%s' and not to be executed." %
                        ( ind, subRequestAttributes['Status'] ) )

      ################################################
      #  Generate the new request string after operation
      newrequestString = oRequest.toXML()['Value']
    except:
      # if something fails return the original request back to the server 
      res = self.requestDBClient.updateRequest( requestName, requestString, sourceServer )
      return S_OK()

    res = self.requestDBClient.updateRequest( requestName, newrequestString, sourceServer )

    if modified and jobID:
      result = self.finalizeRequest( requestName, jobID, sourceServer )

    return S_OK()

  def __getProxyAndRemoveReplica( self, diracSE, lfn ):
    """
    get a proxy from the owner of the file and try to remove it
    returns True if it succeeds, False otherwise
    """

    result = self.replicaManager.getCatalogDirectoryMetadata( lfn, singleFile = True )
    if not result[ 'OK' ]:
      gLogger.error( "Could not get metadata info", result[ 'Message' ] )
      return False
    ownerRole = result[ 'Value' ][ 'OwnerRole' ]
    ownerDN = result[ 'Value' ][ 'OwnerDN' ]
    if ownerRole[0] != "/":
      ownerRole = "/%s" % ownerRole

    userProxy = ''
    for ownerGroup in Registry.getGroupsWithVOMSAttribute( ownerRole ):
      result = gProxyManager.downloadVOMSProxy( ownerDN, ownerGroup, limited = True,
                                                requiredVOMSAttribute = ownerRole )
      if not result[ 'OK' ]:
        gLogger.verbose ( 'Failed to retrieve voms proxy for %s : %s:' % ( ownerDN, ownerRole ),
                          result[ 'Message' ] )
        continue
      userProxy = result[ 'Value' ]
      gLogger.verbose( "Got proxy for %s@%s [%s]" % ( ownerDN, ownerGroup, ownerRole ) )
      break
    if not userProxy:
      return False

    result = userProxy.dumpAllToFile()
    if not result[ 'OK' ]:
      gLogger.verbose( result[ 'Message' ] )
      return False

    upFile = result[ 'Value' ]
    prevProxyEnv = os.environ[ 'X509_USER_PROXY' ]
    os.environ[ 'X509_USER_PROXY' ] = upFile

    try:
      res = self.replicaManager.removeReplica( diracSE, lfn )
      if res['OK'] and lfn in res[ 'Value' ]['Successful']:
        gLogger.verbose( 'Removed %s from %s' % ( lfn, diracSE ) )
        return True
    finally:
      os.environ[ 'X509_USER_PROXY' ] = prevProxyEnv
      os.unlink( upFile )

    return False

  def finalize( self ):
    """
    Called by the Agent framework to cleanly end execution.
    In this case this module will wait until all pending ThreadedJbos in the
    ThreadPool get executed
    """

    self.threadPool.processAllResults()
    return S_OK()
