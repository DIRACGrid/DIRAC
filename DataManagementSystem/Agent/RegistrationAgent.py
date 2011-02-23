########################################################################
# $HeadURL$
########################################################################

"""  RegistrationAgent takes register requests from the RequestDB and registers them
"""

from DIRAC  import gLogger, S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool, ThreadedJob
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.RequestManagementSystem.Agent.RequestAgentMixIn import RequestAgentMixIn

from types import *

__RCSID__ = "$Id$"

AGENT_NAME = 'DataManagement/RegistrationAgent'

class RegistrationAgent( AgentModule, RequestAgentMixIn ):

  def initialize( self ):

    self.RequestDBClient = RequestClient()
    self.ReplicaManager = ReplicaManager()
    self.DataLog = DataLoggingClient()

    self.maxNumberOfThreads = self.am_getOption( 'NumberOfThreads', 1 )
    self.threadPoolDepth = self.am_getOption( 'ThreadPoolDepth', 1 )
    self.threadPool = ThreadPool( 1, self.maxNumberOfThreads )

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    return S_OK()

  def execute( self ):

    for i in range( self.threadPoolDepth ):
      requestExecutor = ThreadedJob( self.executeRequest )
      self.threadPool.queueJob( requestExecutor )
    self.threadPool.processResults()
    return self.executeRequest()

  def executeRequest( self ):
    ################################################
    # Get a request from request DB
    res = self.RequestDBClient.getRequest( 'register' )
    if not res['OK']:
      gLogger.info( "RegistrationAgent.execute: Failed to get request from database." )
      return S_OK()
    elif not res['Value']:
      gLogger.info( "RegistrationAgent.execute: No requests to be executed found." )
      return S_OK()
    requestString = res['Value']['RequestString']
    requestName = res['Value']['RequestName']
    sourceServer = res['Value']['Server']
    try:
      jobID = int( res['Value']['JobID'] )
    except:
      jobID = 0
    gLogger.info( "RegistrationAgent.execute: Obtained request %s" % requestName )

    result = self.RequestDBClient.getCurrentExecutionOrder( requestName, sourceServer )
    if result['OK']:
      currentOrder = result['Value']
    else:
      return S_OK( 'Can not get the request execution order' )

    oRequest = RequestContainer( request = requestString )

    ################################################
    # Find the number of sub-requests from the request
    res = oRequest.getNumSubRequests( 'register' )
    if not res['OK']:
      errStr = "RegistrationAgent.execute: Failed to obtain number of transfer subrequests."
      gLogger.error( errStr, res['Message'] )
      return S_OK()
    gLogger.info( "RegistrationAgent.execute: Found %s sub requests." % res['Value'] )

    ################################################
    # For all the sub-requests in the request
    modified = False
    for ind in range( res['Value'] ):
      gLogger.info( "RegistrationAgent.execute: Processing sub-request %s." % ind )
      subRequestAttributes = oRequest.getSubRequestAttributes( ind, 'register' )['Value']
      subExecutionOrder = int( subRequestAttributes['ExecutionOrder'] )
      subStatus = subRequestAttributes['Status']
      if subStatus == 'Waiting' and subExecutionOrder <= currentOrder:
        subRequestFiles = oRequest.getSubRequestFiles( ind, 'register' )['Value']
        operation = subRequestAttributes['Operation']

        ################################################
        #  If the sub-request is a register file operation
        if operation == 'registerFile':
          gLogger.info( "RegistrationAgent.execute: Attempting to execute %s sub-request." % operation )
          diracSE = str( subRequestAttributes['TargetSE'] )
          if diracSE == 'SE':
            # We do not care about SE, put any there
            diracSE = "CERN-FAILOVER"
          catalog = subRequestAttributes['Catalogue']
          if catalog == "None":
            catalog = ''
          subrequest_done = True
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              lfn = subRequestFile.get( 'LFN', '' )
              if lfn: lfn = str( lfn )
              physicalFile = subRequestFile.get( 'PFN', '' )
              if physicalFile: physicalFile = str( physicalFile )
              fileSize = subRequestFile.get( 'Size', 0 )
              if fileSize: fileSize = int( fileSize )
              fileGuid = subRequestFile.get( 'GUID', '' )
              if fileGuid: fileGuid = str( fileGuid )
              checksum = subRequestFile.get( 'Addler', '' )
              if checksum: checksum = str( checksum )
              if catalog == 'BookkeepingDB':
                diracSE = 'CERN-HIST'
              fileTuple = ( lfn, physicalFile, fileSize, diracSE, fileGuid, checksum )
              res = self.ReplicaManager.registerFile( fileTuple, catalog )
              print res
              if not res['OK']:
                self.DataLog.addFileRecord( lfn, 'RegisterFail', diracSE, '', 'RegistrationAgent' )
                errStr = "RegistrationAgent.execute: Completely failed to register file."
                gLogger.error( errStr, res['Message'] )
                subrequest_done = False
              elif lfn in res['Value']['Failed'].keys():
                self.DataLog.addFileRecord( lfn, 'RegisterFail', diracSE, '', 'RegistrationAgent' )
                errStr = "RegistrationAgent.execute: Completely failed to register file."
                gLogger.error( errStr, res['Value']['Failed'][lfn] )
                subrequest_done = False
              else:
                self.DataLog.addFileRecord( lfn, 'Register', diracSE, '', 'TransferAgent' )
                oRequest.setSubRequestFileAttributeValue( ind, 'transfer', lfn, 'Status', 'Done' )
                modified = True
            else:
              gLogger.info( "RegistrationAgent.execute: File already completed." )
          if subrequest_done:
            oRequest.setSubRequestStatus( ind, 'register', 'Done' )

        ################################################
        #  If the sub-request is none of the above types
        else:
          gLogger.error( "RegistrationAgent.execute: Operation not supported.", operation )

        ################################################
        #  Determine whether there are any active files
        if oRequest.isSubRequestEmpty( ind, 'register' )['Value']:
          oRequest.setSubRequestStatus( ind, 'register', 'Done' )

      ################################################
      #  If the sub-request is already in terminal state
      else:
        gLogger.info( "RegistrationAgent.execute: Sub-request %s is status '%s' and  not to be executed." % ( ind, subRequestAttributes['Status'] ) )

    ################################################
    #  Generate the new request string after operation
    requestString = oRequest.toXML()['Value']
    res = self.RequestDBClient.updateRequest( requestName, requestString, sourceServer )

    if modified and jobID:
      result = self.finalizeRequest( requestName, jobID, sourceServer )

    return S_OK()
