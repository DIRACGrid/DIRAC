# $HeadURL$

"""  DISET Forwarding sends DISET requests to their intended destination

    :deprecated:
"""

__RCSID__ = "$Id$"

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RPCClient import executeRPCStub
from DIRAC.Core.Utilities import DEncode

import time, os, re
from types import *

AGENT_NAME = 'RequestManagement/DISETForwardingAgent'

class DISETForwardingAgent( AgentModule ):

  def initialize( self ):

    self.RequestDBClient = RequestClient()
    backend = self.am_getOption( 'Backend', '' )
    self.RequestDB = False
    if backend == 'mysql':
      from DIRAC.RequestManagementSystem.DB.RequestDBMySQL import RequestDBMySQL
      requestDB = RequestDBMySQL()
      if requestDB._connected:
        self.RequestDB = requestDB

    gMonitor.registerActivity( "Iteration", "Agent Loops", "DISETForwardingAgent", "Loops/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Attempted", "Request Processed", "DISETForwardingAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Successful", "Request Forward Successful", "DISETForwardingAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Failed", "Request Forward Failed", "DISETForwardingAgent", "Requests/min", gMonitor.OP_SUM )

    #self.local = PathFinder.getServiceURL( "RequestManagement/localURL" )
    #if not self.local:
    #  self.local = AgentModule.am_getOption( self, 'localURL', '' )
    #if not self.local:
    #  errStr = 'The RequestManagement/localURL option must be defined.'
    #  gLogger.fatal( errStr )
    #  return S_ERROR( errStr )
    return S_OK()

  def execute( self ):
    """ The main execute method
    """
    self.requestsPerCycle = self.am_getOption( 'RequestsPerCycle', 10 )
    count = 0
    while count < self.requestsPerCycle:
      gLogger.verbose( 'Executing request #%d in this cycle' % count )
      result = self.execute_request()
      if not result['OK']:
        return result
      count += 1

    return S_OK()

  def execute_request( self ):
    """ Takes one DISET request and forward it to the destination service
    """
    gMonitor.addMark( "Iteration", 1 )
    if self.RequestDB:
      res = self.RequestDB.getRequest( 'diset' )
    else:
      res = self.RequestDBClient.getRequest( 'diset' )
    if not res['OK']:
      gLogger.error( "DISETForwardingAgent.execute: Failed to get request from database." )
      return S_OK()
    elif not res['Value']:
      gLogger.info( "DISETForwardingAgent.execute: No requests to be executed found." )
      return S_OK()

    gMonitor.addMark( "Attempted", 1 )
    requestString = res['Value']['RequestString']
    requestName = res['Value']['RequestName']
    try:
      jobID = int( res['Value']['JobID'] )
    except:
      jobID = 0
    gLogger.info( "DISETForwardingAgent.execute: Obtained request %s" % requestName )

    if self.RequestDB:
      result = self.RequestDB._getRequestAttribute( 'RequestID', requestName = requestName )
      if not result['OK']:
        return S_OK( 'Can not get the request execution order' )
      requestID = result['Value']
      result = self.RequestDB.getCurrentExecutionOrder( requestID )
    else:
      result = self.RequestDBClient.getCurrentExecutionOrder( requestName )
    if result['OK']:
      currentOrder = result['Value']
    else:
      return S_OK( 'Can not get the request execution order' )

    oRequest = RequestContainer( request = requestString )
    requestAttributes = oRequest.getRequestAttributes()['Value']

    ################################################
    # Find the number of sub-requests from the request
    res = oRequest.getNumSubRequests( 'diset' )
    if not res['OK']:
      errStr = "DISETForwardingAgent.execute: Failed to obtain number of diset subrequests."
      gLogger.error( errStr, res['Message'] )
      return S_OK()

    gLogger.info( "DISETForwardingAgent.execute: Found %s sub requests for job %s" % ( res['Value'], jobID ) )
    ################################################
    # For all the sub-requests in the request
    modified = False
    for ind in range( res['Value'] ):
      subRequestAttributes = oRequest.getSubRequestAttributes( ind, 'diset' )['Value']
      subExecutionOrder = int( subRequestAttributes['ExecutionOrder'] )
      subStatus = subRequestAttributes['Status']
      gLogger.info( "DISETForwardingAgent.execute: Processing sub-request %s with execution order %d" % ( ind, subExecutionOrder ) )
      if subStatus == 'Waiting' and subExecutionOrder <= currentOrder:
        operation = subRequestAttributes['Operation']
        gLogger.info( "DISETForwardingAgent.execute: Attempting to forward %s type." % operation )
        rpcStubString = subRequestAttributes['Arguments']
        rpcStub, length = DEncode.decode( rpcStubString )
        res = executeRPCStub( rpcStub )
        if res['OK']:
          gLogger.info( "DISETForwardingAgent.execute: Successfully forwarded." )
          oRequest.setSubRequestStatus( ind, 'diset', 'Done' )
          gMonitor.addMark( "Successful", 1 )
          modified = True
        elif res['Message'] == 'No Matching Job':
          gLogger.warn( "DISETForwardingAgent.execute: No corresponding job found. Setting to done." )
          oRequest.setSubRequestStatus( ind, 'diset', 'Done' )
        else:
          gLogger.error( "DISETForwardingAgent.execute: Failed to forward request.", res['Message'] )
      else:
        gLogger.info( "DISETForwardingAgent.execute: Sub-request %s is status '%s' and  not to be executed." % ( ind, subRequestAttributes['Status'] ) )

    ################################################
    #  Generate the new request string after operation
    requestString = oRequest.toXML()['Value']
    if self.RequestDB:
      res = self.RequestDB.updateRequest( requestName, requestString )
    else:
      res = self.RequestDBClient.updateRequest( requestName, requestString )
    if res['OK']:
      gLogger.info( "DISETForwardingAgent.execute: Successfully updated request." )
    else:
      gLogger.error( "DISETForwardingAgent.execute: Failed to update request" )

    if modified and jobID:
      result = self.RequestDBClient.finalizeRequest( requestName, jobID )

    return S_OK()
