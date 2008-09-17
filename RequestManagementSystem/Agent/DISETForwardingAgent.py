# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Agent/DISETForwardingAgent.py,v 1.9 2008/09/17 18:18:34 atsareg Exp $

"""  DISET Forwarding sends DISET requests to their intented destination
"""

__RCSID__ = "$Id: DISETForwardingAgent.py,v 1.9 2008/09/17 18:18:34 atsareg Exp $"

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RPCClient import executeRPCStub
from DIRAC.Core.Utilities import DEncode
from DIRAC.RequestManagementSystem.Agent.RequestAgentMixIn import RequestAgentMixIn

import time,os,re
from types import *

AGENT_NAME = 'RequestManagement/DISETForwardingAgent'

class DISETForwardingAgent(Agent,RequestAgentMixIn):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.RequestDBClient = RequestClient()

    gMonitor.registerActivity("Iteration",          "Agent Loops",                  "DISETForwardingAgent",      "Loops/min",      gMonitor.OP_SUM)
    gMonitor.registerActivity("Attempted",          "Request Processed",            "DISETForwardingAgent",      "Requests/min",   gMonitor.OP_SUM)
    gMonitor.registerActivity("Successful",         "Request Forward Successful",   "DISETForwardingAgent",      "Requests/min",   gMonitor.OP_SUM)
    gMonitor.registerActivity("Failed",             "Request Forward Failed",       "DISETForwardingAgent",      "Requests/min",   gMonitor.OP_SUM)

    self.local = PathFinder.getServiceURL("RequestManagement/localURL")
    if not self.local:
      errStr = 'The RequestManagement/localURL option must be defined.'
      gLogger.fatal(errStr)
      return S_ERROR(errStr)
    return result

  def execute(self):
    """ Takes the DISET requests and forwards to destination service
    """
    gMonitor.addMark("Iteration",1)
    res = self.RequestDBClient.getRequest('diset',url=self.local)
    if not res['OK']:
      gLogger.error("DISETForwardingAgent.execute: Failed to get request from database.",self.local)
      return S_OK()
    elif not res['Value']:
      gLogger.info("DISETForwardingAgent.execute: No requests to be executed found.")
      return S_OK()

    gMonitor.addMark("Attempted",1)
    requestString = res['Value']['RequestString']
    requestName = res['Value']['RequestName']
    try:
      jobID = int(res['Value']['JobID'])
    except:
      jobID = 0
    gLogger.info("DISETForwardingAgent.execute: Obtained request %s" % requestName)

    result = self.RequestDBClient.getCurrentExecutionOrder(requestName,self.local)
    if result['OK']:
      currentOrder = result['Value']
    else:
      return S_OK('Can not get the request execution order')

    oRequest = RequestContainer(request=requestString)
    requestAttributes = oRequest.getRequestAttributes()['Value']

    ################################################
    # Find the number of sub-requests from the request
    res = oRequest.getNumSubRequests('diset')
    if not res['OK']:
      errStr = "DISETForwardingAgent.execute: Failed to obtain number of diset subrequests."
      gLogger.error(errStr,res['Message'])
      return S_OK()

    gLogger.info("DISETForwardingAgent.execute: Found %s sub requests for job %s" % (res['Value'],jobID))
    ################################################
    # For all the sub-requests in the request
    modified = False
    for ind in range(res['Value']):
      subRequestAttributes = oRequest.getSubRequestAttributes(ind,'diset')['Value']
      subExecutionOrder = int(subRequestAttributes['ExecutionOrder'])
      subStatus = subRequestAttributes['Status']
      gLogger.info("DISETForwardingAgent.execute: Processing sub-request %s with execution order %d" % (ind,subExecutionOrder))
      if subStatus == 'Waiting' and subExecutionOrder <= currentOrder:
        operation = subRequestAttributes['Operation']
        gLogger.info("DISETForwardingAgent.execute: Attempting to forward %s type." % operation)
        rpcStubString = subRequestAttributes['Arguments']
        rpcStub,length = DEncode.decode(rpcStubString)
        res = executeRPCStub(rpcStub)
        if res['OK']:
          gLogger.info("DISETForwardingAgent.execute: Successfully forwarded.")
          oRequest.setSubRequestStatus(ind,'diset','Done')
          gMonitor.addMark("Successful",1)
          modified = True
        else:
          oRequest.setSubRequestError(ind,'diset',res['Message'])
          gLogger.error("DISETForwardingAgent.execute: Failed to forward request.",res['Message'])
      else:
        gLogger.info("DISETForwardingAgent.execute: Sub-request %s is status '%s' and  not to be executed." % (ind,subRequestAttributes['Status']))

    ################################################
    #  Generate the new request string after operation
    requestString = oRequest.toXML()['Value']
    res = self.RequestDBClient.updateRequest(requestName,requestString,self.local)
    if res['OK']:
      gLogger.info("DISETForwardingAgent.execute: Successfully updated request.")
    else:
      gLogger.error("DISETForwardingAgent.execute: Failed to update request to", self.central)

    if modified and jobID:
      result = self.finalizeRequest(requestName,jobID,self.local)

    return S_OK()

