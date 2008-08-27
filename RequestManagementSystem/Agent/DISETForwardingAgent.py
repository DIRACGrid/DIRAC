"""  DISET Forwarding sends DISET requests to their intented destination
"""

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RPCClient import executeRPCStub
from DIRAC.Core.Utilities import DEncode

import time,os,re
from types import *

AGENT_NAME = 'RequestManagement/DISETForwardingAgent'

class DISETForwardingAgent(Agent):

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
    jobID = int(res['Value']['JobID'])
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
      gLogger.info("DISETForwardingAgent.execute: Processing sub-request %s with execution order %d" % (ind,subExecutionOrder))
      if subRequestAttributes['Status'] == 'Waiting' and \
         (subExecutionOrder <= currentOrder or subExecutionOrder == 0):
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

    if modified:
      result = self.finalizeRequest(requestName,jobID)

    return S_OK()

  def finalizeRequest(self,requestName,jobID):
    """ Check the request status and perform finalization if necessary
    """

    stateServer = RPCClient('WorkloadManagement/JobStateUpdate',useCertificates=True)

    # Update the request status and the corresponding job parameter
    result = self.RequestDBClient.getRequestStatus(requestName,self.local)
    if result['OK']:
      requestStatus = result['Value']['RequestStatus']
      subrequestStatus = result['Value']['SubRequestStatus']
      if subrequestStatus == "Done":
        result = self.RequestDBClient.setRequestStatus(requestName,'Done',self.local)
        if not result['OK']:
          gLogger.error(self.name+".checkRequest: Failed to set request status", self.central)
        # The request is completed, update the corresponding job status
        if jobID:
          monitorServer = RPCClient('WorkloadManagement/JobMonitoring',useCertificates=True)
          result = monitorServer.getJobPrimarySummary(int(jobID))
          if not result['OK']:
            gLogger.error(self.name+".checkRequest: Failed to get job status")
          else:
            jobStatus = result['Value']['Status']
            jobMinorStatus = result['Value']['MinorStatus']
            if jobMinorStatus == "Pending Requests":
              if jobStatus == "Completed":
                gLogger.info(self.name+'.checkRequest: Updating job status for %d to Done/Requests done' % jobID)
                result = stateServer.setJobStatus(jobID,'Done','Requests done',self.name)
                if not result['OK']:
                  gLogger.error(self.name+".checkRequest: Failed to set job status")
              elif jobStatus == "Failed":
                gLogger.info(self.name+'Updating job minor status for %d to Requests done' % jobID)
                result = stateServer.setJobStatus(jobID,'','Requests done',self.name)
                if not result['OK']:
                  gLogger.error(self.name+".checkRequest: Failed to set job status")
    else:
      gLogger.error("Failed to get request status at", self.central)

    # Update the job pending request digest in any case since it is modified
    gLogger.info(self.name+'.checkRequest: Updating request digest for job %d' % jobID)
    result = self.RequestDBClient.getDigest(requestName,self.local)
    if result['OK']:
      digest = result['Value']
      gLogger.verbose(digest)
      result = stateServer.setJobParameter(jobID,'PendingRequest',digest)
      if not result['OK']:
        gLogger.error(self.name+".checkRequest: Failed to set job parameter")
    else:
      gLogger.error(self.name+'.checkRequest: Failed to get request digest for %s' % requestName, result['Message'])

    return S_OK()
