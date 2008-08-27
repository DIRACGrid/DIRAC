from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import gLogger, S_OK, S_ERROR

class RequestAgentMixIn:

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
