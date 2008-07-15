"""  RegistrationAgent takes register requests from the RequestDB and registers them
"""

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.Core.DISET.RPCClient import RPCClient

import time,os
from types import *

AGENT_NAME = 'DataManagement/RegistrationAgent'

class RegistrationAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.RequestDBClient = RequestClient()
    self.ReplicaManager = ReplicaManager()
    self.DataLog = DataLoggingClient()

    self.maxNumberOfThreads = gConfig.getValue(self.section+'/NumberOfThreads',1)
    self.threadPoolDepth = gConfig.getValue(self.section+'/ThreadPoolDepth',1)
    self.threadPool = ThreadPool(1,self.maxNumberOfThreads)

    self.useProxies = gConfig.getValue(self.section+'/UseProxies','True').lower() in ( "y", "yes", "true" )
    self.proxyLocation = gConfig.getValue( self.section+'/ProxyLocation', '' )
    if not self.proxyLocation:
      self.proxyLocation = False

    return result

  def execute(self):

    if self.useProxies:
      result = setupShifterProxyInEnv( "DataManager", self.proxyLocation )
      if not result[ 'OK' ]:
        self.log.error( "Can't get shifter's proxy: %s" % result[ 'Message' ] )
      return result

    for i in range(self.threadPoolDepth):
      requestExecutor = ThreadedJob(self.executeRequest)
      self.threadPool.queueJob(requestExecutor)
    self.threadPool.processResults()
    return self.executeRequest()

  def executeRequest(self):
    ################################################
    # Get a request from request DB
    res = self.RequestDBClient.getRequest('register')
    if not res['OK']:
      gLogger.info("RegistrationAgent.execute: Failed to get request from database.")
      return S_OK()
    elif not res['Value']:
      gLogger.info("RegistrationAgent.execute: No requests to be executed found.")
      return S_OK()
    requestString = res['Value']['RequestString']
    requestName = res['Value']['RequestName']
    sourceServer= res['Value']['Server']
    gLogger.info("RegistrationAgent.execute: Obtained request %s" % requestName)
    oRequest = RequestContainer(request=requestString)

    ################################################
    # Find the number of sub-requests from the request
    res = oRequest.getNumSubRequests('register')
    if not res['OK']:
      errStr = "RegistrationAgent.execute: Failed to obtain number of transfer subrequests."
      gLogger.error(errStr,res['Message'])
      return S_OK()
    gLogger.info("RegistrationAgent.execute: Found %s sub requests." % res['Value'])

    ################################################
    # For all the sub-requests in the request
    for ind in range(res['Value']):
      gLogger.info("RegistrationAgent.execute: Processing sub-request %s." % ind)
      subRequestAttributes = oRequest.getSubRequestAttributes(ind,'register')['Value']
      if subRequestAttributes['Status'] == 'Waiting':
        subRequestFiles = oRequest.getSubRequestFiles(ind,'register')['Value']
        operation = subRequestAttributes['Operation']

        ################################################
        #  If the sub-request is a register file operation
        if operation == 'registerFile':
          gLogger.info("RegistrationAgent.execute: Attempting to execute %s sub-request." % operation)
          diracSE = str(subRequestAttributes['TargetSE'])
          catalog = subRequestAttributes['Catalogue']
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              lfn = str(subRequestFile['LFN'])
              physicalFile = str(subRequestFile['PFN'])
              fileSize = int(subRequestFiles['Size'])
              fileGuid = str(subRequestFile['GUID'])
              checksum = str(subRequestFile['Addler'])
              fileTuple = (lfn,physicalFile,fileSize,diracSE,fileGuid,checksum)
              res = self.ReplicaManager.registerFile(fileTuple)
              print res
              if not res['OK']:
                self.DataLog.addFileRecord(lfn,'RegisterFail',diracSE,'','RegistrationAgent')
                errStr = "RegistrationAgent.execute: Completely failed to register file."
                gLogger.error(errStr, res['Message'])
              elif lfn in res['Value']['Failed'].keys():
                self.DataLog.addFileRecord(lfn,'RegisterFail',diracSE,'','RegistrationAgent')
                errStr = "RegistrationAgent.execute: Completely failed to register file."
                gLogger.error(errStr, res['Value']['Failed'][lfn])
              else:
                self.DataLog.addFileRecord(lfn,'Register',diracSE,'','TransferAgent')
                oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
            else:
              gLogger.info("RegistrationAgent.execute: File already completed.")

        ################################################
        #  If the sub-request is none of the above types
        else:
          gLogger.error("RegistrationAgent.execute: Operation not supported.", operation)

        ################################################
        #  Determine whether there are any active files
        if oRequest.isSubRequestEmpty(ind,'register')['Value']:
          oRequest.setSubRequestStatus(ind,'register','Done')

      ################################################
      #  If the sub-request is already in terminal state
      else:
        gLogger.info("RegistrationAgent.execute: Sub-request %s is status '%s' and  not to be executed." % (ind,subRequestAttributes['Status']))

    ################################################
    #  Generate the new request string after operation
    requestString = oRequest.toXML()['Value']
    res = self.RequestDBClient.updateRequest(requestName,requestString,sourceServer)

    return S_OK()
