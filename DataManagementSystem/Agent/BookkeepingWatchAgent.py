########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/Agent/BookkeepingWatchAgent.py,v 1.2 2009/05/13 12:22:38 acsmith Exp $
########################################################################

"""  The Transformation Agent prepares production jobs for processing data
     according to transformation definitions in the Production database.
"""

__RCSID__ = "$Id: BookkeepingWatchAgent.py,v 1.2 2009/05/13 12:22:38 acsmith Exp $"

from DIRAC.Core.Base.Agent    import Agent
from DIRAC                    import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.DISET.RPCClient import RPCClient
import os, time


AGENT_NAME = 'DataManagement/BookkeepingWatchAgent'

class BookkeepingWatchAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)
    self.fileLog = {}

  #############################################################################
  def initialize(self):
    """ Make the necessary initilizations
    """
    result = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)

    gMonitor.registerActivity("Iteration","Agent Loops",self.name,"Loops/min",gMonitor.OP_SUM)
    return result

  ##############################################################################
  def execute(self):
    """ Main execution method
    """

    gMonitor.addMark('Iteration',1)
    server = RPCClient('DataManagement/PlacementDB')
    bkserver = RPCClient('Bookkeeping/BookkeepingManager')
    
    result = server.getAllTransformations()

    activeTransforms = []
    if not result['OK']:
      gLogger.error("BookkeepingWatchAgent.execute: Failed to get productions.", result['Message'])
      return S_OK()

    for transDict in result['Value']:    
      transID = long(transDict['TransID'])
      transStatus = transDict['Status']
      bkQueryID = transDict['BkQueryID']

      print transID,transStatus,bkQueryID
      
      if transStatus in ["Active"] and bkQueryID:
        result = server.getBookkeepingQuery(bkQueryID)
        if not result['OK']:
          gLogger.warn("BookkeepingWatchAgent.execute: Failed to get BkQuery", result['Message'])
          continue
          
        bkDict = result['Value']
        
        # Make sure that default values are not passed and int values are converted to strings
        for name,value in bkDict.items():
          if name == "BkQueryID" :
            del bkDict[name]
          elif name == "ProductionID" or name == "EventType":
            if int(value) == 0:
              del bkDict[name]
            else:
              bkDict[name] = str(value)
          else:      
            if value.lower() == "all":
              del bkDict[name]
              
        start = time.time()
        result = bkserver.getFilesWithGivenDataSets(bkDict)    
        rtime = time.time()-start    
        gLogger.verbose('Bk query time: %.2f sec for query %d, transformation %d' % (rtime,bkQueryID,transID) )
        if not result['OK']:
          gLogger.error("BookkeepingWatchAgent.execute: Failed to get response from the Bookkeeping", result['Message'])
          continue
          
        lfnList = result['Value']   
        
        # Check of the number of files has changed since the last cycle
        nlfns = len(lfnList)
        gLogger.verbose('%d files available for production %d in BK DB' % (nlfns,int(transID)) )
        if self.fileLog.has_key(transID):
          if nlfns == self.fileLog[transID]:
            gLogger.verbose('No new files in BK DB since last check, skipping ...')
            continue
        self.fileLog[transID] = nlfns       
             
        if lfnList:
          gLogger.verbose('Processing %d lfns for transformation %d' % (len(lfnList),transID) )
          fileList = []
          for lfn in lfnList:
            fileList.append((lfn,'Unknown',0,'Unknown','0000',0))

          result = server.addFile(fileList,True)
          if not result['OK']:
            gLogger.error("BookkeepingWatchAgent.execute: Failed to add files", result['Message'])
            self.fileLog[transID] = 0
            continue  

          successfulList = result['Value']['Successful']
          failedList = result['Value']['Failed']
          if failedList:
            gLogger.warn("BookkeepingWatchAgent.execute: adding of %d files failed" % len(failedList), result['Message'])  
            self.fileLog[transID] = 0

          lfns = successfulList.keys() 
          gLogger.verbose('Adding %d lfns for transformation %d' % (len(lfns),transID) )
          result = server.addLFNsToTransformation(lfns,transID)
          if not result['OK']:
            gLogger.warn("BookkeepingWatchAgent.execute: failed to add lfns to transformation", result['Message'])   
            self.fileLog[transID] = 0
    
    return S_OK() 
