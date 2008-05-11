########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/PilotStatusAgent.py,v 1.4 2008/05/11 09:31:31 rgracian Exp $
########################################################################

"""  The Pilot Status Agent updates the status of the pilot jobs if the 
     PilotAgents database.
"""

__RCSID__ = "$Id: PilotStatusAgent.py,v 1.4 2008/05/11 09:31:31 rgracian Exp $"

from DIRAC.Core.Base.Agent import Agent
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.ProxyRepositoryDB import ProxyRepositoryDB
from DIRAC.Core.Utilities.GridCredentials import setupProxy
from DIRAC.Core.Utilities.Subprocess import shellCall

import os, sys, re, string, time
from types import *

AGENT_NAME = 'WorkloadManagement/PilotStatusAgent'
MAX_JOBS_QUERY = 10

class PilotStatusAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    result = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.pilotDB = PilotAgentsDB()
    self.proxyDB = ProxyRepositoryDB()
    return result

  #############################################################################
  def execute(self):
    """The PilotAgent execution method.
    """

    # Select pilots in non-final states
    #stateList = ['Ready','Aborted','Submitted','Running','Waiting','Scheduled']
    stateList = ['Ready','Submitted','Running','Waiting','Scheduled']
    
    result = self.pilotDB.selectPilots(stateList)  
    if not result['OK']:
      self.log.warn('Failed to get the Pilot Agents')
      return result
    if not result['Value']:
      return S_OK()
      
    pilotList = result['Value']
    result = self.pilotDB.getPilotInfo(pilotList)
    if not result['OK']:
      self.log.warn('Failed to get the Pilot Agent information from DB')
      return result
        
    resultDict = result['Value']
    
    workDict = {}

    # Sort pilots by grid type, owners, brokers
    for pRef,pilotDict in resultDict.items():
      owner_group = pilotDict['OwnerDN']+":"+pilotDict['OwnerGroup']
      broker = pilotDict['Broker']
      grid = pilotDict['GridType']
      if workDict.has_key(grid):
        if workDict[grid].has_key(broker):
          if workDict[grid][broker].has_key(owner_group):
            workDict[grid][broker][owner_group].append(pRef)
          else:  
            workDict[grid][broker][owner_group] = []
            workDict[grid][broker][owner_group].append(pRef)
        else:
          workDict[grid][broker] = {}
          workDict[grid][broker][owner_group] = []
          workDict[grid][broker][owner_group].append(pRef)
      else:
        workDict[grid] = {}
        workDict[grid][broker] = {}
        workDict[grid][broker][owner_group] = []
        workDict[grid][broker][owner_group].append(pRef)    
            
    # Now the pilot references are sorted, let's do the work
    for grid in workDict.keys():
    
      #if grid != "LCG": continue
    
      for broker in workDict[grid].keys():
        for owner_group,pList in workDict[grid][broker].items():
          owner,group = owner_group.split(":")
          result = self.proxyDB.getProxy(owner,group)
          if not result['OK']:
            self.log.warn('Failed to get proxy for %s/%s' % (owner,group))
            continue

          proxyStr = result['Value']
          proxyFile = 'tmp_proxy'
          setupResult = setupProxy(proxyStr,proxyFile)
          if not setupResult['OK']:
            self.log.warn('Failed to setup proxy for %s/%s' % (owner,group))
            if os.path.exists('tmp_proxy'):
              os.remove('tmp_proxy')
            continue

          self.log.verbose("Getting status for pilots in broker %s" % broker)
          self.log.verbose("for owner %s, group %s" % (owner,group))

          # Do not call more than MAX_JOBS_QUERY pilots at a time
          start_index = 0
          resultDict = {}
          
          while len(pList) > start_index + MAX_JOBS_QUERY:
            self.log.verbose('Querying %d pilots starting from %d' % (MAX_JOBS_QUERY,start_index))
            result = eval("self.get"+grid+"PilotStatus(pList[start_index:start_index+MAX_JOBS_QUERY])")
            if not result['OK']:
              self.log.warn('Failed to get pilot status:')
              self.log.warn('%s/%s, broker: %s, grid: %s' % (owner,group,broker,grid))
              continue
              
            for pRef,pDict in result['Value'].items():
              if pDict:
                result = self.pilotDB.setPilotStatus(pRef,pDict['Status'],
                                                     pDict['Destination'],
                                                     pDict['StatusDate']) 
            start_index += MAX_JOBS_QUERY
            
          self.log.verbose('Querying last %d pilots' % (len(pList)-start_index) )
          result = eval("self.get"+grid+"PilotStatus(pList[start_index:])")
          
          os.remove('tmp_proxy')

          if not result['OK']:
            self.log.warn('Failed to get pilot status:')
            self.log.warn('%s/%s, broker: %s, grid: %s' % (owner,group,broker,grid))
            continue

          for pRef,pDict in result['Value'].items():
            if pDict:
              result = self.pilotDB.setPilotStatus(pRef,pDict['Status'],
                                                   pDict['Destination'],
                                                   pDict['StatusDate'])                                             

    return S_OK()

  #############################################################################
  def getLCGPilotStatus(self,pilotRefList):
    """ Get LCG job status information using the job's owner proxy and
        LCG job IDs. Returns for each JobID its status in the LCG WMS and
        its destination CE as a tuple of 2 elements
    """
          
    pilotList = pilotRefList
    if type(pilotRefList) == StringType:
      pilotList = [pilotRefList] 
   
    resultDict = {}
    for p in pilotList:
      resultDict[p] = None
   
    cmd = "%s %s" % ('edg-job-status'," ".join(pilotList))
    self.log.debug( '--- Executing %s ' % cmd)
    result = self.__exeCommand(cmd)

    if not result['OK']:
      self.log.warn(result)
      return result

    status = result['Status']
    stdout = result['StdOut']
    queryTime = result['Time']
    timing = '>>> LCG status query time %.2fs' % queryTime
    self.log.verbose( timing )
    
    lines = stdout.split('\n')
    normal_mode = False
  
    for line in lines:
    
      if line.find('Status info for the Job') != -1:
        pRef = line.replace('Status info for the Job :','').strip()
        normal_mode = True
        destination = None
        jobStatus = None
        statusDate = None
        
      if normal_mode:
        if line.find('Current Status:') != -1 :
          jobStatus = re.search(':\s+(\w+)',line).group(1)
        if line.find('Destination:') != -1 :
          destination = line.split()[1].split(":")[0]  
        if line.find('reached on:') != -1 : 
          statusDate = line.replace('reached on:','').strip()[4:]
          
          statusDate =  time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(statusDate,'%b %d %H:%M:%S %Y'))
          
          normal_mode = False
          #self.log.verbose('Pilot: %s, PilotStatus: %s, Destination: %s' %(pRef,jobStatus,destination))  
          pilotDict = {}
          pilotDict['Status'] = jobStatus
          pilotDict['Destination'] = destination
          pilotDict['StatusDate'] = statusDate
          resultDict[pRef] = pilotDict
          
    return S_OK(resultDict)    
    
  #############################################################################
  def getgLitePilotStatus(self,pilotRefList):
    """ Get gLite job status information using the job's owner proxy and
        LCG job IDs. Returns for each JobID its status in the LCG WMS and
        its destination CE as a tuple of 2 elements
    """    
    
    pilotList = pilotRefList
    if type(pilotRefList) == StringType:
      pilotList = [pilotRefList] 
   
    resultDict = {}
    for p in pilotList:
      resultDict[p] = None
   
    cmd = "%s %s" % ('glite-wms-job-status'," ".join(pilotList))
    self.log.debug( '--- Executing %s ' % cmd)
    result = self.__exeCommand(cmd)

    if not result['OK']:
      self.log.warn(result)
      return result

    status = result['Status']
    stdout = result['StdOut']
    queryTime = result['Time']
    timing = '>>> gLite status query time %.2fs' % queryTime
    self.log.verbose( timing )
        
    lines = stdout.split('\n')
    normal_mode = False
  
    for line in lines:
    
      if line.find('Status info for the Job') != -1:
        pRef = line.replace('Status info for the Job :','').strip()
        normal_mode = True
        destination = None
        jobStatus = None
        statusDate = None
        
      if normal_mode:
        if line.find('Current Status:') != -1 :
          jobStatus = re.search(':\s+(\w+)',line).group(1)
        if line.find('Destination:') != -1 :
          destination = line.split()[1].split(":")[0]  
          normal_mode = False          
          statusDate =  None
          
          #self.log.verbose('Pilot: %s, PilotStatus: %s, Destination: %s' %(pRef,jobStatus,destination))  
          pilotDict = {}
          pilotDict['Status'] = jobStatus
          pilotDict['Destination'] = destination
          pilotDict['StatusDate'] = statusDate
          resultDict[pRef] = pilotDict
          
    return S_OK(resultDict) 

  #############################################################################
  def __exeCommand(self,cmd):
    """Runs a submit / list-match command and prints debugging information.
    """
    start = time.time()
    self.log.verbose( cmd )
    result = shellCall(60,cmd)

    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]
    #self.log.verbose('Status = %s' %status)
    #self.log.verbose(stdout)
    if stderr:
      self.log.warn(stderr)
    result['Status']=status
    result['StdOut']=stdout
    result['StdErr']=stderr
    subtime = time.time() - start
    result['Time']=subtime
    return result
