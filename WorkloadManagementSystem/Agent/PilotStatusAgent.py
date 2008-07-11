########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/PilotStatusAgent.py,v 1.7 2008/07/11 18:33:17 rgracian Exp $
########################################################################

"""  The Pilot Status Agent updates the status of the pilot jobs if the
     PilotAgents database.
"""

__RCSID__ = "$Id: PilotStatusAgent.py,v 1.7 2008/07/11 18:33:17 rgracian Exp $"

from DIRAC.Core.Base.Agent import Agent
from DIRAC import S_OK, S_ERROR, gConfig, gLogger, List
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.Core.Utilities import systemCall, List
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager

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

    # Sort pilots by grid type, owners
    for pRef,pilotDict in resultDict.items():
      if not pRef or str(pRef) == 'False':
        continue
      owner_group = pilotDict['OwnerDN']+":"+pilotDict['OwnerGroup']
      grid = pilotDict['GridType']
      if workDict.has_key(grid):
        if workDict[grid].has_key(owner_group):
          workDict[grid][owner_group].append(pRef)
        else:
          workDict[grid][owner_group] = []
          workDict[grid][owner_group].append(pRef)
      else:
        workDict[grid] = {}
        workDict[grid][owner_group] = []
        workDict[grid][owner_group].append(pRef)

    # Now the pilot references are sorted, let's do the work
    for grid in workDict.keys():

      #if grid != "LCG": continue

      for owner_group,pList in workDict[grid].items():
        owner,group = owner_group.split(":")
        ret = gProxyManager.getPilotProxyFromVOMSGroup( owner, group )
        if not ret['OK']:
          self.log.error( ret['Message'] )
          self.log.error( 'Could not get proxy:', 'User "%s", Group "%s"' % ( owner, group ) )
          continue
        proxy = ret['Value']

        self.log.verbose("Getting status for pilots for owner %s, group %s" % (owner,group))

        # Do not call more than MAX_JOBS_QUERY pilots at a time
        start_index = 0
        resultDict = {}

        while len(pList) > start_index + MAX_JOBS_QUERY:
          self.log.verbose('Querying %d pilots starting from %d' % (MAX_JOBS_QUERY,start_index))
          result = self.getPilotStatus( proxy, grid, pList[start_index:start_index+MAX_JOBS_QUERY])
          if not result['OK']:
            self.log.warn('Failed to get pilot status:')
            self.log.warn('%s/%s, grid: %s' % (owner,group,grid))
            continue

          for pRef,pDict in result['Value'].items():
            if pDict:
              result = self.pilotDB.setPilotStatus(pRef,pDict['Status'],
                                                    pDict['Destination'],
                                                    pDict['StatusDate'])
          start_index += MAX_JOBS_QUERY

        self.log.verbose('Querying last %d pilots' % (len(pList)-start_index) )
        result = self.getPilotStatus( proxy, grid, pList[start_index:])

        if not result['OK']:
          self.log.warn('Failed to get pilot status:')
          self.log.warn('%s/%s, grid: %s' % (owner,group,grid))
          continue

        for pRef,pDict in result['Value'].items():
          if pDict:
            result = self.pilotDB.setPilotStatus(pRef,pDict['Status'],
                                                  pDict['Destination'],
                                                  pDict['StatusDate'])

    return S_OK()

  #############################################################################
  def getPilotStatus(self, proxy, grid, pilotRefList ):
    """ Get GRID job status information using the job's owner proxy and
        GRID job IDs. Returns for each JobID its status in the GRID WMS and
        its destination CE as a tuple of 2 elements
    """

    if grid == 'LCG':
      cmd = [ 'edg-job-status' ]
    elif grid == 'gLite':
      cmd = [ 'glite-wms-job-status' ]
    else:
      return S_ERROR()
    cmd.extend( pilotRefList )

    gridEnv = dict(os.environ)
    ret = gProxyManager.dumpProxyToFile( proxy )
    if not ret['OK']:
      self.log.error( 'Failed to dump Proxy to file' )
      return ret
    gridEnv[ 'X509_USER_PROXY' ] = ret['Value']
    self.log.verbose( 'Executing', ' '.join(cmd) )
    start = time.time()
    ret =  systemCall( 60, cmd, env = gridEnv )

    if not ret['OK']:
      self.log.error( 'Failed to execute %s Job Status' % grid, ret['Message'] )
      return S_ERROR()
    if ret['Value'][0] != 0:
      self.log.error( 'Error executing %s Job Status:' % grid, str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
      return S_ERROR()
    self.log.info( '%s Job Status Execution Time:' % grid, time.time()-start )

    stdout = ret['Value'][1]
    stderr = ret['Value'][2]

    statusRE      = 'Current Status:\s*(\w*)'
    destinationRE = 'Destination:\s*([\w\.-]*)'
    statusDateRE  = 'reached on:\s*....(.*)'

    resultDict = {}
    for job in List.fromChar(stdout,'Status info for the Job :')[1:]:
      pRef = List.fromChar(job,'\n' )[0].strip()
      status      = None
      destination = None
      statusDate  = None
      try:
        status      = re.search(statusRE,      job).group(1)
        if re.search(destinationRE, job):
          destination = re.search(destinationRE, job).group(1)
        if grid == 'LCG' and re.search(statusDateRE, job):
          statusDate = re.search(statusDateRE, job).group(1)
          statusDate = time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(statusDate,'%b %d %H:%M:%S %Y'))
      except Exception, x:
        self.log.error( 'Error parsing %s Job Status output:\n' % grid, job )
      resultDict[pRef] = { 'Status': status,
                           'Destination': destination,
                           'StatusDate': statusDate }

    return S_OK(resultDict)
