########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/PilotStatusAgent.py,v 1.14 2008/07/18 09:36:53 acasajus Exp $
########################################################################

"""  The Pilot Status Agent updates the status of the pilot jobs if the
     PilotAgents database.
"""

__RCSID__ = "$Id: PilotStatusAgent.py,v 1.14 2008/07/18 09:36:53 acasajus Exp $"

from DIRAC.Core.Base.Agent import Agent
from DIRAC import S_OK, S_ERROR, gConfig, gLogger, List
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.Core.Utilities import systemCall, List
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.AccountingSystem.Client.Types.Pilot import Pilot as PilotAccounting
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.Core.Security import CS
from DIRAC import gConfig

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
    finalStateList = [ 'Done', 'Aborted' ]

    result = self.pilotDB.selectPilots(stateList)
    if not result['OK']:
      self.log.warn('Failed to get the Pilot Agents')
      return result
    if not result['Value']:
      return S_OK()

    result = self.pilotDB.getPilotInfo( result['Value'] )
    if not result['OK']:
      self.log.warn('Failed to get the Pilot Agent information from DB')
      return result

    pilotsDict = result['Value']
    workDict = {}

    # Sort pilots by grid type, owners
    for pRef in pilotsDict:
      pilotDict = pilotsDict[ pRef ]
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
      pilotsToAccount = []
      #if grid != "LCG": continue
      for owner_group,refList in workDict[grid].items():
        owner,group = owner_group.split(":")
        ret = gProxyManager.getPilotProxyFromVOMSGroup( owner, group )
        if not ret['OK']:
          self.log.error( ret['Message'] )
          self.log.error( 'Could not get proxy:', 'User "%s", Group "%s"' % ( owner, group ) )
          continue
        proxy = ret['Value']

        self.log.verbose("Getting status for pilots for owner %s, group %s" % (owner,group))

        # Do not call more than MAX_JOBS_QUERY pilots at a time
        for start_index in range( 0, len( refList ), MAX_JOBS_QUERY ):
          refsToQuery = refList[ start_index : start_index+MAX_JOBS_QUERY ]
          self.log.verbose( 'Querying %d pilots starting from %d' % ( len( refsToQuery ), start_index ) )
          result = self.getPilotStatus( proxy, grid, refsToQuery )
          if not result['OK']:
            self.log.warn('Failed to get pilot status:')
            self.log.warn('%s/%s, grid: %s' % (owner,group,grid))
            continue

          for pRef,pDict in result['Value'].items():
            if pDict:
              result = self.pilotDB.setPilotStatus(pRef,pDict['Status'],
                                                    pDict['Destination'],
                                                    pDict['StatusDate'])
              if pDict[ 'Status' ] in finalStateList:
                pilotsToAccount.append( pRef )

    retVal = self.pilotDB.getPilotInfo( pilotsToAccount, parentId = 0 )
    if not retVal[ 'OK' ] or not retVal[ 'Value' ]:
      continue
    pilotsData = retVal[ 'Value' ]
    for parentRef in pilotsData:
      pilotDict = pilotsData[ parentRef ]
      retVal = self.pilotDB.getPilotInfo( parentId = pilotDict[ 'PilotID' ] )
      if not retVal[ 'OK' ] or not retVal[ 'Value' ]:
        self.__addPilotAccountingReport( pilotDict )
      else:
        childDict = retVal[ 'Value' ]
        for childRef in childDict:
          self.__addPilotAccountingReport( childDict[ childRef ] )

    self.log.info( "Sending accounting records" )
    gDataStoreClient.commit()
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

  def __getSiteFromCE( self, ce ):
    siteSections = gConfig.getSections('/Resources/Sites/LCG/')
    if not siteSections['OK']:
      self.log.error('Could not get LCG site list')
      return "unknown"

    sites = siteSections['Value']
    for site in sites:
      lcgCEs = gConfig.getValue('/Resources/Sites/LCG/%s/CE' %site,[])
      if ce in lcgCEs:
        return site

    self.log.error( 'Could not determine DIRAC site name for CE:', siteName )
    return "unknown"

  def __addPilotAccountingReport( self, pData ):
    pA = PilotAccounting()
    pA.setEndTime()
    pA.setStartTime( pData[ 'SubmissionTime' ] )
    retVal = CS.getUsernameForDN( pData[ 'OwnerDN' ] )
    if not retVal[ 'OK' ]:
      userName = 'unknown'
      self.log.error( "Can't determine username for dn:", pData[ 'OwnerDN' ] )
    else:
      userName = retVal[ 'Value' ]
    pA.setValueByKey( 'User', userName )
    pA.setValueByKey( 'UserGroup', pData[ 'OwnerGroup' ] )
    pA.setValueByKey( 'Site', self.__getSiteFromCE( pData[ 'DestinationSite' ] ) )
    pA.setValueByKey( 'GridCE', pData[ 'DestinationSite' ] )
    pA.setValueByKey( 'GridMiddleware', pData[ 'GridType' ] )
    pA.setValueByKey( 'GridResourceBroker', pData[ 'Broker' ] )
    pA.setValueByKey( 'GridStatus', pData[ 'Status' ] )
    retVal = self.pilotDB.getJobsForPilot( pData[ 'PilotID' ] )
    if not retVal[ 'OK' ]:
      pA.setValueByKey( 'Jobs', 0 )
    else:
      pA.setValueByKey( 'Jobs', len( retVal[ 'Value' ] ) )
    self.log.info( "Added accounting record for pilot %s" % pData[ 'PilotID' ] )
    return gDataStoreClient.addRegister( pA )
