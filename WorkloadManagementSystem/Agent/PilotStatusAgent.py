########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/PilotStatusAgent.py,v 1.56 2009/01/23 18:16:46 atsareg Exp $
########################################################################

"""  The Pilot Status Agent updates the status of the pilot jobs if the
     PilotAgents database.
"""

__RCSID__ = "$Id: PilotStatusAgent.py,v 1.56 2009/01/23 18:16:46 atsareg Exp $"

from DIRAC.Core.Base.Agent import Agent
from DIRAC import S_OK, S_ERROR, gConfig, gLogger, List
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.Core.Utilities import systemCall, List, Time
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.AccountingSystem.Client.Types.Pilot import Pilot as PilotAccounting
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.Core.Security import CS
from DIRAC import gConfig, Source

import os, sys, re, string, time
from types import *

AGENT_NAME = 'WorkloadManagement/PilotStatusAgent'
MAX_JOBS_QUERY = 10
MAX_WAITING_STATE_LENGTH = 6

class PilotStatusAgent(Agent):

  queryStateList = ['Ready','Submitted','Running','Waiting','Scheduled']
  finalStateList = [ 'Done', 'Aborted', 'Cleared', 'Deleted' ]
  identityFieldsList = [ 'OwnerDN', 'OwnerGroup', 'GridType', 'Broker' ]

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
    self.gridEnv     = gConfig.getValue(self.section+'/GridEnv','')
    self.pilotStalledDays =  gConfig.getValue(self.section+'/PilotStalledDays',3)
    self.pilotDB = PilotAgentsDB()
    return result

  #############################################################################
  def execute(self):
    """The PilotAgent execution method.
    """
    parentIDList = [ '0' , '-1' ]

    self.pilotStalledDays = gConfig.getValue( self.section+'/PilotStalledDays', self.pilotStalledDays )
    result = self.pilotDB._getConnection()
    if result['OK']:
      connection = result['Value']
    else:
      return result

    result = self.pilotDB.getPilotGroups( self.identityFieldsList,
                                         {'Status': self.queryStateList } )
    if not result['OK']:
      self.log.error('Fail to get identities Groups', result['Message'])
      return result
    if not result['Value']:
      return S_OK()

    pilotsToAccount = {}

    for ownerDN, ownerGroup, gridType, broker in result['Value']:
      self.log.verbose( 'Getting pilots for %s:%s @ %s %s' % ( ownerDN, ownerGroup, gridType, broker ) )

      condDict = {'Status':self.queryStateList,
                  'OwnerDN':ownerDN,
                  'OwnerGroup':ownerGroup,
                  'GridType':gridType,
                  'Broker':broker}
           
      result = self.clearWaitingPilots(condDict)
      if not result['OK']:
        self.log.warn('Failed to clear Waiting Pilot Jobs')     

      result = self.pilotDB.selectPilots(condDict)
      if not result['OK']:
        self.log.warn('Failed to get the Pilot Agents')
        return result
      if not result['Value']:
        continue
      refList = result['Value']

      ret = gProxyManager.getPilotProxyFromVOMSGroup( ownerDN, ownerGroup )
      if not ret['OK']:
        self.log.error( ret['Message'] )
        self.log.error( 'Could not get proxy:', 'User "%s", Group "%s"' % ( ownerDN, ownerGroup ) )
        continue
      proxy = ret['Value']

      self.log.verbose("Getting status for %s pilots for owner %s and group %s" % ( len( refList ),
                                                                                      ownerDN, ownerGroup))

      for start_index in range( 0, len( refList ), MAX_JOBS_QUERY ):
        refsToQuery = refList[ start_index : start_index+MAX_JOBS_QUERY ]
        self.log.verbose( 'Querying %d pilots of %s starting at %d' % ( len( refsToQuery ), len( refList ), start_index ) )
        result = self.getPilotStatus( proxy, gridType, refsToQuery )
        if not result['OK']:
          if result['Message'] == 'Broker not Available': 
            self.log.error( 'Broker %s not Available' % broker )
            break
          self.log.warn('Failed to get pilot status:')
          self.log.warn('%s:%s @ %s' % ( ownerDN, ownerGroup, gridType ))
          continue

        statusDict = result[ 'Value' ]
        for pRef in statusDict:
          pDict = statusDict[ pRef ]
          if pDict:
            if pDict['isParent']:
              self.log.verbose('Clear parametric parent %s' % pRef)
              result = self.clearParentJob(pRef,pDict,connection)
              if not result['OK']:
                self.log.warn(result['Message'])
              else:
                self.log.info('Parameteric parent removed: %s' % pRef)  
            if pDict[ 'FinalStatus' ]:
              self.log.verbose('Marking Status for %s to %s' % (pRef,pDict['Status']) )
              pilotsToAccount[ pRef ] = pDict
            else:
              self.log.verbose('Setting Status for %s to %s' % (pRef,pDict['Status']) )
              result = self.pilotDB.setPilotStatus( pRef,
                                                    pDict['Status'],
                                                    pDict['DestinationSite'],
                                                    pDict['StatusDate'],
                                                    conn = connection )

        if len( pilotsToAccount ) > 100:
          self.accountPilots( pilotsToAccount, connection )
          pilotsToAccount = {}

    self.accountPilots( pilotsToAccount, connection )
    # Now handle pilots not updated in the last N days (most likely the Broker is no 
    # longer available) and declare them Deleted.
    result = self.handleOldPilots( connection )

    connection.close()

    return S_OK()
    
  def clearWaitingPilots(self,condDict):
    """ Clear pilots in the faulty Waiting state
    """   
    
    last_update = Time.dateTime() - MAX_WAITING_STATE_LENGTH*Time.hour
    clearDict = {}
    clearDict = {'Status':'Waiting',
                  'OwnerDN':condDict['OwnerDN'],
                  'OwnerGroup':condDict['OwnerGroup'],
                  'GridType':condDict['GridType'],
                  'Broker':condDict['Broker']}
    result = self.pilotDB.selectPilots(clearDict,older=last_update)
    if not result['OK']:
      self.log.warn('Failed to get the Pilot Agents fpr Waiting state')
      return result
    if not result['Value']:
      return S_OK()
    refList = result['Value']  
    
    for pilotRef in refList:
      self.log.info('Setting Waiting pilot to Aborted: %s' % pilotRef)
      result = self.pilotDB.setPilotStatus(pilotRef,'Aborted',statusReason='Exceeded max waiting time')
      
    return S_OK()              
    
  def clearParentJob(self,pRef,pDict,connection):
    """ Clear the parameteric parent job from the PilotAgentsDB
    """  
    
    childList = pDict['ChildRefs']
    
    # Check that at least one child is in the database
    children_ok = False
    for child in childList:
      result = self.pilotDB.getPilotInfo(child, conn=connection)
      if result['OK']:
        if result['Value']:
          children_ok = True
         
    if children_ok:   
      return self.pilotDB.deletePilot(pRef, conn=connection)
    else:
      self.log.verbose('Adding children for parent %s' % pRef) 
      result = self.pilotDB.getPilotInfo(pRef)
      parentInfo = result['Value'][pRef]
      tqID = parentInfo['TaskQueueID']
      ownerDN = parentInfo['OwnerDN']
      ownerGroup = parentInfo['OwnerGroup']
      broker = parentInfo['Broker']
      gridType = parentInfo['GridType']
      result = self.pilotDB.addPilotTQReference(childList,tqID,ownerDN,ownerGroup,
                                                broker=broker,gridType=gridType)
      if not result['OK']:
        return result
      children_added = True  
      for chRef,chDict in pDict['ChildDicts'].items():
        result = self.pilotDB.setPilotStatus(chRef,chDict['Status'],
                                             destination=chDict['DestinationSite'],
                                             conn = connection)  
        if not result['OK']:
          children_added = False                                            
      if children_added :
        result = self.pilotDB.deletePilot(pRef, conn=connection) 
      else:
        return S_ERROR('Failed to add children')     
    return S_OK()                    

  def handleOldPilots( self, connection ):
    # select all pilots that have not been updated in the last N days and declared them 
    # Deleted, accounting for them.
    pilotsToAccount = {}
    timeLimitToConsider = Time.toString( Time.dateTime() - Time.day * self.pilotStalledDays )
    # A.T. Below looks to be a bug 
    #result = self.pilotDB.selectPilots( {'Status':self.queryStateList} , older=None, timeStamp='LastUpdateTime' )
    result = self.pilotDB.selectPilots( {'Status':self.queryStateList} , older=timeLimitToConsider, timeStamp='LastUpdateTime' )
    if not result['OK']:
      self.log.error('Failed to get the Pilot Agents')
      return result
    if not result['Value']:
      return S_OK()

    refList = result['Value']
    result = self.pilotDB.getPilotInfo( refList )
    if not result['OK']:
      self.log.error('Failed to get Info for Pilot Agents')
      return result

    pilotsDict = result['Value']

    for pRef in pilotsDict:
      statusDate = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
      deletedJobDict = pilotsDict[pRef]
      deletedJobDict['Status'] = 'Deleted'
      deletedJobDict['StatusDate'] = statusDate
      pilotsToAccount[ pRef ] = deletedJobDict
      if len( pilotsToAccount ) > 100:
        self.accountPilots( pilotsToAccount, connection )
        pilotsToAccount = {}

    self.accountPilots( pilotsToAccount, connection )

    return S_OK()
    
  def accountPilots( self, pilotsToAccount, connection ):

    if not pilotsToAccount:
      self.log.info( 'No pilots to Account' )
      return S_OK()

    retVal = self.pilotDB.getPilotInfo( pilotsToAccount.keys(), conn = connection )
    if not retVal['OK']:
      self.log.error( 'Fail to retrieve Info for pilots', retVal['Message'] )
      return retVal
    dbData = retVal[ 'Value' ]
    for pref in dbData:
      if pref in pilotsToAccount:
        if dbData[pref][ 'Status' ] not in self.finalStateList:
          dbData[pref][ 'Status' ] = pilotsToAccount[pref][ 'Status' ]
          dbData[pref][ 'DestinationSite' ] = pilotsToAccount[pref][ 'DestinationSite' ]
          dbData[pref][ 'LastUpdateTime' ] = Time.fromString( pilotsToAccount[pref][ 'StatusDate' ] )

    retVal = self.__addPilotsAccountingReport( dbData )
    if not retVal['OK']:
      self.log.error( 'Fail to retrieve Info for pilots',retVal['Message'] )
      return retVal

    self.log.info( "Sending accounting records..." )
    retVal = gDataStoreClient.commit()
    if not retVal[ 'OK' ]:
      self.log.error( "Can't send accounting repots", retVal[ 'Message' ] )
    else:
      self.log.info( "Accounting sent for %s pilots" % len(pilotsToAccount) )
      for pRef in pilotsToAccount:
        pDict = pilotsToAccount[pRef]
        self.log.verbose('Setting Status for %s to %s' % (pRef,pDict['Status']) )
        self.pilotDB.setPilotStatus( pRef,
                                     pDict['Status'],
                                     pDict['DestinationSite'],
                                     pDict['StatusDate'],
                                     conn = connection )

    return retVal

  #############################################################################
  def getPilotStatus(self, proxy, gridType, pilotRefList ):
    """ Get GRID job status information using the job's owner proxy and
        GRID job IDs. Returns for each JobID its status in the GRID WMS and
        its destination CE as a tuple of 2 elements
    """

    if gridType == 'LCG':
      cmd = [ 'edg-job-status' ]
    elif gridType == 'gLite':
      cmd = [ 'glite-wms-job-status' ]
    else:
      return S_ERROR()
    cmd.extend( pilotRefList )

    gridEnv = dict(os.environ)
    if self.gridEnv:
      self.log.verbose( 'Sourcing GridEnv script:', self.gridEnv )
      ret = Source( 10, [self.gridEnv] )
      if not ret['OK']:
        self.log.error( 'Failed sourcing GridEnv:', ret['Message'] )
        return S_ERROR( 'Failed sourcing GridEnv' )
      if ret['stdout']: self.log.verbose( ret['stdout'] )
      if ret['stderr']: self.log.warn( ret['stderr'] )
      gridEnv = ret['outputEnv']
    ret = gProxyManager.dumpProxyToFile( proxy )
    if not ret['OK']:
      self.log.error( 'Failed to dump Proxy to file' )
      return ret
    gridEnv[ 'X509_USER_PROXY' ] = ret['Value']
    self.log.verbose( 'Executing', ' '.join(cmd) )
    start = time.time()
    ret =  systemCall( 120, cmd, env = gridEnv )
    self.log.info( '%s Job Status Execution Time for %d jobs:' % (gridType,len(pilotRefList)), time.time()-start )

    if not ret['OK']:
      self.log.error( 'Failed to execute %s Job Status' % gridType, ret['Message'] )
      return S_ERROR()
    if ret['Value'][0] != 0:
      stderr = ret['Value'][2]
      stdout = ret['Value'][1]
      deleted = 0
      resultDict = {}
      status = 'Deleted'
      destination = 'Unknown'
      statusDate = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
      deletedJobDict = { 'Status': status,
             'DestinationSite': destination,
             'StatusDate': statusDate,
             'isChild': False,
             'isParent': False,
             'ParentRef': False,
             'FinalStatus' : status in self.finalStateList,
             'ChildRefs' : [] }
      # Glite returns this error for Deleted jobs to std.err
      for job in List.fromChar(stderr,'\nUnable to retrieve the status for:')[1:]:
        pRef = List.fromChar(job,'\n' )[0].strip()
        resultDict[pRef] = deletedJobDict
        self.pilotDB.setPilotStatus( pRef, "Deleted" )
        deleted += 1
      # EDG returns a similar error for Deleted jobs to std.out
      for job in List.fromChar(stdout,'\nUnable to retrieve the status for:')[1:]:
        pRef = List.fromChar(job,'\n' )[0].strip()
        if re.search( "No such file or directory: no matching jobs found", job ):
          resultDict[pRef] = deletedJobDict
          self.pilotDB.setPilotStatus( pRef, "Deleted" )
          deleted += 1
        if re.search( "edg_wll_JobStatus: Connection refused: edg_wll_ssl_connect()", job ):
          # the Broker is not accesible
          return S_ERROR( 'Broker not Available' )
      if not deleted:
        self.log.error( 'Error executing %s Job Status:' % gridType, str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
        return S_ERROR()
      return S_OK( resultDict )

    stdout = ret['Value'][1]
    stderr = ret['Value'][2]   
    resultDict = {}
    for job in List.fromChar(stdout,'\nStatus info for the Job :')[1:]:
      pRef = List.fromChar(job,'\n' )[0].strip()
      resultDict[pRef] = self.__parseJobStatus( job, gridType )      

    return S_OK(resultDict)

  def __parseJobStatus( self, job, gridType ):

    statusRE      = 'Current Status:\s*(\w*)'
    destinationRE = 'Destination:\s*([\w\.-]*)'
    statusDateRE  = 'reached on:\s*....(.*)'

    status      = None
    destination = 'Unknown'
    statusDate  = None
    try:
      status      = re.search(statusRE,      job).group(1)
      if re.search(destinationRE, job):
        destination = re.search(destinationRE, job).group(1)
      if gridType == 'LCG' and re.search(statusDateRE, job):
        statusDate = re.search(statusDateRE, job).group(1)
        statusDate = time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(statusDate,'%b %d %H:%M:%S %Y'))
    except Exception, x:
      self.log.error( 'Error parsing %s Job Status output:\n' % gridType, job )
      
    isParent = False    
    if re.search('Nodes information',job):
      isParent = True
    isChild = False    
    if re.search('Parent Job',job):
      isChild = True   
    
    childRefs = []
    childDicts = {}
    if isParent:
      for subjob in List.fromChar(job,' Status info for the Job :')[1:]:
        chRef = List.fromChar(subjob,'\n' )[0].strip()
        childDict = self.__parseJobStatus( subjob, gridType )  
        childRefs.append(chRef)
        childDicts[chRef] = childDict

    return { 'Status': status,
             'DestinationSite': destination,
             'StatusDate': statusDate,
             'isChild': isChild,
             'isParent': isParent,
             'ParentRef': False,
             'FinalStatus' : status in self.finalStateList,
             'ChildRefs' : childRefs,
             'ChildDicts' : childDicts }

  def __getSiteFromCE( self, ce ):
    siteSections = gConfig.getSections('/Resources/Sites/LCG/')
    if not siteSections['OK']:
      self.log.error('Could not get LCG site list')
      return "unknown"

    sites = siteSections['Value']
    for site in sites:
      lcgCEs = gConfig.getValue('/Resources/Sites/LCG/%s/CE' % site,[])
      if ce in lcgCEs:
        return site

    self.log.error( 'Could not determine DIRAC site name for CE:', ce )
    return "unknown"

  def __addPilotsAccountingReport( self, pilotsData ):
    for pRef in pilotsData:
      pData = pilotsData[pRef]
      pA = PilotAccounting()
      pA.setEndTime( pData[ 'LastUpdateTime' ] )
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
      if not 'Jobs' in pData:
        pA.setValueByKey( 'Jobs', 0 )
      else:
        pA.setValueByKey( 'Jobs', len( pData['Jobs'] ) )
      self.log.verbose( "Added accounting record for pilot %s" % pData[ 'PilotID' ] )
      retVal = gDataStoreClient.addRegister( pA )
      if not retVal[ 'OK' ]:
        return retVal
    return S_OK()
