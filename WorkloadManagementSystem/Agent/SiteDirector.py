########################################################################
# $HeadURL$
# File :    SiteDirector.py
# Author :  A.T.
########################################################################

"""  The Site Director is a simple agent performing pilot job submission to particular sites.
"""

from DIRAC.Core.Base.AgentModule                           import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers              import CSGlobals, Registry, Operations, Resources
from DIRAC.Resources.Computing.ComputingElementFactory     import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client.ServerUtils     import pilotAgentsDB, jobDB
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities   import getGridEnv
from DIRAC.WorkloadManagementSystem.private.ConfigHelper   import findGenericPilotCredentials
from DIRAC                                                 import S_OK, S_ERROR, gConfig
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.AccountingSystem.Client.Types.Pilot             import Pilot as PilotAccounting
from DIRAC.AccountingSystem.Client.DataStoreClient         import gDataStoreClient
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Security                                   import CS
from DIRAC.Core.Utilities.SiteCEMapping                    import getSiteForCE
from DIRAC.Core.Utilities.Time                             import dateTime, second
import os, base64, bz2, tempfile, random, socket
import DIRAC

__RCSID__ = "$Id$"

DIRAC_PILOT = os.path.join( DIRAC.rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'dirac-pilot.py' )
DIRAC_INSTALL = os.path.join( DIRAC.rootPath, 'DIRAC', 'Core', 'scripts', 'dirac-install.py' )
TRANSIENT_PILOT_STATUS = ['Submitted', 'Waiting', 'Running', 'Scheduled', 'Ready']
WAITING_PILOT_STATUS = ['Submitted', 'Waiting', 'Scheduled', 'Ready']
FINAL_PILOT_STATUS = ['Aborted', 'Failed', 'Done']
MAX_PILOTS_TO_SUBMIT = 100
MAX_JOBS_IN_FILLMODE = 5

class SiteDirector( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

  def initialize( self ):
    """ Standard constructor
    """
    self.am_setOption( "PollingTime", 60.0 )
    self.am_setOption( "maxPilotWaitingHours", 6 )
    self.queueDict = {}
    self.maxJobsInFillMode = MAX_JOBS_IN_FILLMODE
    self.maxPilotsToSubmit = MAX_PILOTS_TO_SUBMIT
    return S_OK()

  def beginExecution( self ):

    self.gridEnv = self.am_getOption( "GridEnv", getGridEnv() )
    # The SiteDirector is for a particular user community
    self.vo = self.am_getOption( "Community", '' )
    if not self.vo:
      self.vo = CSGlobals.getVO()
    # The SiteDirector is for a particular user group
    self.group = self.am_getOption( "Group", '' )
    # self.voGroups contain all the eligible user groups for pilots submutted by this SiteDirector
    self.voGroups = []

    # Choose the group for which pilots will be submitted. This is a hack until
    # we will be able to match pilots to VOs.
    if not self.group:
      if self.vo:
        result = Registry.getGroupsForVO( self.vo )
        if not result['OK']:
          return result
        for group in result['Value']:
          if 'NormalUser' in Registry.getPropertiesForGroup( group ):
            self.voGroups.append( group )
    else:
      self.voGroups = [ self.group ]      

    result = findGenericPilotCredentials( vo = self.vo )
    if not result[ 'OK' ]:
      return result
    self.pilotDN, self.pilotGroup = result[ 'Value' ]
    self.pilotDN = self.am_getOption( "PilotDN", self.pilotDN )
    self.pilotGroup = self.am_getOption( "PilotGroup", self.pilotGroup )
   
    self.platforms = [] 
    self.sites = []
    self.defaultSubmitPools = ''
    if self.group:
      self.defaultSubmitPools = Registry.getGroupOption( self.group, 'SubmitPools', '' )
    elif self.vo:
      self.defaultSubmitPools = Registry.getVOOption( self.vo, 'SubmitPools', '' )
      
    self.pilot = self.am_getOption( 'PilotScript', DIRAC_PILOT )
    self.install = DIRAC_INSTALL
    self.workingDirectory = self.am_getOption( 'WorkDirectory' )
    self.maxQueueLength = self.am_getOption( 'MaxQueueLength', 86400 * 3 )
    self.pilotLogLevel = self.am_getOption( 'PilotLogLevel', 'INFO' )
    self.maxJobsInFillMode = self.am_getOption( 'MaxJobsInFillMode', self.maxJobsInFillMode )
    self.maxPilotsToSubmit = self.am_getOption( 'MaxPilotsToSubmit', self.maxPilotsToSubmit )
    self.pilotWaitingFlag = self.am_getOption( 'PilotWaitingFlag', True )
    self.pilotWaitingTime = self.am_getOption( 'MaxPilotWaitingTime', 7200 )

    # Flags
    self.updateStatus = self.am_getOption( 'UpdatePilotStatus', True )
    self.getOutput = self.am_getOption( 'GetPilotOutput', True )
    self.sendAccounting = self.am_getOption( 'SendPilotAccounting', True )

    # Get the site description dictionary
    siteNames = None
    if not self.am_getOption( 'Site', 'Any' ).lower() == "any":
      siteNames = self.am_getOption( 'Site', [] )
    ceTypes = None
    if not self.am_getOption( 'CETypes', 'Any' ).lower() == "any":
      ceTypes = self.am_getOption( 'CETypes', [] )
    ces = None
    if not self.am_getOption( 'CEs', 'Any' ).lower() == "any":
      ces = self.am_getOption( 'CEs', [] )
    result = Resources.getQueues( community = self.vo,
                                  siteList = siteNames,
                                  ceList = ces,
                                  ceTypeList = ceTypes,
                                  mode = 'Direct' )
    if not result['OK']:
      return result
    resourceDict = result['Value']
    result = self.getQueues( resourceDict )
    if not result['OK']:
      return result

    #if not siteNames:
    #  siteName = gConfig.getValue( '/DIRAC/Site', 'Unknown' )
    #  if siteName == 'Unknown':
    #    return S_OK( 'No site specified for the SiteDirector' )
    #  else:
    #    siteNames = [siteName]
    #self.siteNames = siteNames

    if self.updateStatus:
      self.log.always( 'Pilot status update requested' )
    if self.getOutput:
      self.log.always( 'Pilot output retrieval requested' )
    if self.sendAccounting:
      self.log.always( 'Pilot accounting sending requested' )

    self.log.always( 'Sites:', siteNames )
    self.log.always( 'CETypes:', ceTypes )
    self.log.always( 'CEs:', ces )
    self.log.always( 'PilotDN:', self.pilotDN )
    self.log.always( 'PilotGroup:', self.pilotGroup )
    self.log.always( 'MaxPilotsToSubmit:', self.maxPilotsToSubmit )
    self.log.always( 'MaxJobsInFillMode:', self.maxJobsInFillMode )

    self.localhost = socket.getfqdn()
    self.proxy = ''

    if self.queueDict:
      self.log.always( "Agent will serve queues:" )
      for queue in self.queueDict:
        self.log.always( "Site: %s, CE: %s, Queue: %s" % ( self.queueDict[queue]['Site'],
                                                         self.queueDict[queue]['CEName'],
                                                         queue ) )

    return S_OK()

  def getQueues( self, resourceDict ):
    """ Get the list of relevant CEs and their descriptions
    """

    self.queueDict = {}
    ceFactory = ComputingElementFactory()

    for site in resourceDict:
      for ce in resourceDict[site]:
        ceDict = resourceDict[site][ce]
        qDict = ceDict.pop( 'Queues' )
        for queue in qDict:
          queueName = '%s_%s' % ( ce, queue )
          self.queueDict[queueName] = {}
          self.queueDict[queueName]['ParametersDict'] = qDict[queue]
          self.queueDict[queueName]['ParametersDict']['Queue'] = queue
          self.queueDict[queueName]['ParametersDict']['Site'] = site
          self.queueDict[queueName]['ParametersDict']['GridEnv'] = self.gridEnv
          self.queueDict[queueName]['ParametersDict']['Setup'] = gConfig.getValue( '/DIRAC/Setup', 'unknown' )
          # Evaluate the CPU limit of the queue according to the Glue convention
          # To Do: should be a utility
          if "maxCPUTime" in self.queueDict[queueName]['ParametersDict'] and \
             "SI00" in self.queueDict[queueName]['ParametersDict']:
            maxCPUTime = float( self.queueDict[queueName]['ParametersDict']['maxCPUTime'] )
            # For some sites there are crazy values in the CS
            maxCPUTime = max( maxCPUTime, 0 )
            maxCPUTime = min( maxCPUTime, 86400 * 12.5 )
            si00 = float( self.queueDict[queueName]['ParametersDict']['SI00'] )
            queueCPUTime = 60. / 250. * maxCPUTime * si00
            self.queueDict[queueName]['ParametersDict']['CPUTime'] = int( queueCPUTime )
          qwDir = os.path.join( self.workingDirectory, queue )
          if not os.path.exists( qwDir ):
            os.makedirs( qwDir )
          self.queueDict[queueName]['ParametersDict']['WorkingDirectory'] = qwDir
          
          platform = ''
          if "Platform" in self.queueDict[queueName]['ParametersDict']:
            platform = self.queueDict[queueName]['ParametersDict']['Platform']
          elif "Platform" in ceDict:
            platform = ceDict['Platform']
          elif "OS" in ceDict:
            architecture = ceDict.get( 'architecture', 'x86_64' )
            OS = ceDict['OS']
            platform = '_'.join([architecture,OS])
          if platform and not platform in self.platforms:
            self.platforms.append(platform)
            
          if not "Platform" in self.queueDict[queueName]['ParametersDict'] and platform:
            result = Resources.getDIRACPlatform( platform )
            if result['OK']:
              self.queueDict[queueName]['ParametersDict']['Platform'] = result['Value']  
          
          ceQueueDict = dict( ceDict )
          ceQueueDict.update( self.queueDict[queueName]['ParametersDict'] )
          result = ceFactory.getCE( ceName = ce,
                                    ceType = ceDict['CEType'],
                                    ceParametersDict = ceQueueDict )
          if not result['OK']:
            return result
          self.queueDict[queueName]['CE'] = result['Value']
          self.queueDict[queueName]['CEName'] = ce
          self.queueDict[queueName]['CEType'] = ceDict['CEType']
          self.queueDict[queueName]['Site'] = site
          self.queueDict[queueName]['QueueName'] = queue
          result = self.queueDict[queueName]['CE'].isValid()
          if not result['OK']:
            self.log.fatal( result['Message'] )
            return result
          if 'BundleProxy' in self.queueDict[queueName]['ParametersDict']:
            self.queueDict[queueName]['BundleProxy'] = True
          elif 'BundleProxy' in ceDict:
            self.queueDict[queueName]['BundleProxy'] = True  

          if site not in self.sites:
            self.sites.append( site )

    return S_OK()

  def execute( self ):
    """ Main execution method
    """

    if not self.queueDict:
      self.log.warn( 'No site defined, exiting the cycle' )
      return S_OK()

    result = self.submitJobs()
    if not result['OK']:
      self.log.error( 'Errors in the job submission: ', result['Message'] )


    if self.updateStatus:
      result = self.updatePilotStatus()
      if not result['OK']:
        self.log.error( 'Errors in updating pilot status: ', result['Message'] )

    return S_OK()

  def submitJobs( self ):
    """ Go through defined computing elements and submit jobs if necessary
    """

    # Check that there is some work at all
    setup = CSGlobals.getSetup()
    tqDict = { 'Setup':setup,
               'CPUTime': 9999999,
               'SubmitPool' : self.defaultSubmitPools }
    if self.vo:
      tqDict['Community'] = self.vo
    if self.voGroups:
      tqDict['OwnerGroup'] = self.voGroups  
    
    result = Resources.getCompatiblePlatforms( self.platforms )
    if not result['OK']:
      return result
    tqDict['Platform'] = result['Value']
    tqDict['Site'] = self.sites
    
    self.log.verbose( 'Checking overall TQ availability with requirements' )
    self.log.verbose( tqDict )
    
    rpcMatcher = RPCClient( "WorkloadManagement/Matcher" )
    result = rpcMatcher.getMatchingTaskQueues( tqDict )
    if not result[ 'OK' ]:
      return result
    if not result['Value']:
      self.log.verbose( 'No Waiting jobs suitable for the director' )
      return S_OK()

    # Check if the site is allowed in the mask
    result = jobDB.getSiteMask()
    if not result['OK']:
      return S_ERROR( 'Can not get the site mask' )
    siteMaskList = result['Value']

    queues = self.queueDict.keys()
    random.shuffle( queues )
    for queue in queues:
      ce = self.queueDict[queue]['CE']
      ceName = self.queueDict[queue]['CEName']
      ceType = self.queueDict[queue]['CEType']
      queueName = self.queueDict[queue]['QueueName']
      siteName = self.queueDict[queue]['Site']
      siteMask = siteName in siteMaskList

      if 'CPUTime' in self.queueDict[queue]['ParametersDict'] :
        queueCPUTime = int( self.queueDict[queue]['ParametersDict']['CPUTime'] )
      else:
        self.log.warn( 'CPU time limit is not specified for queue %s, skipping...' % queue )
        continue
      if queueCPUTime > self.maxQueueLength:
        queueCPUTime = self.maxQueueLength

      # Get the working proxy
      cpuTime = queueCPUTime + 86400

      self.log.verbose( "Getting pilot proxy for %s/%s %d long" % ( self.pilotDN, self.pilotGroup, cpuTime ) )
      result = gProxyManager.getPilotProxyFromDIRACGroup( self.pilotDN, self.pilotGroup, cpuTime )
      if not result['OK']:
        return result
      self.proxy = result['Value']
      ce.setProxy( self.proxy, cpuTime - 60 )

      # Get the number of available slots on the target site/queue
      result = ce.available()
      if not result['OK']:
        self.log.warn( 'Failed to check the availability of queue %s: \n%s' % ( queue, result['Message'] ) )
        continue
      ceInfoDict = result['CEInfoDict']
      self.log.info( "CE queue report(%s_%s): Wait=%d, Run=%d, Submitted=%d, Max=%d" % \
                     ( ceName, queueName, ceInfoDict['WaitingJobs'], ceInfoDict['RunningJobs'],
                       ceInfoDict['SubmittedJobs'], ceInfoDict['MaxTotalJobs'] ) )

      totalSlots = result['Value']

      ceDict = ce.getParameterDict()
      ceDict[ 'GridCE' ] = ceName
      if not siteMask and 'Site' in ceDict:
        self.log.info( 'Site not in the mask %s' % siteName )
        self.log.info( 'Removing "Site" from matching Dict' )
        del ceDict[ 'Site' ]
      if self.vo:
        ceDict['Community'] = self.vo
      if self.voGroups:
        ceDict['OwnerGroup'] = self.voGroups
      
      # This is a hack to get rid of !
      ceDict['SubmitPool'] = self.defaultSubmitPools  

      result = Resources.getCompatiblePlatforms( self.platforms )
      if not result['OK']:
        continue
      ceDict['Platform'] = result['Value']

      # Get the number of eligible jobs for the target site/queue
      result = rpcMatcher.getMatchingTaskQueues( ceDict )
      if not result['OK']:
        self.log.error( 'Could not retrieve TaskQueues from TaskQueueDB', result['Message'] )
        return result
      taskQueueDict = result['Value']
      if not taskQueueDict:
        self.log.info( 'No matching TQs found' )
        continue

      totalTQJobs = 0
      tqIDList = taskQueueDict.keys()
      for tq in taskQueueDict:
        totalTQJobs += taskQueueDict[tq]['Jobs']

      pilotsToSubmit = min( totalSlots, totalTQJobs )

      # Get the number of already waiting pilots for this queue
      totalWaitingPilots = 0
      if self.pilotWaitingFlag:
        lastUpdateTime = dateTime() - self.pilotWaitingTime * second
        result = pilotAgentsDB.countPilots( { 'TaskQueueID': tqIDList,
                                              'Status': WAITING_PILOT_STATUS },
                                            None, lastUpdateTime )
        if not result['OK']:
          self.log.error( 'Failed to get Number of Waiting pilots', result['Message'] )
          totalWaitingPilots = 0
        else:
          totalWaitingPilots = result['Value']
          self.log.verbose( 'Waiting Pilots for TaskQueue %s:' % tqIDList, totalWaitingPilots )

      pilotsToSubmit = max( 0, min( totalSlots, totalTQJobs-totalWaitingPilots ) )
      self.log.info( 'Available slots=%d, TQ jobs=%d, Waiting Pilots=%d, Pilots to submit=%d' % \
                              ( totalSlots, totalTQJobs, totalWaitingPilots, pilotsToSubmit ) )

      # Limit the number of pilots to submit to MAX_PILOTS_TO_SUBMIT
      pilotsToSubmit = min( self.maxPilotsToSubmit, pilotsToSubmit )

      while pilotsToSubmit > 0:
        self.log.info( 'Going to submit %d pilots to %s queue' % ( pilotsToSubmit, queue ) )

        bundleProxy = self.queueDict[queue].get( 'BundleProxy', False )
        jobExecDir = ''
        if ceType == 'CREAM':
          jobExecDir = '.'
        jobExecDir = self.queueDict[queue].get( 'JobExecDir', jobExecDir )
        httpProxy = self.queueDict[queue].get( 'HttpProxy', '' )

        result = self.__getExecutable( queue, pilotsToSubmit, bundleProxy, httpProxy, jobExecDir )
        if not result['OK']:
          return result

        executable, pilotSubmissionChunk = result['Value']
        result = ce.submitJob( executable, '', pilotSubmissionChunk )
        if not result['OK']:
          self.log.error( 'Failed submission to queue %s:\n' % queue, result['Message'] )
          pilotsToSubmit = 0
          continue
        
        pilotsToSubmit = pilotsToSubmit - pilotSubmissionChunk
        # Add pilots to the PilotAgentsDB assign pilots to TaskQueue proportionally to the
        # task queue priorities
        pilotList = result['Value']
        self.log.info( 'Submitted %d pilots to %s@%s' % ( len( pilotList), queueName, ceName ) )
        stampDict = {}
        if result.has_key( 'PilotStampDict' ):
          stampDict = result['PilotStampDict']
        tqPriorityList = []
        sumPriority = 0.
        for tq in taskQueueDict:
          sumPriority += taskQueueDict[tq]['Priority']
          tqPriorityList.append( ( tq, sumPriority ) )
        rndm = random.random()*sumPriority
        tqDict = {}
        for pilotID in pilotList:
          rndm = random.random()*sumPriority
          for tq, prio in tqPriorityList:
            if rndm < prio:
              tqID = tq
              break
          if not tqDict.has_key( tqID ):
            tqDict[tqID] = []
          tqDict[tqID].append( pilotID )

        for tqID, pilotList in tqDict.items():
          result = pilotAgentsDB.addPilotTQReference( pilotList,
                                                     tqID,
                                                     self.pilotDN,
                                                     self.pilotGroup,
                                                     self.localhost,
                                                     ceType,
                                                     '',
                                                     stampDict )
          if not result['OK']:
            self.log.error( 'Failed add pilots to the PilotAgentsDB: ', result['Message'] )
            continue
          for pilot in pilotList:
            result = pilotAgentsDB.setPilotStatus( pilot, 'Submitted', ceName,
                                                  'Successfully submitted by the SiteDirector',
                                                  siteName, queueName )
            if not result['OK']:
              self.log.error( 'Failed to set pilot status: ', result['Message'] )
              continue

    return S_OK()

#####################################################################################
  def __getExecutable( self, queue, pilotsToSubmit, bundleProxy = True, httpProxy = '', jobExecDir = '' ):
    """ Prepare the full executable for queue
    """

    proxy = None
    if bundleProxy:
      proxy = self.proxy
    pilotOptions, pilotsToSubmit = self.__getPilotOptions( queue, pilotsToSubmit )
    if pilotOptions is None:
      return S_ERROR( 'Errors in compiling pilot options' )
    executable = self.__writePilotScript( self.workingDirectory, pilotOptions, proxy, httpProxy, jobExecDir )
    return S_OK( [ executable, pilotsToSubmit ] )

#####################################################################################
  def __getPilotOptions( self, queue, pilotsToSubmit ):
    """ Prepare pilot options
    """

    queueDict = self.queueDict[queue]['ParametersDict']
    pilotOptions = []

    setup = gConfig.getValue( "/DIRAC/Setup", "unknown" )
    if setup == 'unknown':
      self.log.error( 'Setup is not defined in the configuration' )
      return [ None, None ]
    pilotOptions.append( '-S %s' % setup )
    opsHelper = Operations.Operations( group = self.pilotGroup, setup = setup )

    #Installation defined?
    installationName = opsHelper.getValue( "Pilot/Installation", "" )
    if installationName:
      pilotOptions.append( '-V %s' % installationName )

    #Project defined?
    projectName = opsHelper.getValue( "Pilot/Project", "" )
    if projectName:
      pilotOptions.append( '-l %s' % projectName )
    else:
      self.log.info( 'DIRAC project will be installed by pilots' )

    #Request a release
    diracVersion = opsHelper.getValue( "Pilot/Version", [] )
    if not diracVersion:
      self.log.error( 'Pilot/Version is not defined in the configuration' )
      return [ None, None ]
    #diracVersion is a list of accepted releases. Just take the first one
    pilotOptions.append( '-r %s' % diracVersion[0] )

    ownerDN = self.pilotDN
    ownerGroup = self.pilotGroup
    # Request token for maximum pilot efficiency
    result = gProxyManager.requestToken( ownerDN, ownerGroup, pilotsToSubmit * self.maxJobsInFillMode )
    if not result[ 'OK' ]:
      self.log.error( 'Invalid proxy token request', result['Message'] )
      return [ None, None ]
    ( token, numberOfUses ) = result[ 'Value' ]
    pilotOptions.append( '-o /Security/ProxyToken=%s' % token )
    # Use Filling mode
    pilotOptions.append( '-M %s' % min( numberOfUses, self.maxJobsInFillMode ) )

    # Since each pilot will execute min( numberOfUses, self.maxJobsInFillMode )
    # with numberOfUses tokens we can submit at most: 
    #    numberOfUses / min( numberOfUses, self.maxJobsInFillMode )
    # pilots
    newPilotsToSubmit = numberOfUses / min( numberOfUses, self.maxJobsInFillMode )
    if newPilotsToSubmit != pilotsToSubmit:
      self.log.info( 'Number of pilots to submit is changed to %d after getting the proxy token' % newPilotsToSubmit )
      pilotsToSubmit = newPilotsToSubmit
    # Debug
    if self.pilotLogLevel.lower() == 'debug':
      pilotOptions.append( '-d' )
    # CS Servers
    csServers = gConfig.getValue( "/DIRAC/Configuration/Servers", [] )
    pilotOptions.append( '-C %s' % ",".join( csServers ) )
    # DIRAC Extensions
    extensionsList = CSGlobals.getCSExtensions()
    if extensionsList:
      pilotOptions.append( '-e %s' % ",".join( extensionsList ) )
    # Requested CPU time
    pilotOptions.append( '-T %s' % queueDict['CPUTime'] )
    # CEName
    pilotOptions.append( '-N %s' % self.queueDict[queue]['CEName'] )
    # SiteName
    pilotOptions.append( '-n %s' % queueDict['Site'] )
    if 'ClientPlatform' in queueDict:
      pilotOptions.append( "-p '%s'" % queueDict['ClientPlatform'] )

    if 'SharedArea' in queueDict:
      pilotOptions.append( "-o '/LocalSite/SharedArea=%s'" % queueDict['SharedArea'] )

    if 'SI00' in queueDict:
      factor = float( queueDict['SI00'] ) / 250.
      pilotOptions.append( "-o '/LocalSite/CPUScalingFactor=%s'" % factor )
      pilotOptions.append( "-o '/LocalSite/CPUNormalizationFactor=%s'" % factor )
    else:
      if 'CPUScalingFactor' in queueDict:
        pilotOptions.append( "-o '/LocalSite/CPUScalingFactor=%s'" % queueDict['CPUScalingFactor'] )
      if 'CPUNormalizationFactor' in queueDict:
        pilotOptions.append( "-o '/LocalSite/CPUNormalizationFactor=%s'" % queueDict['CPUNormalizationFactor'] )

    # Hack
    if self.defaultSubmitPools:
      pilotOptions.append( '-o /Resources/Computing/CEDefaults/SubmitPool=%s' %  self.defaultSubmitPools )

    if self.group:
      pilotOptions.append( '-G %s' % self.group )

    self.log.verbose( "pilotOptions: ", ' '.join( pilotOptions ) )

    return [ pilotOptions, pilotsToSubmit ]

#####################################################################################
  def __writePilotScript( self, workingDirectory, pilotOptions, proxy = None, httpProxy = '', pilotExecDir = '' ):
    """ Bundle together and write out the pilot executable script, admixt the proxy if given
    """

    try:
      compressedAndEncodedProxy = ''
      proxyFlag = 'False'
      if proxy is not None:
        compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) )
        proxyFlag = 'True'
      compressedAndEncodedPilot = base64.encodestring( bz2.compress( open( self.pilot, "rb" ).read(), 9 ) )
      compressedAndEncodedInstall = base64.encodestring( bz2.compress( open( self.install, "rb" ).read(), 9 ) )
    except:
      self.log.exception( 'Exception during file compression of proxy, dirac-pilot or dirac-install' )
      return S_ERROR( 'Exception during file compression of proxy, dirac-pilot or dirac-install' )

    localPilot = """#!/bin/bash
/usr/bin/env python << EOF
#
import os, tempfile, sys, shutil, base64, bz2
try:
  pilotExecDir = '%(pilotExecDir)s'
  if not pilotExecDir:
    pilotExecDir = None
  pilotWorkingDirectory = tempfile.mkdtemp( suffix = 'pilot', prefix = 'DIRAC_', dir = pilotExecDir )
  pilotWorkingDirectory = os.path.realpath( pilotWorkingDirectory )
  os.chdir( pilotWorkingDirectory )
  if %(proxyFlag)s:
    open( 'proxy', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedProxy)s\"\"\" ) ) )
    os.chmod("proxy",0600)
    os.environ["X509_USER_PROXY"]=os.path.join(pilotWorkingDirectory, 'proxy')
  open( '%(pilotScript)s', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedPilot)s\"\"\" ) ) )
  open( '%(installScript)s', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedInstall)s\"\"\" ) ) )
  os.chmod("%(pilotScript)s",0700)
  os.chmod("%(installScript)s",0700)
  if "LD_LIBRARY_PATH" not in os.environ:
    os.environ["LD_LIBRARY_PATH"]=""
  if "%(httpProxy)s":
    os.environ["HTTP_PROXY"]="%(httpProxy)s"
  os.environ["X509_CERT_DIR"]=os.path.join(pilotWorkingDirectory, 'etc/grid-security/certificates')
  # TODO: structure the output
  print '==========================================================='
  print 'Environment of execution host'
  for key in os.environ.keys():
    print key + '=' + os.environ[key]
  print '==========================================================='
except Exception, x:
  print >> sys.stderr, x
  sys.exit(-1)
cmd = "python %(pilotScript)s %(pilotOptions)s"
print 'Executing: ', cmd
sys.stdout.flush()
os.system( cmd )

shutil.rmtree( pilotWorkingDirectory )

EOF
""" % { 'compressedAndEncodedProxy': compressedAndEncodedProxy,
        'compressedAndEncodedPilot': compressedAndEncodedPilot,
        'compressedAndEncodedInstall': compressedAndEncodedInstall,
        'httpProxy': httpProxy,
        'pilotExecDir': pilotExecDir,
        'pilotScript': os.path.basename( self.pilot ),
        'installScript': os.path.basename( self.install ),
        'pilotOptions': ' '.join( pilotOptions ),
        'proxyFlag': proxyFlag }

    fd, name = tempfile.mkstemp( suffix = '_pilotwrapper.py', prefix = 'DIRAC_', dir = workingDirectory )
    pilotWrapper = os.fdopen( fd, 'w' )
    pilotWrapper.write( localPilot )
    pilotWrapper.close()
    return name

  def updatePilotStatus( self ):
    """ Update status of pilots in transient states
    """
    for queue in self.queueDict:
      ce = self.queueDict[queue]['CE']
      ceName = self.queueDict[queue]['CEName']
      queueName = self.queueDict[queue]['QueueName']
      ceType = self.queueDict[queue]['CEType']
      siteName = self.queueDict[queue]['Site']

      result = pilotAgentsDB.selectPilots( {'DestinationSite':ceName,
                                           'Queue':queueName,
                                           'GridType':ceType,
                                           'GridSite':siteName,
                                           'Status':TRANSIENT_PILOT_STATUS} )
      if not result['OK']:
        self.log.error( 'Failed to select pilots: %s' % result['Message'] )
        continue
      pilotRefs = result['Value']
      if not pilotRefs:
        continue

      #print "AT >>> pilotRefs", pilotRefs

      result = pilotAgentsDB.getPilotInfo( pilotRefs )
      if not result['OK']:
        self.log.error( 'Failed to get pilots info from DB', result['Message'] )
        continue
      pilotDict = result['Value']

      #print "AT >>> pilotDict", pilotDict

      stampedPilotRefs = []
      for pRef in pilotDict:
        if pilotDict[pRef]['PilotStamp']:
          stampedPilotRefs.append( pRef + ":::" + pilotDict[pRef]['PilotStamp'] )
        else:
          stampedPilotRefs = list( pilotRefs )
          break

      result = ce.isProxyValid()
      if not result['OK']:
        result = gProxyManager.getPilotProxyFromDIRACGroup( self.pilotDN, self.pilotGroup, 600 )
        if not result['OK']:
          return result
        self.proxy = result['Value']
        ce.setProxy( self.proxy, 500 )

      result = ce.getJobStatus( stampedPilotRefs )
      if not result['OK']:
        self.log.error( 'Failed to get pilots status from CE', '%s: %s' % ( ceName, result['Message'] ) )
        continue
      pilotCEDict = result['Value']

      #print "AT >>> pilotCEDict", pilotCEDict

      for pRef in pilotRefs:
        newStatus = ''
        oldStatus = pilotDict[pRef]['Status']
        ceStatus = pilotCEDict[pRef]
        if oldStatus == ceStatus:
          # Status did not change, continue
          continue
        elif ceStatus == "Unknown" and not oldStatus in FINAL_PILOT_STATUS:
          # Pilot finished without reporting, consider it Aborted
          newStatus = 'Aborted'
        elif ceStatus != 'Unknown' :
          # Update the pilot status to the new value
          newStatus = ceStatus

        if newStatus:
          self.log.info( 'Updating status to %s for pilot %s' % ( newStatus, pRef ) )
          result = pilotAgentsDB.setPilotStatus( pRef, newStatus, '', 'Updated by SiteDirector' )
        # Retrieve the pilot output now
        if newStatus in FINAL_PILOT_STATUS:
          if pilotDict[pRef]['OutputReady'].lower() == 'false' and self.getOutput:
            self.log.info( 'Retrieving output for pilot %s' % pRef )
            pilotStamp = pilotDict[pRef]['PilotStamp']
            pRefStamp = pRef
            if pilotStamp:
              pRefStamp = pRef + ':::' + pilotStamp
            result = ce.getJobOutput( pRefStamp )
            if not result['OK']:
              self.log.error( 'Failed to get pilot output', '%s: %s' % ( ceName, result['Message'] ) )
            else:
              output, error = result['Value']
              if output:
                result = pilotAgentsDB.storePilotOutput( pRef, output, error )
                if not result['OK']:
                  self.log.error( 'Failed to store pilot output', result['Message'] )
              else:
                self.log.warn( 'Empty pilot output not stored to PilotDB' )    

    # The pilot can be in Done state set by the job agent check if the output is retrieved
    for queue in self.queueDict:
      ce = self.queueDict[queue]['CE']

      if not ce.isProxyValid( 120 ):
        result = gProxyManager.getPilotProxyFromDIRACGroup( self.pilotDN, self.pilotGroup, 1000 )
        if not result['OK']:
          return result
        ce.setProxy( self.proxy, 940 )

      ceName = self.queueDict[queue]['CEName']
      queueName = self.queueDict[queue]['QueueName']
      ceType = self.queueDict[queue]['CEType']
      siteName = self.queueDict[queue]['Site']
      result = pilotAgentsDB.selectPilots( {'DestinationSite':ceName,
                                           'Queue':queueName,
                                           'GridType':ceType,
                                           'GridSite':siteName,
                                           'OutputReady':'False',
                                           'Status':FINAL_PILOT_STATUS} )

      if not result['OK']:
        self.log.error( 'Failed to select pilots', result['Message'] )
        continue
      pilotRefs = result['Value']
      if not pilotRefs:
        continue
      result = pilotAgentsDB.getPilotInfo( pilotRefs )
      if not result['OK']:
        self.log.error( 'Failed to get pilots info from DB', result['Message'] )
        continue
      pilotDict = result['Value']
      if self.getOutput:
        for pRef in pilotRefs:
          self.log.info( 'Retrieving output for pilot %s' % pRef )
          pilotStamp = pilotDict[pRef]['PilotStamp']
          pRefStamp = pRef
          if pilotStamp:
            pRefStamp = pRef + ':::' + pilotStamp
          result = ce.getJobOutput( pRefStamp )
          if not result['OK']:
            self.log.error( 'Failed to get pilot output', '%s: %s' % ( ceName, result['Message'] ) )
          else:
            output, error = result['Value']
            result = pilotAgentsDB.storePilotOutput( pRef, output, error )
            if not result['OK']:
              self.log.error( 'Failed to store pilot output', result['Message'] )

      # Check if the accounting is to be sent
      if self.sendAccounting:
        result = pilotAgentsDB.selectPilots( {'DestinationSite':ceName,
                                             'Queue':queueName,
                                             'GridType':ceType,
                                             'GridSite':siteName,
                                             'AccountingSent':'False',
                                             'Status':FINAL_PILOT_STATUS} )

        if not result['OK']:
          self.log.error( 'Failed to select pilots', result['Message'] )
          continue
        pilotRefs = result['Value']
        if not pilotRefs:
          continue
        result = pilotAgentsDB.getPilotInfo( pilotRefs )
        if not result['OK']:
          self.log.error( 'Failed to get pilots info from DB', result['Message'] )
          continue
        pilotDict = result['Value']
        result = self.sendPilotAccounting( pilotDict )
        if not result['OK']:
          self.log.error( 'Failed to send pilot agent accounting' )

    return S_OK()

  def sendPilotAccounting( self, pilotDict ):
    """ Send pilot accounting record
    """
    for pRef in pilotDict:
      self.log.verbose( 'Preparing accounting record for pilot %s' % pRef )
      pA = PilotAccounting()
      pA.setEndTime( pilotDict[pRef][ 'LastUpdateTime' ] )
      pA.setStartTime( pilotDict[pRef][ 'SubmissionTime' ] )
      retVal = CS.getUsernameForDN( pilotDict[pRef][ 'OwnerDN' ] )
      if not retVal[ 'OK' ]:
        userName = 'unknown'
        self.log.error( "Can't determine username for dn:", pilotDict[pRef][ 'OwnerDN' ] )
      else:
        userName = retVal[ 'Value' ]
      pA.setValueByKey( 'User', userName )
      pA.setValueByKey( 'UserGroup', pilotDict[pRef][ 'OwnerGroup' ] )
      result = getSiteForCE( pilotDict[pRef][ 'DestinationSite' ] )
      if result['OK'] and result[ 'Value' ].strip():
        pA.setValueByKey( 'Site', result['Value'].strip() )
      else:
        pA.setValueByKey( 'Site', 'Unknown' )
      pA.setValueByKey( 'GridCE', pilotDict[pRef][ 'DestinationSite' ] )
      pA.setValueByKey( 'GridMiddleware', pilotDict[pRef][ 'GridType' ] )
      pA.setValueByKey( 'GridResourceBroker', pilotDict[pRef][ 'Broker' ] )
      pA.setValueByKey( 'GridStatus', pilotDict[pRef][ 'Status' ] )
      if not 'Jobs' in pilotDict[pRef]:
        pA.setValueByKey( 'Jobs', 0 )
      else:
        pA.setValueByKey( 'Jobs', len( pilotDict[pRef]['Jobs'] ) )
      self.log.info( "Adding accounting record for pilot %s" % pilotDict[pRef][ 'PilotID' ] )
      retVal = gDataStoreClient.addRegister( pA )
      if not retVal[ 'OK' ]:
        self.log.error( 'Failed to send accounting info for pilot ', pRef )
      else:
        # Set up AccountingSent flag
        result = pilotAgentsDB.setAccountingFlag( pRef )
        if not result['OK']:
          self.log.error( 'Failed to set accounting flag for pilot ', pRef )

    self.log.info( 'Committing accounting records for %d pilots' % len( pilotDict ) )
    result = gDataStoreClient.commit()
    if result['OK']:
      for pRef in pilotDict:
        self.log.verbose( 'Setting AccountingSent flag for pilot %s' % pRef )
        result = pilotAgentsDB.setAccountingFlag( pRef )
        if not result['OK']:
          self.log.error( 'Failed to set accounting flag for pilot ', pRef )
    else:
      return result

    return S_OK()
  
