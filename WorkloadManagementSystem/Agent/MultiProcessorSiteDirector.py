"""  The Multi Processor Site Director is an agent performing pilot job submission to particular sites. It is able to handle multicore jobs.
"""
import os
import random

import DIRAC
from DIRAC                                                 import S_OK, S_ERROR, gConfig
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector     import SiteDirector, WAITING_PILOT_STATUS
from DIRAC.ConfigurationSystem.Client.Helpers              import CSGlobals, Registry, Operations, Resources
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.WorkloadManagementSystem.Client.ServerUtils     import pilotAgentsDB, jobDB
from DIRAC.Core.Utilities.Time                             import dateTime, second
from DIRAC.Core.Utilities.List                             import fromChar

__RCSID__ = "$Id$"

class MultiProcessorSiteDirector( SiteDirector ):
  def getQueues( self, resourceDict ):
    """ Get the list of relevant CEs and their descriptions
    """
    result = SiteDirector.getQueues( self, resourceDict )
    if not result['OK']: return result

    for queueName in self.queueDict.keys():
      ce = self.queueDict[queueName]['CEName']
      site = self.queueDict[queueName]['Site']
      ceDef = resourceDict[site][ce]

      ceMaxProcessors = ceDef.get( 'MaxProcessors' )
      maxProcessors = self.queueDict[queueName]['ParametersDict'].get( 'MaxProcessors', ceMaxProcessors )
      if maxProcessors:
        maxProcessorsList = range( 1, int( maxProcessors ) + 1 )
        processorsTags = ['%dProcessors' % processors for processors in maxProcessorsList]
        if processorsTags:
          self.queueDict[queueName]['ParametersDict'].setdefault( 'Tag', [] )
          self.queueDict[queueName]['ParametersDict']['Tag'] += processorsTags

      ceWholeNode = ceDef.get( 'WholeNode', 'false' )
      wholeNode = self.queueDict[queueName]['ParametersDict'].get( 'WholeNode', ceWholeNode )
      if wholeNode.lower() in ( 'yes', 'true' ):
        self.queueDict[queueName]['ParametersDict'].setdefault( 'Tag', [] )
        self.queueDict[queueName]['ParametersDict']['Tag'].append( 'WholeNode' )

      if 'Tag' not in self.queueDict[queueName]['ParametersDict']:
        del self.queueDict[queueName]
      else:
        tags = self.queueDict[queueName]['ParametersDict']['Tag']
        if '2Processors' not in tags and 'WholeNode' not in tags:
          del self.queueDict[queueName]

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
    tqDict['Tag'] = []
    self.log.verbose( 'Checking overall TQ availability with requirements' )
    self.log.verbose( tqDict )

    rpcMatcher = RPCClient( "WorkloadManagement/Matcher" )
    result = rpcMatcher.getMatchingTaskQueues( tqDict )
    if not result[ 'OK' ]:
      return result
    if not result['Value']:
      self.log.verbose( 'No Waiting jobs suitable for the director' )
      return S_OK()

    jobSites = set()
    anySite = False
    testSites = set()
    totalWaitingJobs = 0
    for tqID in result['Value']:
      if "Sites" in result['Value'][tqID]:
        for site in result['Value'][tqID]['Sites']:
          if site.lower() != 'any':
            jobSites.add( site )
          else:
            anySite = True
      else:
        anySite = True
      if "JobTypes" in result['Value'][tqID]:
        if "Sites" in result['Value'][tqID]:
          for site in result['Value'][tqID]['Sites']:
            if site.lower() != 'any':
              testSites.add( site )
      totalWaitingJobs += result['Value'][tqID]['Jobs']

    tqIDList = result['Value'].keys()
    self.log.info( tqIDList )
    result = pilotAgentsDB.countPilots( { 'TaskQueueID': tqIDList,
                                          'Status': WAITING_PILOT_STATUS },
                                           None )
    totalWaitingPilots = 0
    if result['OK']:
      totalWaitingPilots = result['Value']
    self.log.info( 'Total %d jobs in %d task queues with %d waiting pilots' % ( totalWaitingJobs, len( tqIDList ), totalWaitingPilots ) )
    self.log.info( 'Queues: ', self.queueDict.keys() )
    # if totalWaitingPilots >= totalWaitingJobs:
    #  self.log.info( 'No more pilots to be submitted in this cycle' )
    #  return S_OK()

    # Check if the site is allowed in the mask
    result = jobDB.getSiteMask()
    if not result['OK']:
      return S_ERROR( 'Can not get the site mask' )
    siteMaskList = result['Value']

    queues = self.queueDict.keys()
    random.shuffle( queues )
    totalSubmittedPilots = 0
    matchedQueues = 0
    for queue in queues:

      # Check if the queue failed previously
      failedCount = self.failedQueues[ queue ] % self.failedQueueCycleFactor
      if failedCount != 0:
        self.log.warn( "%s queue failed recently, skipping %d cycles" % ( queue, 10 - failedCount ) )
        self.failedQueues[queue] += 1
        continue

      ce = self.queueDict[queue]['CE']
      ceName = self.queueDict[queue]['CEName']
      ceType = self.queueDict[queue]['CEType']
      queueName = self.queueDict[queue]['QueueName']
      siteName = self.queueDict[queue]['Site']
      platform = self.queueDict[queue]['Platform']
      siteMask = siteName in siteMaskList

      if not anySite and siteName not in jobSites:
        self.log.verbose( "Skipping queue %s at %s: no workload expected" % ( queueName, siteName ) )
        continue
      if not siteMask and siteName not in testSites:
        self.log.verbose( "Skipping queue %s at site %s not in the mask" % ( queueName, siteName ) )
        continue

      if 'CPUTime' in self.queueDict[queue]['ParametersDict'] :
        queueCPUTime = int( self.queueDict[queue]['ParametersDict']['CPUTime'] )
      else:
        self.log.warn( 'CPU time limit is not specified for queue %s, skipping...' % queue )
        continue
      if queueCPUTime > self.maxQueueLength:
        queueCPUTime = self.maxQueueLength

      # Prepare the queue description to look for eligible jobs
      ceDict = ce.getParameterDict()
      ceDict[ 'GridCE' ] = ceName
      # if not siteMask and 'Site' in ceDict:
      #  self.log.info( 'Site not in the mask %s' % siteName )
      #  self.log.info( 'Removing "Site" from matching Dict' )
      #  del ceDict[ 'Site' ]
      if not siteMask:
        ceDict['JobType'] = "Test"
      if self.vo:
        ceDict['Community'] = self.vo
      if self.voGroups:
        ceDict['OwnerGroup'] = self.voGroups

      # This is a hack to get rid of !
      ceDict['SubmitPool'] = self.defaultSubmitPools

      result = Resources.getCompatiblePlatforms( platform )
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
        self.log.verbose( 'No matching TQs found for %s' % queue )
        continue

      matchedQueues += 1
      totalTQJobs = 0
      tqIDList = taskQueueDict.keys()
      for tq in taskQueueDict:
        totalTQJobs += taskQueueDict[tq]['Jobs']

      self.log.verbose( '%d job(s) from %d task queue(s) are eligible for %s queue' % ( totalTQJobs, len( tqIDList ), queue ) )

      # Get the number of already waiting pilots for these task queues
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
      if totalWaitingPilots >= totalTQJobs:
        self.log.verbose( "%d waiting pilots already for all the available jobs" % totalWaitingPilots )
        continue

      self.log.verbose( "%d waiting pilots for the total of %d eligible jobs for %s" % ( totalWaitingPilots, totalTQJobs, queue ) )

      # Get the working proxy
      cpuTime = queueCPUTime + 86400
      self.log.verbose( "Getting pilot proxy for %s/%s %d long" % ( self.pilotDN, self.pilotGroup, cpuTime ) )
      result = gProxyManager.getPilotProxyFromDIRACGroup( self.pilotDN, self.pilotGroup, cpuTime )
      if not result['OK']:
        return result
      self.proxy = result['Value']
      ce.setProxy( self.proxy, cpuTime - 60 )

      # Get the number of available slots on the target site/queue
      totalSlots = self.__getQueueSlots( queue )
      if totalSlots == 0:
        self.log.debug( '%s: No slots available' % queue )
        continue

      pilotsToSubmit = max( 0, min( totalSlots, totalTQJobs - totalWaitingPilots ) )
      self.log.info( '%s: Slots=%d, TQ jobs=%d, Pilots: waiting %d, to submit=%d' % \
                              ( queue, totalSlots, totalTQJobs, totalWaitingPilots, pilotsToSubmit ) )

      # Limit the number of pilots to submit to MAX_PILOTS_TO_SUBMIT
      pilotsToSubmit = min( self.maxPilotsToSubmit, pilotsToSubmit )

      while pilotsToSubmit > 0:
        self.log.info( 'Going to submit %d pilots to %s queue' % ( pilotsToSubmit, queue ) )

        bundleProxy = self.queueDict[queue].get( 'BundleProxy', False )
        jobExecDir = ''
        jobExecDir = self.queueDict[queue]['ParametersDict'].get( 'JobExecDir', jobExecDir )
        httpProxy = self.queueDict[queue]['ParametersDict'].get( 'HttpProxy', '' )

        result = self.__getExecutable( queue, pilotsToSubmit, bundleProxy, httpProxy, jobExecDir )
        if not result['OK']:
          return result

        executable, pilotSubmissionChunk = result['Value']
        result = ce.submitJob( executable, '', pilotSubmissionChunk )
        # ## FIXME: The condor thing only transfers the file with some
        # ## delay, so when we unlink here the script is gone
        # ## FIXME 2: but at some time we need to clean up the pilot wrapper scripts...
        if ceType != 'HTCondorCE':
          os.unlink( executable )
        if not result['OK']:
          self.log.error( 'Failed submission to queue %s:\n' % queue, result['Message'] )
          pilotsToSubmit = 0
          self.failedQueues[queue] += 1
          continue

        pilotsToSubmit = pilotsToSubmit - pilotSubmissionChunk
        # Add pilots to the PilotAgentsDB assign pilots to TaskQueue proportionally to the
        # task queue priorities
        pilotList = result['Value']
        self.queueSlots[queue]['AvailableSlots'] -= len( pilotList )
        totalSubmittedPilots += len( pilotList )
        self.log.info( 'Submitted %d pilots to %s@%s' % ( len( pilotList ), queueName, ceName ) )
        stampDict = {}
        if result.has_key( 'PilotStampDict' ):
          stampDict = result['PilotStampDict']
        tqPriorityList = []
        sumPriority = 0.
        for tq in taskQueueDict:
          sumPriority += taskQueueDict[tq]['Priority']
          tqPriorityList.append( ( tq, sumPriority ) )
        rndm = random.random() * sumPriority
        tqDict = {}
        for pilotID in pilotList:
          rndm = random.random() * sumPriority
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

    self.log.info( "%d pilots submitted in total in this cycle, %d matched queues" % ( totalSubmittedPilots, matchedQueues ) )
    return S_OK()
