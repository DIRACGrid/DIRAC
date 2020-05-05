""" The Multi Processor Site Director is an agent performing pilot job submission to particular sites.
    It is able to handle multicore jobs.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import random
import re

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import pilotAgentsDB
from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector, WAITING_PILOT_STATUS


class MultiProcessorSiteDirector(SiteDirector):

  def getQueues(self, resourceDict):
    """ Get the list of relevant CEs and their descriptions
    """

    result = SiteDirector.getQueues(self, resourceDict)
    if not result['OK']:
      return result

    remQueues = []
    for queueName in self.queueDict:
      ce = self.queueDict[queueName]['CEName']
      site = self.queueDict[queueName]['Site']
      ceDef = resourceDict[site][ce]

      ceMaxProcessors = ceDef.get('MaxProcessors')
      maxProcessors = self.queueDict[queueName]['ParametersDict'].get('MaxProcessors', ceMaxProcessors)
      if maxProcessors:
        maxProcessorsList = range(1, int(maxProcessors) + 1)
        processorsTags = ['%dProcessors' % processors for processors in maxProcessorsList]
        if processorsTags:
          self.queueDict[queueName]['ParametersDict'].setdefault('Tag', [])
          self.queueDict[queueName]['ParametersDict']['Tag'] += processorsTags

      ceWholeNode = ceDef.get('WholeNode', 'false')
      wholeNode = self.queueDict[queueName]['ParametersDict'].get('WholeNode', ceWholeNode)
      if wholeNode.lower() in ('yes', 'true'):
        self.queueDict[queueName]['ParametersDict'].setdefault('Tag', [])
        self.queueDict[queueName]['ParametersDict']['Tag'].append('WholeNode')

      if 'Tag' not in self.queueDict[queueName]['ParametersDict']:
        remQueues.append(queueName)
      else:
        tags = self.queueDict[queueName]['ParametersDict']['Tag']
        if '2Processors' not in tags and 'WholeNode' not in tags:
          remQueues.append(queueName)

    for queueName in remQueues:
      del self.queueDict[queueName]

    return S_OK()

  def submitPilots(self):
    """ Go through defined computing elements and submit jobs if necessary
    """

    queues = self.queueDict.keys()

    # Check that there is some work at all
    setup = CSGlobals.getSetup()
    tqDict = {'Setup': setup,
              'CPUTime': 9999999,
              'SubmitPool': self.defaultSubmitPools}
    if self.vo:
      tqDict['Community'] = self.vo
    if self.voGroups:
      tqDict['OwnerGroup'] = self.voGroups

    if self.checkPlatform:
      result = self.resourcesModule.getCompatiblePlatforms(self.platforms)
      if not result['OK']:
        return result
      tqDict['Platform'] = result['Value']
    tqDict['Site'] = self.sites
    tags = []
    for queue in queues:
      tags += self.queueDict[queue]['ParametersDict']['Tag']
    tqDict['Tag'] = list(set(tags))

    self.log.verbose('Checking overall TQ availability with requirements')
    self.log.verbose(tqDict)

    matcherClient = MatcherClient()
    result = matcherClient.getMatchingTaskQueues(tqDict)
    if not result['OK']:
      return result
    if not result['Value']:
      self.log.verbose('No Waiting jobs suitable for the director')
      return S_OK()

    jobSites = set()
    anySite = False
    testSites = set()
    totalWaitingJobs = 0
    for tqID in result['Value']:
      if "Sites" in result['Value'][tqID]:
        for site in result['Value'][tqID]['Sites']:
          if site.lower() != 'any':
            jobSites.add(site)
          else:
            anySite = True
      else:
        anySite = True
      if "JobTypes" in result['Value'][tqID]:
        if "Sites" in result['Value'][tqID]:
          for site in result['Value'][tqID]['Sites']:
            if site.lower() != 'any':
              testSites.add(site)
      totalWaitingJobs += result['Value'][tqID]['Jobs']

    tqIDList = result['Value'].keys()
    self.log.info(tqIDList)
    result = pilotAgentsDB.countPilots({'TaskQueueID': tqIDList,
                                        'Status': WAITING_PILOT_STATUS},
                                       None)
    tagWaitingPilots = 0
    if result['OK']:
      tagWaitingPilots = result['Value']
    self.log.info('Total %d jobs in %d task queues with %d waiting pilots' %
                  (totalWaitingJobs, len(tqIDList), tagWaitingPilots))
    self.log.info('Queues: ', self.queueDict.keys())
    # if tagWaitingPilots >= totalWaitingJobs:
    #  self.log.info( 'No more pilots to be submitted in this cycle' )
    #  return S_OK()

    result = self.siteClient.getUsableSites()
    if not result['OK']:
      return result
    siteMaskList = result['Value']

    queues = self.queueDict.keys()
    random.shuffle(queues)
    totalSubmittedPilots = 0
    matchedQueues = 0
    for queue in queues:

      # Check if the queue failed previously
      failedCount = self.failedQueues[queue] % self.failedQueueCycleFactor
      if failedCount != 0:
        self.log.warn("%s queue failed recently, skipping %d cycles" % (queue, 10 - failedCount))
        self.failedQueues[queue] += 1
        continue

      ce = self.queueDict[queue]['CE']
      ceName = self.queueDict[queue]['CEName']
      ceType = self.queueDict[queue]['CEType']
      queueName = self.queueDict[queue]['QueueName']
      siteName = self.queueDict[queue]['Site']
      queueTags = self.queueDict[queue]['ParametersDict']['Tag']
      siteMask = siteName in siteMaskList
      processorTags = []

      # Check the status of the Site
      result = self.siteClient.getUsableSites(siteName)
      if not result['OK']:
        self.log.error("Can not get the status of site %s: %s" %
                       (siteName, result['Message']))
        continue
      if siteName not in result.get('Value', []):
        self.log.info("site %s is not active" % siteName)
        continue

      if self.rssFlag:
        # Check the status of the ComputingElement
        result = self.rssClient.getElementStatus(ceName, "ComputingElement")
        if not result['OK']:
          self.log.error("Can not get the status of computing element",
                         " %s: %s" % (siteName, result['Message']))
          continue
        if result['Value']:
          # get the value of the status
          result = result['Value'][ceName]['all']

        if result not in ('Active', 'Degraded'):
          self.log.verbose(
              "Skipping computing element %s at %s: resource not usable" % (ceName, siteName))
          continue

      for tag in queueTags:
        if re.match(r'^[0-9]+Processors$', tag):
          processorTags.append(tag)
      if 'WholeNode' in queueTags:
        processorTags.append('WholeNode')

      if not anySite and siteName not in jobSites:
        self.log.verbose("Skipping queue %s at %s: no workload expected" % (queueName, siteName))
        continue
      if not siteMask and siteName not in testSites:
        self.log.verbose("Skipping queue %s at site %s not in the mask" % (queueName, siteName))
        continue

      if 'CPUTime' in self.queueDict[queue]['ParametersDict']:
        queueCPUTime = int(self.queueDict[queue]['ParametersDict']['CPUTime'])
      else:
        self.log.warn('CPU time limit is not specified for queue %s, skipping...' % queue)
        continue
      if queueCPUTime > self.maxQueueLength:
        queueCPUTime = self.maxQueueLength

      # Prepare the queue description to look for eligible jobs
      ceDict = ce.getParameterDict()
      ceDict['GridCE'] = ceName
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

      if self.checkPlatform:
        platform = self.queueDict[queue]['Platform']
        result = self.resourcesModule.getCompatiblePlatforms(platform)
        if not result['OK']:
          continue
        ceDict['Platform'] = result['Value']

      ceDict['Tag'] = queueTags
      # Get the number of eligible jobs for the target site/queue
      result = matcherClient.getMatchingTaskQueues(ceDict)
      if not result['OK']:
        self.log.error('Could not retrieve TaskQueues from TaskQueueDB', result['Message'])
        return result
      taskQueueDict = result['Value']
      if not taskQueueDict:
        self.log.verbose('No matching TQs found for %s' % queue)
        continue

      matchedQueues += 1
      totalTQJobs = 0
      totalTQJobsByProcessors = {}
      tqIDList = taskQueueDict.keys()
      tqIDListByProcessors = {}
      for tq in taskQueueDict:
        if 'Tags' not in taskQueueDict[tq]:
          # skip non multiprocessor tqs
          continue
        for tag in taskQueueDict[tq]['Tags']:
          if tag in processorTags:
            tqIDListByProcessors.setdefault(tag, [])
            tqIDListByProcessors[tag].append(tq)

            totalTQJobsByProcessors.setdefault(tag, 0)
            totalTQJobsByProcessors[tag] += taskQueueDict[tq]['Jobs']

        totalTQJobs += taskQueueDict[tq]['Jobs']

      self.log.verbose('%d job(s) from %d task queue(s) are eligible for %s queue' % (totalTQJobs,
                                                                                      len(tqIDList), queue))

      queueSubmittedPilots = 0
      for tag in tqIDListByProcessors:

        self.log.verbose("Try to submit pilots for Tag=%s (TQs=%s)" % (tag, tqIDListByProcessors[tag]))

        processors = 1

        m = re.match(r'^(?P<processors>[0-9]+)Processors$', tag)
        if m:
          processors = int(m.group('processors'))
        if tag == 'WholeNode':
          processors = -1

        tagTQJobs = totalTQJobsByProcessors[tag]
        tagTqIDList = tqIDListByProcessors[tag]

        # Get the number of already waiting pilots for these task queues
        tagWaitingPilots = 0
        if self.pilotWaitingFlag:
          result = pilotAgentsDB.countPilots({'TaskQueueID': tagTqIDList,
                                              'Status': WAITING_PILOT_STATUS},
                                             None)
          if not result['OK']:
            self.log.error('Failed to get Number of Waiting pilots', result['Message'])
            tagWaitingPilots = 0
          else:
            tagWaitingPilots = result['Value']
            self.log.verbose('Waiting Pilots for TaskQueue %s:' % tagTqIDList, tagWaitingPilots)
        if tagWaitingPilots >= tagTQJobs:
          self.log.verbose("%d waiting pilots already for all the available jobs" % tagWaitingPilots)
          continue

        self.log.verbose("%d waiting pilots for the total of %d eligible jobs for %s" % (tagWaitingPilots,
                                                                                         tagTQJobs, queue))

        # Get the working proxy
        cpuTime = queueCPUTime + 86400
        self.log.verbose("Getting pilot proxy for %s/%s %d long" % (self.pilotDN, self.pilotGroup, cpuTime))
        result = gProxyManager.getPilotProxyFromDIRACGroup(self.pilotDN, self.pilotGroup, cpuTime)
        if not result['OK']:
          return result
        self.proxy = result['Value']
        ce.setProxy(self.proxy, cpuTime - 60)

        # Get the number of available slots on the target site/queue
        totalSlots = self.getQueueSlots(queue, False)
        if totalSlots == 0:
          self.log.debug('%s: No slots available' % queue)
          continue

        # Note: comparing slots to job numbers is not accurate in multiprocessor case.
        #       This could lead to over submission.
        pilotsToSubmit = max(0, min(totalSlots, tagTQJobs - tagWaitingPilots))
        self.log.info('%s: Slots=%d, TQ jobs=%d, Pilots: waiting %d, to submit=%d' %
                      (queue, totalSlots, tagTQJobs, tagWaitingPilots, pilotsToSubmit))

        # Limit the number of pilots to submit to MAX_PILOTS_TO_SUBMIT
        pilotsToSubmit = min(self.maxPilotsToSubmit - queueSubmittedPilots, pilotsToSubmit)

        while pilotsToSubmit > 0:
          self.log.info('Going to submit %d pilots to %s queue' % (pilotsToSubmit, queue))

          bundleProxy = self.queueDict[queue].get('BundleProxy', False)
          jobExecDir = ''
          jobExecDir = self.queueDict[queue]['ParametersDict'].get('JobExecDir', jobExecDir)

          executable, pilotSubmissionChunk = self.getExecutable(queue, pilotsToSubmit,
                                                                bundleProxy=bundleProxy,
                                                                jobExecDir=jobExecDir,
                                                                processors=processors)
          result = ce.submitJob(executable, '', pilotSubmissionChunk, processors=processors)
          # ## FIXME: The condor thing only transfers the file with some
          # ## delay, so when we unlink here the script is gone
          # ## FIXME 2: but at some time we need to clean up the pilot wrapper scripts...
          if ceType != 'HTCondorCE':
            os.unlink(executable)
          if not result['OK']:
            self.log.error('Failed submission to queue %s:\n' % queue, result['Message'])
            pilotsToSubmit = 0
            self.failedQueues[queue] += 1
            continue

          pilotsToSubmit = pilotsToSubmit - pilotSubmissionChunk
          queueSubmittedPilots += pilotSubmissionChunk
          # Add pilots to the PilotAgentsDB assign pilots to TaskQueue proportionally to the
          # task queue priorities
          pilotList = result['Value']
          self.queueSlots[queue]['AvailableSlots'] -= len(pilotList)
          totalSubmittedPilots += len(pilotList)
          self.log.info('Submitted %d pilots to %s@%s' % (len(pilotList), queueName, ceName))
          stampDict = {}
          if 'PilotStampDict' in result:
            stampDict = result['PilotStampDict']
          tqPriorityList = []
          sumPriority = 0.
          for tq in tagTqIDList:
            sumPriority += taskQueueDict[tq]['Priority']
            tqPriorityList.append((tq, sumPriority))
          rndm = random.random() * sumPriority
          tqDict = {}
          for pilotID in pilotList:
            rndm = random.random() * sumPriority
            for tq, prio in tqPriorityList:
              if rndm < prio:
                tqID = tq
                break
            if tqID not in tqDict:
              tqDict[tqID] = []
            tqDict[tqID].append(pilotID)

          for tqID, pilotList in tqDict.items():
            result = pilotAgentsDB.addPilotTQReference(pilotList,
                                                       tqID,
                                                       self.pilotDN,
                                                       self.pilotGroup,
                                                       self.localhost,
                                                       ceType,
                                                       stampDict)
            if not result['OK']:
              self.log.error('Failed add pilots to the PilotAgentsDB: ', result['Message'])
              continue
            for pilot in pilotList:
              result = pilotAgentsDB.setPilotStatus(pilot, 'Submitted', ceName,
                                                    'Successfully submitted by the SiteDirector',
                                                    siteName, queueName)
              if not result['OK']:
                self.log.error('Failed to set pilot status: ', result['Message'])
                continue

    self.log.info(
        "%d pilots submitted in total in this cycle, %d matched queues" %
        (totalSubmittedPilots, matchedQueues))
    return S_OK()
