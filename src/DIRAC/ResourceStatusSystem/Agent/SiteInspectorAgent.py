""" SiteInspectorAgent

  This agent inspect Sites, and evaluates policies that apply.

The following options can be set for the SiteInspectorAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN SiteInspectorAgent
  :end-before: ##END
  :dedent: 2
  :caption: SiteInspectorAgent options
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

import math
from six.moves import queue as Queue

from DIRAC import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP

AGENT_NAME = 'ResourceStatus/SiteInspectorAgent'


class SiteInspectorAgent(AgentModule):
  """ SiteInspectorAgent

  The SiteInspectorAgent agent is an agent that is used to get the all the site names
  and trigger PEP to evaluate their status.

  """

  # Max number of worker threads by default
  __maxNumberOfThreads = 15

  # Inspection freqs, defaults, the lower, the higher priority to be checked.
  # Error state usually means there is a glitch somewhere, so it has the highest
  # priority.
  __checkingFreqs = {'Active': 20,
                     'Degraded': 20,
                     'Probing': 20,
                     'Banned': 15,
                     'Unknown': 10,
                     'Error': 5}

  def __init__(self, *args, **kwargs):

    AgentModule.__init__(self, *args, **kwargs)

    # ElementType, to be defined among Site, Resource or Node
    self.sitesToBeChecked = None
    self.threadPool = None
    self.siteClient = None
    self.clients = {}

  def initialize(self):
    """ Standard initialize.
    """

    maxNumberOfThreads = self.am_getOption('maxNumberOfThreads', self.__maxNumberOfThreads)
    self.threadPool = ThreadPool(maxNumberOfThreads, maxNumberOfThreads)

    res = ObjectLoader().loadObject('DIRAC.ResourceStatusSystem.Client.SiteStatus',
                                    'SiteStatus')
    if not res['OK']:
      self.log.error('Failed to load SiteStatus class: %s' % res['Message'])
      return res
    siteStatusClass = res['Value']

    res = ObjectLoader().loadObject('DIRAC.ResourceStatusSystem.Client.ResourceManagementClient',
                                    'ResourceManagementClient')
    if not res['OK']:
      self.log.error('Failed to load ResourceManagementClient class: %s' % res['Message'])
      return res
    rmClass = res['Value']

    self.siteClient = siteStatusClass()
    self.clients['SiteStatus'] = siteStatusClass()
    self.clients['ResourceManagementClient'] = rmClass()

    return S_OK()

  def execute(self):
    """ execute

    This is the main method of the agent. It gets the sites from the Database, calculates how many threads should be
    started and spawns them. Each thread will get a site from the queue until
    it is empty. At the end, the method will join the queue such that the agent
    will not terminate a cycle until all sites have been processed.

    """

    # Gets sites to be checked ( returns a Queue )
    sitesToBeChecked = self.getSitesToBeChecked()
    if not sitesToBeChecked['OK']:
      self.log.error("Failure getting sites to be checked", sitesToBeChecked['Message'])
      return sitesToBeChecked
    self.sitesToBeChecked = sitesToBeChecked['Value']

    queueSize = self.sitesToBeChecked.qsize()
    pollingTime = self.am_getPollingTime()

    # Assigns number of threads on the fly such that we exhaust the PollingTime
    # without having to spawn too many threads. We assume 10 seconds per element
    # to be processed ( actually, it takes something like 1 sec per element ):
    # numberOfThreads = elements * 10(s/element) / pollingTime
    numberOfThreads = int(math.ceil(queueSize * 10. / pollingTime))

    self.log.info('Needed %d threads to process %d elements' % (numberOfThreads, queueSize))

    for _x in range(numberOfThreads):
      jobUp = self.threadPool.generateJobAndQueueIt(self._execute)
      if not jobUp['OK']:
        self.log.error(jobUp['Message'])

    self.log.info('blocking until all sites have been processed')
    # block until all tasks are done
    self.sitesToBeChecked.join()
    self.log.info('done')

    return S_OK()

  def getSitesToBeChecked(self):
    """ getElementsToBeChecked

    This method gets all the site names from the SiteStatus table,
    after that it get the details of each
    site (status, name, etc..) and adds them to a queue.

    """

    toBeChecked = Queue.Queue()

    res = self.siteClient.getSites('All')
    if not res['OK']:
      return res

    # get the current status
    res = self.siteClient.getSiteStatuses(res['Value'])
    if not res['OK']:
      return res

    # filter elements
    for site in res['Value']:
      status = res['Value'].get(site, 'Unknown')

      toBeChecked.put({'status': status,
                       'name': site,
                       'site': site,
                       'element': 'Site',
                       'statusType': 'all',
                       'elementType': 'Site'})

    return S_OK(toBeChecked)

  # Private methods ............................................................

  def _execute(self):
    """
      Method run by each of the thread that is in the ThreadPool.
      It enters a loop until there are no sites on the queue.

      On each iteration, it evaluates the policies for such site
      and enforces the necessary actions. If there are no more sites in the
      queue, the loop is finished.
    """

    pep = PEP(clients=self.clients)

    while True:

      try:
        site = self.sitesToBeChecked.get_nowait()
      except Queue.Empty:
        return S_OK()

      try:
        resEnforce = pep.enforce(site)
        if not resEnforce['OK']:
          self.log.error('Failed policy enforcement', resEnforce['Message'])
      except Exception as e:
        self.log.exception('Exception during enforcement')
      # Used together with join !
      self.sitesToBeChecked.task_done()
