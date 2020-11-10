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

from six.moves import queue as Queue
from concurrent.futures import ThreadPoolExecutor

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
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
    self.siteClient = None
    self.clients = {}

  def initialize(self):
    """ Standard initialize.
    """

    res = ObjectLoader().loadObject('DIRAC.ResourceStatusSystem.Client.SiteStatus')
    if not res['OK']:
      self.log.error('Failed to load SiteStatus class: %s' % res['Message'])
      return res
    siteStatusClass = res['Value']

    res = ObjectLoader().loadObject('DIRAC.ResourceStatusSystem.Client.ResourceManagementClient')
    if not res['OK']:
      self.log.error('Failed to load ResourceManagementClient class: %s' % res['Message'])
      return res
    rmClass = res['Value']

    self.siteClient = siteStatusClass()
    self.clients['SiteStatus'] = siteStatusClass()
    self.clients['ResourceManagementClient'] = rmClass()

    maxNumberOfThreads = self.am_getOption('maxNumberOfThreads', 15)
    with ThreadPoolExecutor(max_workers=maxNumberOfThreads) as executor:
      executor.map(self._execute, list(range(maxNumberOfThreads)))

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

  def _execute(self):
    """
      Method run by each of the thread that is in the ThreadPool.
      It enters a loop until there are no sites on the queue.

      On each iteration, it evaluates the policies for such site
      and enforces the necessary actions.
    """

    pep = PEP(clients=self.clients)

    while True:
      site = self.sitesToBeChecked.get()
      self.log.verbose(
	  '%s ( VO=%s / status=%s / statusType=%s ) being processed' % (
	      site['name'],
	      site['vO'],
	      site['status'],
	      site['statusType']))

      try:
        resEnforce = pep.enforce(site)
      except Exception:
        self.log.exception('Exception during enforcement')
	resEnforce = S_ERROR('Exception during enforcement')
      if not resEnforce['OK']:
	self.log.error('Failed policy enforcement', resEnforce['Message'])
	continue

      resEnforce = resEnforce['Value']

      oldStatus = resEnforce['decisionParams']['status']
      statusType = resEnforce['decisionParams']['statusType']
      newStatus = resEnforce['policyCombinedResult']['Status']
      reason = resEnforce['policyCombinedResult']['Reason']

      if oldStatus != newStatus:
	self.log.info('%s (%s) is now %s ( %s ), before %s' % (site['name'],
							       statusType,
							       newStatus,
							       reason,
							       oldStatus))
