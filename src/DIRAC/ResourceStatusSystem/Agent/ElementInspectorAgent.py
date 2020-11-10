""" ElementInspectorAgent

  This agent inspect Resources (or maybe Nodes), and evaluates policies that apply.


The following options can be set for the ElementInspectorAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN ElementInspectorAgent
  :end-before: ##END
  :dedent: 2
  :caption: ElementInspectorAgent options
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

import datetime
from six.moves import queue as Queue
from concurrent.futures import ThreadPoolExecutor

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP

AGENT_NAME = 'ResourceStatus/ElementInspectorAgent'


class ElementInspectorAgent(AgentModule):
  """ ElementInspectorAgent

  The ElementInspector agent is a generic agent used to check the elements
  of type "Resource" -- which includes ComputingElement, StorageElement, and other types

  This Agent takes care of the Elements. In order to do so, it gathers
  the eligible ones and then evaluates their statuses with the PEP.
  """

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
    """ c'tor
    """

    AgentModule.__init__(self, *args, **kwargs)

    # ElementType, to be defined among Resource or Node
    self.elementType = 'Resource'
    self.elementsToBeChecked = None
    self.rsClient = None
    self.clients = {}

  def initialize(self):
    """ Standard initialize.
    """

    self.elementType = self.am_getOption('elementType', self.elementType)

    res = ObjectLoader().loadObject('DIRAC.ResourceStatusSystem.Client.ResourceStatusClient')
    if not res['OK']:
      self.log.error('Failed to load ResourceStatusClient class: %s' % res['Message'])
      return res
    rsClass = res['Value']

    res = ObjectLoader().loadObject('DIRAC.ResourceStatusSystem.Client.ResourceManagementClient')
    if not res['OK']:
      self.log.error('Failed to load ResourceManagementClient class: %s' % res['Message'])
      return res
    rmClass = res['Value']

    self.rsClient = rsClass()
    self.clients['ResourceStatusClient'] = rsClass()
    self.clients['ResourceManagementClient'] = rmClass()

    if not self.elementType:
      return S_ERROR('Missing elementType')

    maxNumberOfThreads = self.am_getOption('maxNumberOfThreads', 15)
    with ThreadPoolExecutor(max_workers=maxNumberOfThreads) as executor:
      executor.map(self._execute, list(range(maxNumberOfThreads)))

    return S_OK()

  def execute(self):
    """ execute

    This is the main method of the agent.
    It gets the elements from the Database which are eligible to be re-checked.
    """

    # Gets elements to be checked (returns a Queue)
    elementsToBeChecked = self.getElementsToBeChecked()
    if not elementsToBeChecked['OK']:
      self.log.error(elementsToBeChecked['Message'])
      return elementsToBeChecked
    self.elementsToBeChecked = elementsToBeChecked['Value']

    return S_OK()

  def getElementsToBeChecked(self):
    """ getElementsToBeChecked

    This method gets all the rows in the <self.elementType>Status table, and then
    discards entries with TokenOwner != rs_svc. On top of that, there are check
    frequencies that are applied: depending on the current status of the element,
    they will be checked more or less often.

    """

    toBeChecked = Queue.Queue()

    # We get all the elements, then we filter.
    elements = self.rsClient.selectStatusElement(self.elementType, 'Status')
    if not elements['OK']:
      return elements

    utcnow = datetime.datetime.utcnow().replace(microsecond=0)

    # filter elements by Type
    for element in elements['Value']:

      # Maybe an overkill, but this way I have NEVER again to worry about order
      # of elements returned by mySQL on tuples
      elemDict = dict(zip(elements['Columns'], element))

      # This if-clause skips all the elements that should not be checked yet
      timeToNextCheck = self.__checkingFreqs[elemDict['Status']]
      if utcnow <= elemDict['LastCheckTime'] + datetime.timedelta(minutes=timeToNextCheck):
        continue

      # We skip the elements with token different than "rs_svc"
      if elemDict['TokenOwner'] != 'rs_svc':
        self.log.verbose('Skipping %s ( %s ) with token %s' % (elemDict['Name'],
                                                               elemDict['StatusType'],
                                                               elemDict['TokenOwner']))
        continue

      # We are not checking if the item is already on the queue or not. It may
      # be there, but in any case, it is not a big problem.

      lowerElementDict = {'element': self.elementType}
      for key, value in elemDict.items():
        if len(key) >= 2:  # VO !
          lowerElementDict[key[0].lower() + key[1:]] = value

      # We add lowerElementDict to the queue
      toBeChecked.put(lowerElementDict)
      self.log.verbose('%s # "%s" # "%s" # %s # %s' % (elemDict['Name'],
                                                       elemDict['ElementType'],
                                                       elemDict['StatusType'],
                                                       elemDict['Status'],
                                                       elemDict['LastCheckTime']))
    return S_OK(toBeChecked)

  def _execute(self):
    """
      Method run by the ThreadPool. It enters a loop until there are no elements
      on the queue. On each iteration, it evaluates the policies for such element
      and enforces the necessary actions.
    """

    pep = PEP(clients=self.clients)

    while True:
      element = self.elementsToBeChecked.get()
      self.log.verbose(
	  '%s ( VO=%s / status=%s / statusType=%s ) being processed' % (
	      element['name'],
	      element['vO'],
	      element['status'],
	      element['statusType']))

      try:
        resEnforce = pep.enforce(element)
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
        self.log.info('%s (%s) is now %s ( %s ), before %s' % (element['name'],
                                                               statusType,
                                                               newStatus,
                                                               reason,
                                                               oldStatus))
