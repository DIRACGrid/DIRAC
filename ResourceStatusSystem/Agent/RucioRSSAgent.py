""" :mod: RucioRSSAgent

    Agent that synchronizes Rucio and Dirac
"""

# # imports
from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

from rucio.client import Client

__RCSID__ = "Id$"


class RucioRSSAgent(AgentModule):
  """
  .. class:: RucioRSSAgent

  Agent that synchronizes Rucio and Dirac
  """

  def initialize(self):
    """ agent's initalisation

    :param self: self reference
    """
    self.log = gLogger.getSubLogger('RucioSynchronizer')
    self.log.info("Starting RucioRSSAgent")
    return S_OK()

  def execute(self):
    """ execution in one agent's cycle

    :param self: self reference
    """
    rSS = ResourceStatus()
    client = Client()
    try:
      for rse in client.list_rses():
        thisSe = rse['rse']
        self.log.info("Running on %s", thisSe)
        resStatus = rSS.getElementStatus(thisSe, "StorageElement")
        dictSe = client.get_rse(thisSe)
        if resStatus['OK']:
          seAccessValue = resStatus['Value'][thisSe]
          availabilityRead = True if seAccessValue['ReadAccess'] in ['Active', 'Degraded'] else False
          availabilityWrite = True if seAccessValue['WriteAccess'] in ['Active', 'Degraded'] else False
          availabilityDelete = True if seAccessValue['RemoveAccess'] in ['Active', 'Degraded'] else False
          isUpdated = False
          if dictSe['availability_read'] != availabilityRead:
            self.log.info('Set availability_read for %s to %s', thisSe, availabilityRead)
            client.update_rse(thisSe, {'availability_read': availabilityRead})
            isUpdated = True
          if dictSe['availability_write'] != availabilityWrite:
            self.log.info('Set availability_write for %s to %s', thisSe, availabilityWrite)
            client.update_rse(thisSe, {'availability_write': availabilityWrite})
            isUpdated = True
          if dictSe['availability_delete'] != availabilityDelete:
            self.log.info('Set availability_delete for %s to %s', thisSe, availabilityDelete)
            client.update_rse(thisSe, {'availability_delete': availabilityDelete})
            isUpdated = True
    except Exception as err:
      return S_ERROR(str(err))
    return S_OK()
