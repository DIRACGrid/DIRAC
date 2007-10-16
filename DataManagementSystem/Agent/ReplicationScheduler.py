"""  Replication Scheduler assigns replication requests to channels
"""

from DIRAC.Core.Base.Agent import Agent
from DIRAC  import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection

AGENT_NAME = 'DataManagement/ReplicationScheduler'

class ReplicationScheduler(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):

    result = Agent.initialize(self)
    self.requestDB = RequestDB()
    return result

  def execute(self):
    """ The main agent execution method
    """
    result = self.jobDB.selectRequestWithStatus('waiting')
    if not result['OK']:
      errStr = 'Failed to get a request list from RequestDB'
      self.log.error(errStr)
      return S_ERROR(errStr)

    if not len(result['Value']):
      return S_OK()

    requestList = result['Value']
    for request in requestList:
      result = self.assignRequestToChannel(request)
    return result

  def insertJobInQueue(self,jobID):
    return S_OK()