"""  SystemLoggingDBCleaner erases records whose messageTime column 
     contains a time older than 'RemoveDate' days, where 'RemoveDate' 
     is an entry in the Configuration Service section of the agent.
"""

from DIRAC.Core.Base.Agent import Agent
from DIRAC  import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.LoggingSystem.DB.SystemLoggingDB import SystemLoggingDB
from DIRAC.Core.Utilities import dateTime, toString, day

AGENT_NAME = 'Logging/SystemLoggingDBCleaner'

class SystemLoggingDBCleaner(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    from DIRAC.ConfigurationSystem.Client.PathFinder import getAgentSection

    result = Agent.initialize(self)
    if not result['OK']:
      self.log.error('Agent could not initialize')
      return result
    
    self.SystemLoggingDB = SystemLoggingDB()
    
    self.section=getAgentSection( AGENT_NAME )
    
    self.period = int( gConfig.getValue( "%s/RemoveDate" %
                                         self.section ) ) * day
    
    return result

  def execute(self):
    """ The main agent execution method
    """
    limitDate = toString( dateTime() - self.period )
    limitDate = limitDate[:limitDate.find('.')]

    cmd = "SELECT count(*) FROM MessageRepository WHERE messageTime < '%s'" %limitDate
    result = self.SystemLoggingDB._query( cmd )
    if not result['OK']: 
      return result
    recordsToErase=result['Value'][0][0]

    if recordsToErase == 0:
      self.log.info('No records to erase')
      return S_OK('No records to erase')
    else:
      cmd = "DELETE LOW_PRIORITY FROM MessageRepository WHERE messageTime < '%s'" % limitDate
      result =  self.SystemLoggingDB._update( cmd )
      if not result['OK']:
        self.log.error('Could not erase the requested records','those older than %s' % limitDate)
        return result
      else:
        self.log.info('%s records have been erased' % recordsToErase )
        return result
