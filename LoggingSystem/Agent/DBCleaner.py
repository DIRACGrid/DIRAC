"""  DB Cleaner erases records whose messageTime column contains a time
     older 'RemoveDate' days, where 'RemoveDate' is an entry in the 
     Configuration Service section of the agent.
"""

from DIRAC.Core.Base.Agent import Agent
from DIRAC  import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.LoggingSystem.DB.SystemLoggingDB import SystemLoggingDB
from DIRAC.Core.Utilities import dateTime, toString, day
from DIRAC.ConfigurationSystem.Client.Config import gConfig

AGENT_NAME = 'Logging/DBCleaner'

class DBCleaner(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    from DIRAC.ConfigurationSystem.Client.PathFinder import getAgentSection

    result = Agent.initialize(self)
    if not result['OK']:
      self.log.info('')
    
    self.SystemLoggingDB = SystemLoggingDB()
    
    self.section=getAgentSection( AGENT_NAME )
    
    self.period = toString( dateTime() -
                            int( gConfig.getValue( "%s/RemoveDate" %
                                                   self.section ) ) * day )
    
    return result

  def execute(self):
    """ The main agent execution method
    """
    limitDate = self.period[:self.period.find('.')]

    cmd = "SELECT messageTime FROM MessageRepository WHERE messageTime < '%s'" %limitDate
    result = self.SystemLoggingDB._query( cmd )
    if not result['OK']:
      self.log.error('SystemLogging','Could not query the SystemLoggingDB')
      return S_ERROR('Could not query the SystemLoggingDB')
    elif not len(result['Value']):
      return S_OK('No records to erase')
    else:
      cmd = "DELETE LOW_PRIORITY FROM MessageRepository WHERE messageTime < '%s'" % limitDate
      result =  self.SystemLoggingDB._update( cmd )
      if not result['OK']:
        self.log.error('LoggingSystem','Could not erase the required records')
        return S_ERROR('Could not erase the required records')
      else:
        return result
