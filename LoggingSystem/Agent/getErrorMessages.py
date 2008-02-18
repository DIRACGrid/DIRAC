# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/Agent/Attic/getErrorMessages.py,v 1.2 2008/02/18 16:28:01 mseco Exp $
__RCSID__ = "$Id: getErrorMessages.py,v 1.2 2008/02/18 16:28:01 mseco Exp $"
"""  getErrorNames get new errors that have been injected into the
     SystemLoggingDB and sends them by mail to the person(s) in charge
     of checking that they conform with DIRAC style. ReviewersMail option
     contains the list of mails where the information must be sent.
"""

from DIRAC.Core.Base.Agent import Agent
from DIRAC  import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.LoggingSystem.DB.SystemLoggingDB import SystemLoggingDB
from DIRAC.Core.Utilities import List, Mail

AGENT_NAME = 'Logging/getErrorNames'

class getErrorMessages(Agent):

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

    self.mail=Mail.Mail()
    mailString = gConfig.getValue( self.section+"/ReviewersMail",
                                   'secomig@usc.es' )
    mailList = List.fromChar( mailString, ",")

    self.mail._mailAddress = mailList
    self.mail._subject = 'New error messages were entered in the SystemLoggingDB'
    return S_OK()

  def execute(self):
    """ The main agent execution method
    """
    cmd = "SELECT count(*) FROM FixedTextMessages WHERE ReviewedMessage=0"
    result = self.SystemLoggingDB._query( cmd )
    if not result['OK']: 
      return result
    recordsToReview=result['Value'][0][0]

    if recordsToReview == 0:
      self.log.info('No messages need review')
      return S_OK('No messages need review')
    else:
      cmd = "SELECT FixedTextID,FixedTextString FROM FixedTextMessages WHERE ReviewedMessage=0"
      result =  self.SystemLoggingDB._query( cmd )
      if not result['OK']:
        self.log.error('Failed to obtain ')
        self.runFlag = False
        return S_OK()
      messageList = result['Value']

      mailBody ='This new messages have arrived into the Logging Service\n'
      for message in messageList:
        mailBody = mailBody+message[1]+"\n"

      self.mail._message = mailBody
      result = self.mail._send()
      if not result[ 'OK' ]:
         self.log.warn( "The mail could not be sent" )
         return S_OK()

      for message in messageList:
        cmd = "UPDATE LOW_PRIORITY FixedTextMessages SET ReviewedMessage=1 WHERE FixedTextID=%s" % message[0]
        result =  self.SystemLoggingDB._query( cmd )
        if not result['OK']:
          self.log.error( 'Could not update status of Message', message[1] )
          return S_OK()

      self.log.info( "The messages have been sent for review",
                      "There are %s new descriptions" % recordsToReview )
      return S_OK( "%s Messages have been sent for review" % recordsToReview )
