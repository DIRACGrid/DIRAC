# $HeadURL$
"""  ErrorMessageMonitor gets new errors that have been injected into the
     SystemLoggingDB and reports them by mail to the person(s) in charge
     of checking that they conform with DIRAC style. Reviewer option
     contains the list of users to be notified.
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule                         import AgentModule
from DIRAC                                               import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry   import getUserOption
from DIRAC.FrameworkSystem.DB.SystemLoggingDB            import SystemLoggingDB
from DIRAC.FrameworkSystem.Client.NotificationClient     import NotificationClient
from DIRAC.Core.Utilities                                import List

AGENT_NAME = 'Logging/ErrorMessageMonitor'


class ErrorMessageMonitor( AgentModule ):

  def initialize( self ):

    self.systemLoggingDB = SystemLoggingDB()

    self.notification = NotificationClient()

    userString = self.am_getOption( "Reviewer", 'mseco' )

    self.log.debug( "Users to be notified", ": " + userString )

    userList = List.fromChar( userString, "," )

    mailList = []
    for user in userList:
      mail = getUserOption( user, 'Email', '' )
      if not mail:
        self.log.warn( "Could not get user's mail", user )
      else:
        mailList.append( mail )

    if not mailList:
      mailList = Operations().getValue( 'EMail/Logging', [] )

    if not len( mailList ):
      errString = "There are no valid users in the list"
      varString = "[" + ','.join( userList ) + "]"
      self.log.error( errString, varString )
      return S_ERROR( errString + varString )

    self.log.info( "List of mails to be notified", ','.join( mailList ) )

    self._mailAddress = mailList
    self._subject = 'New error messages were entered in the SystemLoggingDB'
    return S_OK()

  def execute( self ):
    """ The main agent execution method
    """
    cmd = "SELECT count(*) FROM FixedTextMessages WHERE ReviewedMessage=0"
    result = self.systemLoggingDB._query( cmd )
    if not result['OK']:
      return result
    recordsToReview = result['Value'][0][0]

    if recordsToReview == 0:
      self.log.info( 'No messages need review' )
      return S_OK( 'No messages need review' )
    else:
      conds = { 'ReviewedMessage': '0' }
      returnFields = [ 'FixedTextID', 'FixedTextString', 'SystemName',
                       'SubSystemName' ]
      result = self.systemLoggingDB._queryDB( showFieldList = returnFields,
                                              groupColumn = 'FixedTextString',
                                              condDict = conds )
      if not result['OK']:
        self.log.error( 'Failed to obtain the non reviewed Strings',
                       result['Message'] )
        return S_OK()
      messageList = result['Value']

      if messageList == 'None' or messageList == ():
        self.log.error( 'The DB query returned an empty result' )
        return S_OK()

      mailBody = 'These new messages have arrived to the Logging Service\n'
      for message in messageList:
        mailBody = mailBody + "String: '" + message[1] + "'\tSystem: '" \
                   + message[2] + "'\tSubsystem: '" + message[3] + "'\n"

      result = self.notification.sendMail( self._mailAddress, self._subject, mailBody )
      if not result[ 'OK' ]:
        self.log.warn( "The mail could not be sent" )
        return S_OK()

      for message in messageList:
        cmd = "UPDATE LOW_PRIORITY FixedTextMessages SET ReviewedMessage=1"
        cond = " WHERE FixedTextID=%s" % message[0]
        result = self.systemLoggingDB._update( cmd + cond )
        self.log.verbose( 'Message Status updated',
                         '(%d, %s)' % ( message[0], message[1] ) )
        if not result['OK']:
          self.log.error( 'Could not update status of Message', message[1] )
          return S_OK()

      self.log.info( "The messages have been sent for review",
                      "There are %s new descriptions" % recordsToReview )
      return S_OK( "%s Messages have been sent for review" % recordsToReview )
