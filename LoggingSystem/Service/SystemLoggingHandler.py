# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/Service/SystemLoggingHandler.py,v 1.6 2008/03/05 11:26:32 mseco Exp $
__RCSID__ = "$Id: SystemLoggingHandler.py,v 1.6 2008/03/05 11:26:32 mseco Exp $"
"""
SystemLoggingHandler is the implementation of the Logging service
    in the DISET framework

    The following methods are available in the Service interface

    addMessages()

"""
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities import Time
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.LoggingSystem.private.Message import tupleToMessage
from DIRAC.LoggingSystem.DB.SystemLoggingDB import SystemLoggingDB

def initializeSystemLoggingHandler( serviceInfo ):

  global LogDB
  LogDB = SystemLoggingDB()
  return S_OK()


class SystemLoggingHandler( RequestHandler ):

  def __addMessage( self, messageObject, site, nodeFQDN ):
    """  This is the function that actually adds the Message to 
         the log Database
    """
    Credentials=self.getRemoteCredentials()
    if Credentials.has_key('DN'):
      userDN=Credentials['DN']
    else:
      userDN='unknown'
    userGroup = Credentials['group']
    remoteAddress = self.getRemoteAddress()[0]
    return LogDB._insertMessageIntoSystemLoggingDB( messageObject, site,
                                                    nodeFQDN, userDN,
                                                    userGroup, remoteAddress )

        
  types_addMessages = []

  #A normal exported function (begins with export_)
  def export_addMessages( self, messagesList, site, nodeFQDN ):
    """ This is the interface to the service
        inputs:
           msgList contains a  list of Message Objects.
        outputs:
           S_OK if no exception was raised
           S_ERROR if an exception was raised
    """
    for messageTuple in messagesList:
      messageObject = tupleToMessage( messageTuple )
      try:
        self.__addMessage( messageObject, site, nodeFQDN )
      except Exception, v:
        errorString = 'Message was not added because of exception: '
        exceptionString = str(v)
        gLogger.exception( errorString ,exceptionString )
        return S_ERROR( "%s %s" % ( errorString, exceptionString ) )
    return S_OK()

