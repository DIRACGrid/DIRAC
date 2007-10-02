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

  def __addMessage( self, messageObject ):
    """  This is the function that actually adds the Message to 
         the log Database
    """
    Credentials=self.getRemoteCredentials()
    if Credentials.has_key('DN'):
      UserDN=Credentials['DN']
    else:
      UserDN='unknown'
    Usergroup=Credentials['group']  
    Address=self.transport.getRemoteAddress()[0]
    LogDB.insertMsgIntoDB( messageObject, UserDN, Usergroup, Address )

        
  types_addMessages = []

  #A normal exported function (begins with export_)
  def export_addMessages( self, msgList ):
    """ This is the interface to the service
        inputs:
           msgList contains a  list of Message Objects.
    """
    for msgTuple in msgList:
      msgObject = tupleToMessage( msgTuple )
      try:
        self.__addMessage( msgObject )
      except Exception, v:
        print v
        return S_ERROR( "Error doing something: %s" % str( v ) )
    return S_OK()

