from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities import Time
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.LoggingSystem.private.Message import tupleToMessage
from DIRAC.LoggingSystem.DB.MsgLoggingDB import MsgLoggingDB

def initializeLoggingHandler( serviceInfo ):

  global LogDB
  LogDB = MsgLoggingDB()
  return S_OK()


class LoggingHandler( RequestHandler ):

  def __doSomething( self, messageObject ):
    Credentials=self.getRemoteCredentials()
    if Credentials.has_key('DN'):
      UserDN=Credentials['DN']
    else:
      UserDN='unknown'
    Usergroup=Credentials['group']  
    Address=self.transport.getRemoteAddress()[0]
    LogDB.insertMsgIntoDB( messageObject, UserDN, Usergroup, Address )

        
  types_doSomethingForEveryone = []

  #A normal exported function (begins with export_)
  def export_doSomethingForEveryone( self, msgList ):
    for msgTuple in msgList:
      msgObject = tupleToMessage( msgTuple )
      try:
        self.__doSomething( msgObject )
      except Exception, v:
        print v
        return S_ERROR( "Error doing something: %s" % str( v ) )
    return S_OK()

