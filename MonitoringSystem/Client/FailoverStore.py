import threading
import json

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.MessageQueue.MQListener import MQListener
from DIRAC.Resources.MessageQueue.MQPublisher import MQPublisher
from DIRAC.Resources.MessageQueue.MQConnection import MQConnectionError

class FailoverStore( object ):
  
  def __init__( self, db, monitoringType = '', autoCommit = False ):
    self.__maxRecordsInABundle = 5000
    self.__documentLock = threading.RLock()
    self.__db = db
    self.__documents = []
    self.__mq = False
    self.__monitoringType = None
    self.__commitTimer = None
    
    if autoCommit:
      self.__commitTimer = threading.Timer( 5, self.commit )
    try:
      self.__mqListener = MQListener( monitoringType, callback = self.consumeRecords )
      self.__mqPublisher = MQPublisher( "TestQueue" )
      self.__mq = True
    except MQConnectionError as exc:
      gLogger.error( "Fail to create Publisher: %s" % exc )
       
    if self.__mq:
      self.__mqListener.run()
      # if we use a MQ we have to consume the messages and the consummed messages
      # have to be inserted...
      self.__commitTimer = threading.Timer( 5, self.commit )
      
    self.__monitoringType = monitoringType
  
  def addRecord( self, rec ):
    self.__documents.append( rec )
  
  def consumeRecords( self, headers, message ):
    record = json.loads( message )
    self.addRecord( record )
     
  def publishRecords( self, records ):
    for record in records:
      self.__mqPublisher.put( record )
      
  def commit( self ):
    
    self.__documentLock.acquire()
    documents = self.__documents
    self.__documents = []
    self.__documentLock.release()
    recordSent = 0
    try:
      while documents:
        recordsToSend = documents[ :self.__maxRecordsInABundle ]
        retVal = self.__db.put( recordsToSend )
        if retVal[ 'OK' ]:
          recordSent += len( recordsToSend )
          del documents[ :self.__maxRecordsInABundle ]          
        else:
          gLogger.error( 'Error sending monitoring record. Data will be sent again', retVal['Message'] )
          if self.__mq:
            self.publishRecords( recordsToSend )
    except Exception as e:  # pylint: disable=broad-except
      gLogger.exception( "Error committing", lException = e )
      return S_ERROR( "Error committing %s" % repr( e ).replace( ',)', ')' ) )    
    finally:
      self.__documents.extend( documents )

    return S_OK( recordSent ) 
