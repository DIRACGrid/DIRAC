import threading
import json

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.MessageQueue.MQListener import MQListener
from DIRAC.Resources.MessageQueue.MQPublisher import MQPublisher
from DIRAC.Resources.MessageQueue.MQConnection import MQConnectionError



class MonitoringReporter( object ):
  
  def __init__( self, db, monitoringType = '' ):
    self.__maxRecordsInABundle = 5000
    self.__documentLock = threading.RLock()
    self.__db = db
    self.__documents = []
    self.__mq = False
    self.__monitoringType = None
    self.__commitTimer = None
    
    try:
      self.__mqListener = MQListener( monitoringType ) #, callback = self.consumeRecords )
      #self.__mqListener.setCallback(self.consumeRecords)
      self.__mqPublisher = MQPublisher( monitoringType )
      self.__mq = True
    except MQConnectionError as exc:
      gLogger.error( "Fail to create Publisher: %s" % exc )
             
    self.__monitoringType = monitoringType
  
  def consumeRecords(self, headers, message):
    records = json.loads(message) 
    print '####', type(records)
    #records = json.loads( message )   
    print '!!!!', records[0]
    return S_ERROR()
  
  def addRecord( self, rec ):
    self.__documents.append( rec )
       
  def publishRecords( self, records ):
    return self.__mqPublisher.put( json.dumps(records) )
      
  def commit( self ):
    
    self.__documentLock.acquire()
    documents = self.__documents
    self.__documents = []
    self.__documentLock.release()
    recordSent = 0
    try:
      while documents:
        recordsToSend = documents[ :self.__maxRecordsInABundle ]
        retVal = self.__db.put( recordsToSend, self.__monitoringType )
        if retVal[ 'OK' ]:
          recordSent += len( recordsToSend )
          del documents[ :self.__maxRecordsInABundle ]          
        else:
          if self.__mq:
            res = self.publishRecords( recordsToSend[:2] )
            documents = []
            #if we managed to publish the records we can delete from the list
            if res['OK']:
              recordSent += len( recordsToSend )
              del documents[ :self.__maxRecordsInABundle ]
            else:
              return res #in case of MQ problem
    except Exception as e:  # pylint: disable=broad-except
      gLogger.exception( "Error committing", lException = e )
      return S_ERROR( "Error committing %s" % repr( e ).replace( ',)', ')' ) )    
    finally:
      self.__documents.extend( documents )
    
    print 'start'
    
    result = S_OK()
    self.__mqListener.start()
    while result['OK']:
      result = self.__mqListener.get()
      print '!!!!', result['OK']
      if result['OK']:
        records = json.loads( result['Value'] )
        retVal = self.__db.put( list(records), self.__monitoringType )
        if not retVal['OK']:
          self.__mqListener.stop()
          res = self.publishRecords( records )
          if not res['OK']:
            return res
          break
      else:
        print 'SS@@#######', result['Message']
        self.__mqListener.stop()
        break
    
    '''
    result = self.__mqListener.get()
    print 'EEE',result
    if result['OK']:
      records = json.loads( result['Value'] )
      print 'records', records
    '''
    print 'END'
    return S_OK( recordSent ) 
  
