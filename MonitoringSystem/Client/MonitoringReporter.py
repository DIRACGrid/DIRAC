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
      #self.__mqListener = MQListener( monitoringType ) #, callback = self.consumeRecords )
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
  
  def processRecords( self ):
    print 'start'
    try:
      mqListener = MQListener( self.__monitoringType )
    except MQConnectionError as exc:
      gLogger.error( "Fail to create Listener: %s" % exc )
      return S_ERROR( "Fail to create Listener: %s" % exc )
    
    result = S_OK()
    while result['OK']:
      print 'while'
      result = mqListener.get()
      print '!!!!', result['OK']
      if result['OK']:
        records = json.loads( result['Value'] )
        retVal = self.__db.put( list( records ), self.__monitoringType )
        if not retVal['OK']:
          mqListener.stop()
          res = self.publishRecords( records )
          print res
          if not res['OK']:
            return res
          break
      else:
        print 'SS@@#######', result['Message']
        mqListener.stop()
        break
    print 'end'
    return S_OK()
    
  def addRecord( self, rec ):
    self.__documents.append( rec )
       
  def publishRecords( self, records ):
    print 'publishRecords'
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
            res = self.publishRecords( recordsToSend )
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
        
    if self.__mq:
      result = self.processRecords()
      if not result['OK']:
        gLogger.error( "Unable to insert data from the MQ", result['Message'] )
        
    
    '''
    result = self.__mqListener.get()
    print 'EEE',result
    if result['OK']:
      records = json.loads( result['Value'] )
      print 'records', records
    '''
  
    return S_OK( recordSent ) 
  
