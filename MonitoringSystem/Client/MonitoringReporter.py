"""
This class is used to insert data to a db (currently elasticsearch).
It uses an internal list which is used to keep messages in the memory.
addRecord is used to insert messages to the internal queue. commit is used
to insert the acumulated messages to elasticsearch.
It provides two failover mechanism:
1.) If the database is not available, the data will be keept in the memory.
2.) If a MQ is available, we store the messages in MQ service.

Note: In order to not send too many rows to the db we use  __maxRecordsInABundle.

"""

import threading
import json

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.MessageQueue.MQCommunication import createConsumer
from DIRAC.Resources.MessageQueue.MQCommunication import createProducer
from DIRAC.MonitoringSystem.Client.ServerUtils import monitoringDB

__RCSID__ = "$Id$"

class MonitoringReporter( object ):

  """
  .. class:: MonitoringReporter

  This class is used to interact with the db using failover mechanism.

  :param int __maxRecordsInABundle: limit the number of records to be inserted to the db.
  :param threading.RLock __documentLock: is used to lock the local store when it is being modified.
  :param __documents: contains the recods which will be inserted to the db\
  :type __documents: python:list
  :param str __monitoringType: type of the records which will be inserted to the db. For example: WMSHistory.
  """

  def __init__( self, monitoringType = '' ):

    self.__maxRecordsInABundle = 5000
    self.__documentLock = threading.RLock()
    self.__documents = []
    self.__monitoringType = None

    self.__monitoringType = monitoringType

  def processRecords( self ):
    """
    It consumes all messaged from the MQ (these are failover messages). In case of failure, the messages
    will be inserted to the MQ again.
    """
    retVal = monitoringDB.pingDB()  # if the db is not accessible, the records will be not processed from MQ
    if retVal['OK']:
      if not retVal['Value']:  # false if we can not connect to the db
        return retVal
    else:
      return retVal

    result = createConsumer( "Monitoring::Queue::%s" % self.__monitoringType )
    if not result['OK']:
      gLogger.error( "Fail to create Consumer: %s" % result['Message'] )
      return S_ERROR( "Fail to create Consumer: %s" % result['Message'] )
    else:
      mqConsumer = result['Value']

    result = S_OK()
    failedToProcess = []
    while result['OK']:
      # we consume all messages from the consumer internal queue.
      result = mqConsumer.get()
      if result['OK']:
        records = json.loads( result['Value'] )
        retVal = monitoringDB.put( list( records ), self.__monitoringType )
        if not retVal['OK']:
          failedToProcess.append( records )

    mqConsumer.close()  # make sure that we will not process any more messages.
    # the db is not available and we publish again the data to MQ
    for records in failedToProcess:
      res = self.publishRecords( records )
      if not res['OK']:
        return res
    return S_OK()

  def addRecord( self, rec ):
    """
    It inserts the record to the list
    :param dict rec: it kontains a key/value pair.
    """
    self.__documents.append( rec )

  def publishRecords( self, records, mqProducer = None ):
    """
    send data to the MQ. If the mqProducer instance is provided, it will be used for publishing the data to MQ.
    :param list records: contains a list of key/value pairs (dictionaries)
    :param object mqProducer: We can provide the instance of a producer, which will be used to publish the data
    """

    if not mqProducer:
      mqProducer = self.__createProducer()
      result = mqProducer.put( json.dumps( records ) )
      mqProducer.close()
      return result
    return mqProducer.put( json.dumps( records ) )


  def commit( self ):
    """
    It inserts the accumulated data to the db. In case of failure
    it keeps in memory/MQ
    """
    # before we try to insert the data to the db, we process all the data
    # which are already in the queue
    mqProducer = self.__createProducer()  # we are sure that we can connect to MQ
    if mqProducer:
      result = self.processRecords()
      if not result['OK']:
        gLogger.error( "Unable to insert data to the db:", result['Message'] )

    self.__documentLock.acquire()
    documents = self.__documents
    self.__documents = []
    self.__documentLock.release()
    recordSent = 0
    try:
      while documents:
        recordsToSend = documents[ :self.__maxRecordsInABundle ]
        retVal = monitoringDB.put( recordsToSend, self.__monitoringType )
        if retVal[ 'OK' ]:
          recordSent += len( recordsToSend )
          del documents[ :self.__maxRecordsInABundle ]
          gLogger.info( "%d records inserted to the db" % ( recordSent ) )
        else:
          if mqProducer:
            res = self.publishRecords( recordsToSend, mqProducer )
            # if we managed to publish the records we can delete from the list
            if res['OK']:
              recordSent += len( recordsToSend )
              del documents[ :self.__maxRecordsInABundle ]
            else:
              return res  # in case of MQ problem
          else:
            gLogger.warn( "Failed to insert the records:", retVal['Message'] )
    except Exception as e:  # pylint: disable=broad-except
      gLogger.exception( "Error committing", lException = e )
      return S_ERROR( "Error committing %s" % repr( e ).replace( ',)', ')' ) )
    finally:
      if mqProducer:
        mqProducer.close()
      self.__documents.extend( documents )

    return S_OK( recordSent )

  def __createProducer( self ):
    """
    This method is used to create a producer
    """
    mqProducer = None
    result = createProducer( "Monitoring::Queue::%s" % self.__monitoringType )
    if not result['OK']:
      gLogger.warn( "Fail to create Producer:", result['Message'] )
    else:
      mqProducer = result['Value']

    return mqProducer
