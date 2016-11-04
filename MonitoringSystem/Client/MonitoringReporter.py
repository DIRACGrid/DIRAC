"""
This class is used to insert data to a db (currently elasticsearch). It uses an internal list which is used to keep messages in the memory.
addRecord is used to insert messages to the internal queue. commit is used to insert the acumulated messages to elasticsearch.
It provides two failover mechanism:
1.) If the database is not available, the data will be keept in the memory.
2.) If a MQ is avaulable, we store the messages in MQ service.

Note: In order to not send too many rows to the db we use  __maxRecordsInABundle.

"""

import threading
import json

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.MessageQueue.MQListener import MQListener
from DIRAC.Resources.MessageQueue.MQPublisher import MQPublisher
from DIRAC.Resources.MessageQueue.MQConnection import MQConnectionError

from DIRAC.MonitoringSystem.Client.ServerUtils import monitoringDB

__RCSID__ = "$Id$"

class MonitoringReporter( object ):

  """
  .. class:: MonitoringReporter

  This class is used to interact with the db using failover mechanism.

  :param: int __maxRecordsInABundle limit the number of records to be inserted to the db.
  :param: threading.RLock __documentLock is used to lock the local store when it is being modified.
  :param: list __documents contains the recods which will be inserted to the db
  :param: bool __mq we can use MQ if it is available... By default it is not allowed.
  :param: str __monitoringType type of the records which will be inserted to the db. For example: WMSHistory.
  :param: object __mqPublisher publisher used to publish the records to the MQ
  """

  def __init__( self, monitoringType = '' ):

    self.__maxRecordsInABundle = 5000
    self.__documentLock = threading.RLock()
    self.__documents = []
    self.__mq = False
    self.__monitoringType = None

    try:
      self.__mqPublisher = MQPublisher( monitoringType )
      self.__mq = True
    except MQConnectionError as exc:
      gLogger.warn( "Fail to create Publisher: %s" % exc )

    self.__monitoringType = monitoringType

  def processRecords( self ):
    """
    It consumes all messaged from the MQ (these are failover messages). In case of failure, the messages
    will be inserted to the MQ again.
    """
    try:
      mqListener = MQListener( self.__monitoringType )
    except MQConnectionError as exc:
      gLogger.error( "Fail to create Listener: %s" % exc )
      return S_ERROR( "Fail to create Listener: %s" % exc )

    result = S_OK()
    while result['OK']:
      # we consume all messages from the listener internal queue.
      result = mqListener.get()
      mqListener.stop()  # make sure that we will not proccess any more messages.
      if result['OK']:
        records = json.loads( result['Value'] )
        retVal = monitoringDB.put( list( records ), self.__monitoringType )
        if not retVal['OK']:
          # the db is not available and we publish again the data to MQ
          res = self.publishRecords( records )
          if not res['OK']:
            return res

    return S_OK()

  def addRecord( self, rec ):
    """
    It inserts the record to the list
    :param: dict rec it kontains a key/value pair.
    """
    self.__documents.append( rec )

  def publishRecords( self, records ):
    """
    send data to the MQ
    :param: list records contains a list of key/value pairs (dictionaries)
    """
    return self.__mqPublisher.put( json.dumps( records ) )

  def commit( self ):
    """
    It inserts the accumulated data to the db. In case of failure
    it keeps in memory/MQ
    """
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
          if self.__mq:
            res = self.publishRecords( recordsToSend )
            # if we managed to publish the records we can delete from the list
            if res['OK']:
              recordSent += len( recordsToSend )
              del documents[ :self.__maxRecordsInABundle ]
            else:
              return res  # in case of MQ problem
          else:
            gLogger.warn( "Failed to insert the records: %s", retVal['Message'] )
    except Exception as e:  # pylint: disable=broad-except
      gLogger.exception( "Error committing", lException = e )
      return S_ERROR( "Error committing %s" % repr( e ).replace( ',)', ')' ) )
    finally:
      self.__documents.extend( documents )

    if self.__mq:
      result = self.processRecords()
      if not result['OK']:
        gLogger.error( "Unable to insert data from the MQ", result['Message'] )

    return S_OK( recordSent )
