"""
Consumer in charge of reading from RabbitMQ and sending the information to Elasticsearch
"""

import json

from DIRAC import gLogger, S_OK
from DIRAC.Resources.MessageQueue.MQListener import MQListener
from DIRAC.Resources.MessageQueue.MQConnection import MQConnectionError

class WMSMonitoringConsumer ( object ):
  """
  Consumer class for WMS monitoring
  """

  def __init__( self ):
    
    try:
      self.listener = MQListener( "TestQueue", callback = self.consumeMessages )
    except MQConnectionError as exc:
      gLogger.error( "Fail to create Publisher: %s" % exc )

  
  def consumeMessages( self, headers, message ):

    message = json.loads( message )
    messageID = headers['message-id']
    print 'self.message', message, messageID
    return S_OK()
    
  def start( self ):
    """
    Reads messages from RabbitMQ and sends them to ElasticSearch
    """
    self.listener.run()
    #result = S_OK()
    #while result['OK']:
    #  result = self.listener.get()
    #  print '!!!', result
       
consumer = WMSMonitoringConsumer()
consumer.start()
