"""
Consumer in charge of reading from RabbitMQ and sending the information to Elasticsearch
"""

import time
from DIRAC import gLogger, gConfig
from DIRAC.Core.Utilities.RabbitMQ import RabbitConnection
from DIRAC.FrameworkSystem.DB.DynamicMonitoringDB import DynamicMonitoringDB

class DynamicMonitoringConsumer ( object ):
  """
  Consumer class
  """

  def __init__( self ):
    self.rabbitMQ = RabbitConnection()

    self.docType = 'ComponentMonitoring'

    # ElasticSearch initialization
    self.elasticDB = DynamicMonitoringDB()

  def start( self ):
    """
    Reads messages from RabbitMQ and sends them to ElasticSearch
    """

    # Read RabbitMQ parameters
    result = gConfig.getOption( 'DIRAC/Setup' )
    if not result[ 'OK' ]:
      return result
    setup = result[ 'Value' ]
    result = gConfig.getOption( 'DIRAC/Setups/%s/%s' % ( setup, system ) )
    if not result[ 'OK' ]:
      return result
    sysSetup = result[ 'Value' ]
    result = gConfig.getOptionsDict( 'Systems/%s/%s/MessageQueueing/%s' % ( system, sysSetup, queueName ) )
    if not result[ 'OK' ]:
      return result
    parameters = result[ 'Value' ]

    result = self.rabbitMQ.setupConnection( 'Framework', 'ComponentMonitoring', parameters, True )
    if not result[ 'OK' ]:
      gLogger.error( result[ 'Message' ] )
      return result

    while True:
      time.sleep( 1 )
      # Get logs from the queue
      result = self.rabbitMQ.get()
      if result[ 'OK' ]:
        # insert the logs into ElasticSearch
        result = self.elasticDB.insertLog( result[ 'Value' ] )
        if not result[ 'OK' ]:
          gLogger.error( result[ 'Message' ] )
      else:
        gLogger.error( result[ 'Message' ] )

    result = self.rabbitMQ.unsetupConnection()
    if not result[ 'OK' ]:
      gLogger.error( result[ 'Message' ] )
      return result

consumer = DynamicMonitoringConsumer()
consumer.start()
