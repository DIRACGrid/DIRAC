"""
Abstract class for management of MQ connections
"""

__RCSID__ = "$Id$"

import time
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class MQConnector( object ):
  """
  Abstract class for management of message queue connections
  Allows to both send and receive messages from a queue
  """

  def __init__( self ):
    raise NotImplementedError( 'Cannot instantiate an abstract class' )

  # Callback functions for message receiving mode

  def on_error( self, headers, message ):
    """
    Callback function called when an error happens
    """
    gLogger.error( message )

  # Rest of functions

  def setQueueParameters( self, system, queueName ):
    """
    Reads the MessageQueue parameters from the CS and sets the appropriate variables in the class with their values
    system is the DIRAC system where the queue works
    queueName is the name of the queue
    """

    # Utility function to lowercase the first letter of a string ( to create valid variable names )
    toLowerFirst = lambda s: s[:1].lower() + s[1:] if s else ''

    self.queueName = queueName

    setup = gConfig.getValue( '/DIRAC/Setup', '' )

    # Get the parameters from the CS and set them
    for parameter in [ 'Host', 'Port', 'User', 'VH', 'Type' ]:
      result = gConfig.getOption( '/Systems/%s/%s/MessageQueueing/%s/%s' % ( system, setup, queueName, parameter ) )
      if not result[ 'OK' ]:
        return S_ERROR( 'Failed to get the parameter \'%s\' for the queue: %s' % ( parameter, result[ 'Message' ] ) )
      setattr( self, toLowerFirst( parameter ), result[ 'Value' ] )

    result = gConfig.getOption( '/LocalInstallation/MessageQueueing/Password' )
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to get the password for RabbitMQ: %s' % result[ 'Message' ] )
    self.password = result[ 'Value' ]

    return S_OK( 'Queue parameters set successfully' )

  def setupConnection( self, system, queueName, receive = False, messageCallback = None ):
    """
    Establishes a new non-blocking connection to the message queue
    system indicates in which System the queue works
    queueName is the name of the queue to read from/write to
    receive indicates whether this object will read from the queue or read from it
    exchange indicates whether the destination will be a exchange (True) or a queue (False). Only taken into account if receive = True
    messageCallback is the function to be called when a new message is received from the queue ( only receiver mode ). If None, the defaultCallback method is used instead
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def blockingConnection( self, system, queueName, receive = False, messageCallback = None ):
    """
    Establishes a new blocking connection to the message queue
    system indicates in which System the queue works
    queueName is the name of the queue to read from/write to
    receive indicates whether this object will read from the queue or read from it
    exchange indicates whether the destination will be a exchange (True) or a queue (False). Only taken into account if receive = True
    messageCallback is the function to be called when a new message is received from the queue ( only receiver mode ). If None, the defaultCallback method is used instead
    """
    result = self.setupConnection( system, queueName, receive, messageCallback )
    if not result[ 'OK' ]:
      return result

    while 1:
      time.sleep( 1 )

    self.unsetupConnection()

  def put( self, message ):
    """
    Sends a message to the queue
    message contains the body of the message
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def get( self ):
    """
    Retrieves a message from the queue ( if any )
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def unsetupConnection( self ):
    """
    Disconnects from the message queue server
    """
    raise NotImplementedError( 'This method should be implemented by child class' )
