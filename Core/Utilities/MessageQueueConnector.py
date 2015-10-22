"""
Abstract class for management of MQ connections
"""

__RCSID__ = "$Id$"

import time
from DIRAC import gLogger

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
