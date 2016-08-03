""" MQListener class encapsulates listener functionality fully configurable
    by the CS options

    The MQListener can be used in two modes:

    Simple mode with a default callback allows to get messages one by one by an explicit
    get() call:

      listener = MQListener( queueName )
      listener.get()

    Consumer mode with a custom callback function provided in the constructor or through
    setCallback call:

      listener = MQListener( queueName, callback = myCallback )
      listener run()
"""

import time
from DIRAC import S_ERROR
from DIRAC.Resources.MessageQueue.MQConnectionFactory import MQConnectionFactory
from DIRAC.Resources.MessageQueue.MQConnection import MQConnectionError


__RCSID__ = "$Id$"

class MQListener( object ):

  def __init__(self, messageQueue, callback = None ):

    self.callback = callback
    mqFactory = MQConnectionFactory()
    result = mqFactory.getMQListener( messageQueue )
    if not result['OK']:
      raise MQConnectionError( 'Failed to instantiate MQService connection: %s' % result['Message'] )
    self.mqConnection = result['Value']
    if callback:
      self.mqConnection.callback = callback

  def get( self ):
    """ Get one message, if any, from the MQ server

    :return: S_OK( message )/ S_ERROR if not message available or connection error
    """
    if self.callback:
      return S_ERROR( 'Can not do get(): the listener is configured with callback' )
    if self.mqConnection.connection is None:
      result = self.mqConnection.setupConnection( receive = True )
      if not result['OK']:
        return result
      # Give some time for the first check for messages on the server
      time.sleep(0.2)
    return self.mqConnection.get()

  def run( self ):
    """ Runs the listener with a configured callback forever. It can be stopped by explicit
        stop() call, for example in a control thread

    :return: S_OK/S_ERROR
    """
    result = self.mqConnection.run()
    return result

  def stop( self ):
    """ Stops consuming messages and disconnects from the MQ server

    :return: S_OK/S_ERROR
    """

    result = self.mqConnection.disconnect()
    return result

  def setCallback( self, callback ):
    """ Set the call back function in a consumer mode

    :param func callback: callback function
    :return: S_OK
    """
    self.mqConnection.callback = callback
    return S_OK()