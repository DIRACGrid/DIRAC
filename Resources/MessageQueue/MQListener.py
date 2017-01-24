""" MQListener class encapsulates listener functionality fully configurable
    by the CS options

    The MQListener can be used in two modes:

    Simple mode with a default callback allows to get messages one by one by an explicit
    get() call::

      listener = MQListener( queueName )
      listener.get()

    Consumer mode with a custom callback function provided in the constructor or through
    setCallback call::

      listener = MQListener( queueName, callback = myCallback )
      listener.run()

"""

from DIRAC import S_OK
from DIRAC.Resources.MessageQueue.MQConnectionFactory import MQConnectionFactory
from DIRAC.Resources.MessageQueue.MQConnection import MQConnectionError


__RCSID__ = "$Id$"

class MQListener( object ):

  def __init__(self, messageQueue, callback = None ):

    mqFactory = MQConnectionFactory()
    result = mqFactory.getMQListener( messageQueue )
    if not result['OK']:
      raise MQConnectionError( 'Failed to instantiate MQService connection: %s' % result['Message'] )
    self.mqConnection = result['Value']

    if callback:
      self.setCallback( callback )

  def get( self ):
    """ Get one message, if any, from the MQ server

    :return: S_OK( message )/ S_ERROR if not message available or connection error
    """

    return self.mqConnection.get()

  def run( self ):
    """ Runs the listener with a configured callback forever. It can be stopped by explicit
        stop() call, for example in a control thread

    :return: S_OK/S_ERROR
    """

    return self.mqConnection.run()

  def stop( self ):
    """ Stops consuming messages and disconnects from the MQ server

    :return: S_OK/S_ERROR
    """

    return self.mqConnection.disconnect()

  def setCallback( self, callback ):
    """ Set the call back function in a consumer mode

    :param func callback: callback function
    :return: S_OK
    """

    self.mqConnection.callback = callback
    return S_OK()
