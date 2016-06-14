"""
Abstract class for management of MQ connections
"""

import time

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DErrno import EMQUKN

__RCSID__ = "$Id$"

class MQConnectionError( Exception ):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)

class MQConnection( object ):
  """
  Abstract class for management of message queue connections
  Allows to both send and receive messages from a queue
  """

  def __init__( self ):
    """ Standard constructor
    """

    self.alive = False

  def on_error( self, headers, message ):
    """ Default callback function called when an error happens
    """
    gLogger.error( message )

  # Rest of functions

  def setParameters( self, parameters ):
    """ Sets the parameters attribute

    :param dict parameters: dictionary with the parameters for the queue. It should include the following parameters:
    'Host', 'Port', 'User', 'VH' and 'Type'. Otherwise, the function will return an error
    """

    if not parameters:
      return S_ERROR( EMQUKN, 'Queue parameters are not provided' )

    for param in [ 'Host', 'Port', 'User', 'VHost' and 'Type' ]:
      if not param in parameters:
        return S_ERROR( EMQUKN, "Parameter %s not provided" % param )

    self.parameters = parameters

    return S_OK( 'Queue parameters set successfully' )

  def setupConnection( self, parameters = {}, receive = False, messageCallback = None ):
    """
    Establishes a new non-blocking connection to the message queue

    :param dict parameters: dictionary with additional MQ parameters if any
    :param bool receive: flag to enable the MQ connection for getting message
    :param func messageCallback: function to be called when a new message is received from the queue ( only receiver mode ).
                                If None, the defaultCallback method is used instead
    :return: S_OK/S_ERROR
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def run( self, parameters = {}, receive = True, messageCallback = None ):
    """
    Establishes a new blocking connection to the message queue
    :param dict parameters: dictionary with additional MQ parameters if any
    :param bool receive: flag to enable the MQ connection for getting message
    :param func messageCallback: function to be called when a new message is received from the queue
                                 ( only receiver mode ).
    :return: S_OK/S_ERROR
    """
    result = self.setupConnection( parameters, receive, messageCallback )
    if not result[ 'OK' ]:
      return result

    while self.alive:
      time.sleep( 1 )

    self.disconnect()

  def put( self, message ):
    """ Send message to a MQ server

    :param message: any json encodable structure
    :return: S_OK/S_ERROR
    """

    raise NotImplementedError( 'This method should be implemented by child class' )

  def get( self ):
    """ Get one message, if any, from the MQ server

    :return: S_OK( message )/ S_ERROR if not message available or connection error
    """

    raise NotImplementedError( 'This method should be implemented by child class' )

  def disconnect( self ):
    """
    Disconnects from the message queue server
    """
    raise NotImplementedError( 'This method should be implemented by child class' )
