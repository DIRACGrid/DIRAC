"""
Class for management of MQ communication 
"""

import time

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DErrno import EMQUKN, EMQNOM

from DIRAC.Resources.MessageQueue.MQProducer import MQProducer

__RCSID__ = "$Id$"

class MQConnectionError( Exception ):
  pass

class MQConnector( object ):
  """
  Class for management of message queue connections
  Allows to both send and receive messages from a queue
  """

  def __init__( self, parameters = {} ):
    """ Standard constructor
    """

  def setupConnection( self, parameters = None):
    """
    Establishes a new non-blocking connection to the message queue

    :param dict parameters: dictionary with additional MQ parameters if any
    :param func messageCallback: function to be called when a new message is received from the queue ( only receiver mode ).
                                If not set, the defaultCallback method is used instead
    :return: S_OK/S_ERROR
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def put( self, message, destination = None):
    """ Send message to a MQ server

    :param message: any json encodable structure
    :return: S_OK/S_ERROR
    """

    raise NotImplementedError( 'This method should be implemented by child class' )

  def get( self , destination = None):
    """
    Retrieves a message, if any, from the internal queue. This method is only valid
    if the default behaviour for the message callback is being used.

    :return: S_OK( message )/S_ERROR if there are no messages in the queue
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def connect( self, parameters = None ):

    raise NotImplementedError( 'This method should be implemented by child class' )

  def disconnect( self ):
    """
    Disconnects from the message queue server
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def subscribe( self, parameters = None ):

    raise NotImplementedError( 'This method should be implemented by child class' )

  def unsubscribe( self, parameters = None ):

    raise NotImplementedError( 'This method should be implemented by child class' )

