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

#disable pylint warning: abstract class not referenced
#pylint: disable=R0921
class MQConnector( object ):
  """
  Class for management of message queue connections
  """

  def __init__( self, parameters = None ):
    """ Standard constructor
    """

  def setupConnection( self, parameters = None):
    """
    :param dict parameters: dictionary with additional MQ parameters if any
    :return: S_OK/S_ERROR
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def put(self, message, parameters = None):
    """ Send message to a MQ server

    :param message: any json encodable structure
    :return: S_OK/S_ERROR
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def connect(self, parameters = None):
    """
    :param dict parameters: dictionary with additional parameters if any
    :return: S_OK/S_ERROR
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def disconnect(self, parameters = None):
    """
    Disconnects from the message queue server
    :param dict parameters: dictionary with additional parameters if any
    :return: S_OK/S_ERROR
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def subscribe(self, parameters = None):
    """
    Subscribes to the message queue server
    :param dict parameters: dictionary with additional parameters if any
    :return: S_OK/S_ERROR
    """

    raise NotImplementedError( 'This method should be implemented by child class' )

  def unsubscribe(self, parameters = None):
    """
    Subscribes to the message queue server
    :param dict parameters: dictionary with additional parameters if any
    :return: S_OK/S_ERROR
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

