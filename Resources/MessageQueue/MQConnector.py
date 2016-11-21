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

  MANDATORY_PARAMETERS = [ 'Host', 'Port', 'User', 'VHost', 'MQType' ]


  def __init__( self, parameters = {} ):
    """ Standard constructor
    """

    self.alive = False
    self.parameters = parameters

    self.callback = None  # set the default callback function
    self.msgList = []  # derived classes should add new messages to this list

    self.log = gLogger.getSubLogger( 'MQConnector' )


  # Rest of functions
  def setParameters( self, parameters ):
    """ Sets the parameters attribute

    :param dict parameters: dictionary with the parameters for the queue. It should include the following parameters:
    'Host', 'Port', 'User', 'VH' and 'MQType'. Otherwise, the function will return an error
    """

    if not parameters:
      return S_ERROR( EMQUKN, 'Queue parameters are not provided' )

    missingParameters = set( self.MANDATORY_PARAMETERS ) - set( parameters )
    if missingParameters:
      return S_ERROR( EMQUKN, "Parameter(s) %s not provided" % ','.join( missingParameters ) )

    self.parameters = parameters

    return S_OK( 'Queue parameters set successfully' )

  def defaultCallback( self, headers, message ):
    """
    Default callback function called every time something is read from the queue.

    :param dict headers: message headers
    :param dict message: message body
    """

    self.msgList.append( message )
    return S_OK()

  def setupConnection( self, parameters = None, messageCallback = defaultCallback ):
    """
    Establishes a new non-blocking connection to the message queue

    :param dict parameters: dictionary with additional MQ parameters if any
    :param func messageCallback: function to be called when a new message is received from the queue ( only receiver mode ).
                                If not set, the defaultCallback method is used instead
    :return: S_OK/S_ERROR
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

  def run( self, parameters = None, messageCallback = defaultCallback ):
    """
    Establishes a new blocking connection to the message queue

    :param dict parameters: dictionary with additional MQ parameters if any
    :param func messageCallback: function to be called when a new message is received from the queue ( only receiver mode ).
                                 If not set, the defaultCallback method is used instead
    :return: S_OK/S_ERROR
    """
    result = self.setupConnection( parameters, messageCallback )
    if not result[ 'OK' ]:
      return result

    while self.alive:
      time.sleep( 1 )

    return self.disconnect()

  def put( self, message, destination = None ):
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

    #if self.callback is None:
      #return S_ERROR( EMQUKN, 'Cannot get(): MQConnection is configured in publisher mode' )
    #elif self.callback is not self.defaultCallback:
      #return S_ERROR( EMQUKN, 'Cannot get(): MQConnection is configured with a custom callback function' )

    ## check the connection
    #if self.connection is None:
      #result = self.setupConnection( messageCallback = self.defaultCallback )
      #if not result['OK']:
        #return result

    #try:
      #msg = self.msgList.pop( 0 )
    #except IndexError:
      #return S_ERROR( EMQNOM, 'No messages in queue' )

    #return S_OK( msg )

  def disconnect( self ):
    """
    Disconnects from the message queue server
    """
    raise NotImplementedError( 'This method should be implemented by child class' )

