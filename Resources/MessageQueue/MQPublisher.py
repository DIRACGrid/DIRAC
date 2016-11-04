""" MQPublisher class encapsulates publisher functionality fully configurable
    by the CS options
"""

from DIRAC.Resources.MessageQueue.MQConnectionFactory import MQConnectionFactory
from DIRAC.Resources.MessageQueue.MQConnection import MQConnectionError

__RCSID__ = "$Id$"

class MQPublisher(object):

  def __init__(self, messageQueue ):

    mqFactory = MQConnectionFactory()
    result = mqFactory.getMQPublisher( messageQueue )
    if not result['OK']:
      raise MQConnectionError( 'Failed to instantiate MQService connection: %s' % result['Message'] )
    self.mqConnection = result['Value']

  def put( self, message ):
    """ Send message to a MQ server

    :param message: any json encodable structure
    :return: S_OK/S_ERROR
    """
    result = self.mqConnection.put( message )
    return result

  def stop( self ):
    """ Dosconnect publisher from the MQ server

    :return: S_OK/S_ERROR
    """

    result = self.mqConnection.disconnect()
    return result