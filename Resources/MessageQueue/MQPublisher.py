""" MQPublisher class encapsulates publisher functionality fully configurable
    by the CS options
"""

from DIRAC.Resources.MessageQueue.MQConnectionFactory import MQConnectionFactory

__RCSID__ = "$Id$"

class MQPublisher(object):

  def __init__(self, messageQueue ):

    mqFactory = MQConnectionFactory()
    result = mqFactory.getMQPublisher( messageQueue )
    if not result['OK']:
      raise Exception( 'Failed to instantiate MQService connection: %s' % result['Message'] )
    self.mqConnection = result['Value']

  def put( self, message ):

    result = self.mqConnection.put( message )
    return result

  def stop( self ):

    result = self.mqConnection.disconnect()
    return result