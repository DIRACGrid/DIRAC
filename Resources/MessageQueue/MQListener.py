""" MQListener class encapsulates listener functionality fully configurable
    by the CS options
"""

from DIRAC.Resources.MessageQueue.MQConnectionFactory import MQConnectionFactory

__RCSID__ = "$Id$"

class MQListener(object):

  def __init__(self, messageQueue ):

    mqFactory = MQConnectionFactory()
    if self.__getattribute__( 'callback' ):

      print "AT >>> getMQListener"

      result = mqFactory.getMQListener( messageQueue, messageCallback = self.callback )
    else:
      result = mqFactory.getMQPublisher( messageQueue )
    if not result['OK']:
      raise Exception( 'Failed to instantiate MQService connection: %s' % result['Message'] )
    self.mqConnection = result['Value']

  def get( self ):

    result = self.mqConnection.get()
    return result

  def stop( self ):

    result = self.mqConnection.disconnect()
    return result

  def callback( self, headers, message ):
    print "AT >>> headers, message", headers, message

  def setCallback( self, callback ):
    mqFactory = MQConnectionFactory()
    result = mqFactory.getMQListener( messageQueue, messageCallback = callback )
    self.mqConnection = result['Value']