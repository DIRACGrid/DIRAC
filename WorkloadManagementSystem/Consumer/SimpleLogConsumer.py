from DIRAC.Core.Base.ConsumerModule import ConsumerModule
from DIRAC.Core.Utilities.RabbitMQ import RabbitInterface

import time
import sys

import stomp

class MyListener(stomp.ConnectionListener):
    def on_error(self, headers, message):
        print('received an error "%s"' % message)
    def on_message(self, headers, message):
        print('received a message "%s"' % message)




class SimpleLogConsumer( ConsumerModule):
  """
  """
  def __init__ ( self ):
    self.conn = stomp.Connection()
    self.conn.set_listener('', MyListener())
    self.conn.start()
    self.conn.connect('ala', 'ala', wait=True)

  def execute( self ):
    self.conn.subscribe(destination='/queue/test', id=1, ack='auto')
    time.sleep(50)
