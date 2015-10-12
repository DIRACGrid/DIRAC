import time
import sys
import stomp

class MyListener(stomp.ConnectionListener):
  def __init__(self):
    self.msgs= []
    self.last_msg=""
  def on_error(self, headers, message):
    print('received an error "%s"' % message)
  def on_message(self, headers, message):
    print('received a message "%s"' % message)
    self.last_msg = message
    self.msgs.append(message)

class TestStompConsumer(object):

  def __init__(self,host_and_ports= [('127.0.0.1',61613)], destination = '/queue/test', user='ala', password='ala'):

    self.conn = stomp.Connection(host_and_ports = host_and_ports)
    self.listener = MyListener()
    self.conn.set_listener('', self.listener)
    self.user = user
    self.password = password
    self.dest = destination

  def start(self,user = '', password = ''):
    self.conn.start()
    if not user:
      user = self.user
    if not password:
      password = self.password
    self.conn.connect(user, password, wait=True)
    self.conn.subscribe(destination=self.dest, id=1, ack='auto')


  def stopAndReturnAllMessages(self):
    self.conn.disconnect()
    return self.listener.msgs

def main():
  print "Starting main"
  consumer = TestStompConsumer()
  consumer.start()
  print consumer.stopAndReturnAllMessages()

if __name__ == '__main__':
  main()

