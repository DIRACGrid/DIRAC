# $HeadURL$

""" Pilot logger module for the remote loggin system
"""

__RCSID__ = "$Id$"

import pika
import sys
from PilotLoggerTools import (
    generateDict,
    encodeMessage,
    generateTimeStamp,
    isMessageFormatCorrect
    )

def getPilotIdFromFile( filename = 'PilotAgentUUID' ):
  """
  Function retrives PilotUniqueId from the file of given name
  @return : string or empty string in case of errors
  """
  try:
    myFile = open( filename, 'r' )
    uniqueId = myFile.read()
  except IOError:
    print 'could not open file'
    return ""
  else:
    myFile.close()
    return uniqueId



class PilotLogger( object ):
  """ Base pilot logger class
  """

  def __init__( self, fileWithID = 'PilotAgentUUID' ):
    """ctr
    """
    self.FLAGS = ['info', 'warning', 'error', 'debug']
    self.pilotID = getPilotIdFromFile( fileWithID )
    self.networkCfg = {
    'host':'localhost',
    'exchangeName': 'myExchanger',
    'exchangeType': 'direct'}

  def _isCorrectFlag ( self, flag ):
    """
    Method check if the flag corresponds to one of the predefined
    FLAGS, check constructor for current set
    """

    if flag in self.FLAGS:
      return True
    return False

  def _sendMessage( self, msg, flag ):
    """Method sends the msg to the RabbitMQ exchange object
       the flag string is used as routing_key, it can be
       of type 'info', 'warning', 'error', 'debug'
       @return: False in case of any errors, True otherwise
    """
    if not self._isCorrectFlag( flag ):
      return False
    connection = pika.BlockingConnection( pika.ConnectionParameters(
                   self.networkCfg['host'] )
                 )
    channel = connection.channel()
    channel.exchange_declare(
                exchange = self.networkCfg['exchangeName'],
                type = self.networkCfg['exchangeType']
                )
    channel.basic_publish(
                        exchange = self.networkCfg['exchangeName'],
                        routing_key = flag,
                        body = msg
                        )
    print " [x] Sent %r %r" % ( type, msg )
    connection.close()
    return True

  def sendMessage( self, content, flag = "info" ):
    """
    Method creates the correct format of the message
    including content, timestamp, status and the id
    of the pilot
    @return : False in case of any errors, True otherwise
    """
    myId = getPilotIdFromFile()
    message = generateDict(
        myId,
        "installing",
        content,
        generateTimeStamp(),
        "pilot"
        )
    if not isMessageFormatCorrect( message ):
      return False
    encodedMsg = encodeMessage( message )
    return self._sendMessage( encodedMsg, flag )


def main():
  """ main() function  is used to send a message
  before any DIRAC related part is installed
  """
  message = ' '.join( sys.argv[1:] ) or "Something wrong not message to send!"
  logger = PilotLogger()
  logger.sendMessage( message )

if __name__ == '__main__':
  main()

