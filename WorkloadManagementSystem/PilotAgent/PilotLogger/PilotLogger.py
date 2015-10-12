""" Pilot logger module for the remote loggin system
"""

__RCSID__ = "$Id$"

import stomp
import sys
from DIRAC.WorkloadManagementSystem.PilotAgent.PilotLogger.PilotLoggerTools import generateDict, encodeMessage
from DIRAC.WorkloadManagementSystem.PilotAgent.PilotLogger.PilotLoggerTools import generateTimeStamp
from DIRAC.WorkloadManagementSystem.PilotAgent.PilotLogger.PilotLoggerTools import isMessageFormatCorrect

import Queue


def getPilotUUIDFromFile( filename = 'PilotAgentUUID' ):
  """ Retrives Pilot UUID from the file of given name.
  Returns:
    str: empty string in case of errors.
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

def eraseFileContent( filename ):
  """ Erases the content of a given file.
  """

  with open(filename, 'r+') as myFile:
    myFile.truncate()

def saveMessageToFile( msg, filename = 'myLocalQueueOfMessages' ):
  """ Adds the message to a file appended as a next line.
  """

  with open(filename, 'a+') as myFile:
    print >>myFile, msg

def readMessagesFromFileAndEraseFileContent( filename = 'myLocalQueueOfMessages' ):
  """ Generates the queue FIFO and fills it
      with values from the file, assuming that one line
      corresponds to one message.
      Finallym the file content is erased.
  Returns:
    Queue:
  """

  queue = Queue.Queue()
  with open( filename, 'r') as myFile:
    for line in myFile:
      queue.put(line)
  eraseFileContent( filename )
  return queue

class PilotLogger( object ):
  """ Base pilot logger class.
  """

  def __init__( self, fileWithID = 'PilotAgentUUID' ):
    """ ctr
    """
    self.FLAGS = ['info', 'warning', 'error', 'debug']
    self.STATUSES = [
        'Landed',
        'Installing',
        'Configuring',
        'Matching',
        'Running',
        'Done',
        'Failed'
        ]
    self.pilotID = getPilotUUIDFromFile( fileWithID )
    self.networkCfg = {
    'host':'127.0.0.1',
    'port':61614,
    'key_file':'/home/krzemien/workdir/lhcb/dirac_development/certificates/client/key.pem',
    'cert_file' : '/home/krzemien/workdir/lhcb/dirac_development/certificates/client/cert.pem',
    'ca_certs' : '/home/krzemien/workdir/lhcb/dirac_development/certificates/testca/cacert.pem'
    }


  def _isCorrectFlag( self, flag ):
    """ Checks if the flag corresponds to one of the predefined
        FLAGS, check constructor for current set.
    """

    if flag in self.FLAGS:
      return True
    return False

  def _isCorrectStatus( self, status ):

    """ Checks if the flag corresponds to one of the predefined
        STATUSES, check constructor for current set.
    """

    if status in self.STATUSES:
      return True
    return False

  def _connect(self):
    """ Connects to RabbitMQ and returns connection
        handler or None in case of connection down.
    """
    try:
      connection = stomp.Connection(host_and_ports=self.networkCfg['host_port'], use_ssl = True)
      connection.set_ssl(for_hosts=self.networkCfg['host_port'], key_file = self.networkCfg['key_file']
                        ,cert_file = self.networkCfg['cert_file'], ca_certs=self.networkCfg['ca_certs'])
      connection.start()
      connection.connect()
    except stomp.exception.ConnectFailedException:
      print 'Connection error:'
      return None
    else:
      return connection

  def _sendAllLocalMessages(self, connect_handler, flag = 'info' ):
    """ Retrives all messages from the local storage
        and sends it.
    """

    queue =readMessagesFromFileAndEraseFileContent()
    while not queue.empty():
      msg = queue.get()
      connect_handler.send(body=msg, destination='/queue/test')
      print " [x] Sent %r %r" % ( type, msg )


  def _sendMessage( self, msg, flag ):
    """ Method first copies the message content to the
        local storage, then it checks if the connection
        to RabbitMQ server is up,
        If it is the case it sends all messages stored
        locally.  The string flag can be used as routing_key,
        it can contain:  'info', 'warning', 'error',
        'debug'. If the connection is down, the method
        does nothing and returns False
    Returns:
      bool: False in case of any errors, True otherwise
    """

    if not self._isCorrectFlag( flag ):
      return False
    saveMessageToFile(msg)
    connection = self._connect()
    if not connection:
      return False
    self._sendAllLocalMessages(connection, flag)
    connection.disconnect()
    return True

  def sendMessage( self, minorStatus, flag = 'info', status='Installing' ):
    """ Sends the message after
        creating the correct format:
        including content, timestamp, status, minor status and the uuid
        of the pilot
    Returns:
      bool: False in case of any errors, True otherwise
    """
    if not self._isCorrectFlag( flag ):
      return False
    if not self._isCorrectStatus( status ):
      return False
    myUUID = getPilotUUIDFromFile()
    message = generateDict(
        myUUID,
        '',  # pilotID is unknown for a moment
        status,
        minorStatus,
        generateTimeStamp(),
        "pilot"
        )
    if not isMessageFormatCorrect( message ):
      return False
    encodedMsg = encodeMessage( message )
    return self._sendMessage( encodedMsg, flag )

def main():
  """ main() function  is used to send a message
      before any DIRAC related part is installed.
  """
  message = ' '.join( sys.argv[1:] ) or "Something wrong no message to send!"
  logger = PilotLogger()
  logger.sendMessage( message, 'info', 'Landed' )

if __name__ == '__main__':
  main()

