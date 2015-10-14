""" Pilot logger module for the remote loggin system
    Only functions send and connect are depended on the client library used.
    The current implementation uses stomp.
"""

__RCSID__ = "$Id$"

import stomp
import sys
import Queue
#Since PilotLogger can be used without DIRAC relaying only on PilotLoggerTools, we
#check two possiblities:
try:
  from DIRAC.WorkloadManagementSystem.PilotAgent.PilotLogger.PilotLoggerTools import generateDict, encodeMessage
  from DIRAC.WorkloadManagementSystem.PilotAgent.PilotLogger.PilotLoggerTools import generateTimeStamp
  from DIRAC.WorkloadManagementSystem.PilotAgent.PilotLogger.PilotLoggerTools import isMessageFormatCorrect
except ImportError:
  from PilotLoggerTools import generateDict, encodeMessage
  from PilotLoggerTools import generateTimeStamp
  from PilotLoggerTools import isMessageFormatCorrect


def connect(host_and_port, ssl_cfg):
  """ Connects to RabbitMQ and returns connection
      handler or None in case of connection down.
      Stomp-depended function.
  """
  try:
    connection = stomp.Connection(host_and_ports=host_and_port, use_ssl = True)
    connection.set_ssl(for_hosts=host_and_port,
                       key_file = ssl_cfg['key_file'],
                       cert_file = ssl_cfg['cert_file'],
                       ca_certs = ssl_cfg['ca_certs'])
    connection.start()
    connection.connect()
  except stomp.exception.ConnectFailedException:
    print 'Connection error:'
    return None
  else:
    return connection

def send(msg ,destination, connect_handler):
  """Sends a message and prints info on the screen.
     Stomp-depended function.
  """
  if not connect_handler:
    return False
  connect_handler.send(destination=destination,
                       body=msg)
  print " [x] Sent %r %r" % ( type, msg )
  return True

def disconnect(connect_handler):
  """Disconnects.
     Stomp-depended function.
  """
  connect_handler.disconnect()


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

  def __init__( self, fileWithID = 'PilotAgentUUID', host_port=[('127.0.0.1',61614)] ):
    """ ctr
    Args:
    
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
    self.fileWithUUID = fileWithID
    self.networkCfg= host_port
    self.queuePath = '/queue/test'
    self.sslCfg = {
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




  def _sendAllLocalMessages(self, connect_handler, flag = 'info' ):
    """ Retrives all messages from the local storage
        and sends it.
    """
    queue =readMessagesFromFileAndEraseFileContent()
    while not queue.empty():
      msg = queue.get()

      send(msg, self.queuePath, connect_handler)


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
    connection = connect(self.networkCfg, self.sslCfg)
    if not connection:
      return False
    self._sendAllLocalMessages(connection, flag)
    disconnect(connection)
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
    myUUID = getPilotUUIDFromFile(self.fileWithUUID)
    message = generateDict(
        myUUID,
        '',  # pilotID is unknown for a moment
        status,
        minorStatus,
        generateTimeStamp(),
        "pilot"
        )
    print message
    if not isMessageFormatCorrect( message ):
      return False
    encodedMsg = encodeMessage( message )
    return self._sendMessage( encodedMsg, flag )

def main():
  """ main() function  is used to send a message
      before any DIRAC related part is installed.
      Remember that it is assumed that the PilotUUID was 
      already generated and stored into some file.
  """
  message = ' '.join( sys.argv[1:] ) or "Something wrong no message to send!"
  logger = PilotLogger()
  logger.sendMessage( message, 'info', 'Landed' )

if __name__ == '__main__':
  main()

