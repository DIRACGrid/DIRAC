"""A set of tools for the remote pilot agent logging system
"""

__RCSID__ = "$Id$"

import time
import json
from uuid import uuid1
import sys
import os

def createPilotLoggerConfigFile( filename = 'PilotLogger.cfg',
                                 host = '',
                                 port = '',
                                 queuePath = '',
                                 key_file  = '',
                                 cert_file = '',
                                 ca_certs = '',
                                 fileWithID = ''):
  """Helper function that creates proper configuration file.
     The format is json encoded file with the following options included
  """
  keys = [
      'host',
      'port',
      'queuePath',
      'key_file',
      'cert_file',
      'ca_certs',
      'fileWithID'
      ]
  values = [
      host,
      port,
      queuePath,
      key_file,
      cert_file,
      ca_certs,
      fileWithID
      ]
  config = dict( zip( keys, values ) )
  config = json.dumps(config)
  with open(filename, 'w') as myFile:
    myFile.write(config)

def readPilotLoggerConfigFile ( filename ):
  """Helper function that loads configuration file.
  Returns:
    dict:
  """
  try:
    with open(filename, 'r') as myFile:
      config = myFile.read()
      config = json.loads(config)
      return config
  except (IOError, ValueError):
    return None

def generateDict( pilotUUID, status, minorStatus, timestamp, source ):
  """Helper function that returs a dictionnary based on the
     set of input values.
  Returns
    dict:
  """

  keys = [
      'pilotUUID',
      'status',
      'minorStatus',
      'timestamp',
      'source'
      ]
  values = [
      pilotUUID,
      status,
      minorStatus,
      timestamp,
      source
      ]
  return dict( zip( keys, values ) )

def encodeMessage( content ):
  """Method encodes the message in form of the serialized JSON string
     see https://docs.python.org/2/library/json.html#py-to-json-table
  Args:
    content(dict):
  Returns:
    str: in the JSON format.
  Raises:
    TypeError:if cannont encode json properly
  """
  return json.dumps( content )

def decodeMessage( msgJSON ):
  """Decodes the message from the serialized JSON string
     See https://docs.python.org/2/library/json.html#py-to-json-table.
  Args:
    msgJSON(str):in the JSON format.
  Returns:
    str: decoded objecst.
  Raises:
    TypeError: if cannot decode JSON properly.
  """
  return json.loads( msgJSON )

def isMessageFormatCorrect( content ):
  """Checks if input format is correct.
     Function checks if the input format is a dictionnary
     in the following format:
     0) content is a dictionary,
     1) it contains only those keys of basestring types:
     'pilotUUID', 'status', 'minorStatus', 'timestamp', 'source',
     2) it contains only values of basestring types.
  Args:
    content(dict): all values must be non-empty
  Returns:
    bool: True if message format is correct, False otherwise
  Example:
    {"status": "DIRAC Installation",
     "timestamp": "1427121370.7",
      "minorStatus": "Uname = Linux localhost 3.10.64-85.cernvm.x86_64",
      "pilotUUID": "eda78924-d169-11e4-bfd2-0800275d1a0a",
      "source": "pilot"
      }
  """
  if not isinstance( content, dict ):
    return False
  refKeys = [
      'pilotUUID',
      'status',
      'minorStatus',
      'timestamp',
      'source'
      ]
  refKeys.sort()
  keys = content.keys()
  keys.sort()
  if not keys == refKeys:
    return False
  values = content.values()
  # if any value is not of basestring type
  if any( not isinstance( val, basestring ) for val in values ):
    return False
  #checking if all elements are not empty 
  if any( not val for val in values ):
    return False
  return True

def generateTimeStamp():
  """Generates the current timestamp in Epoch format.
  Returns:
    str: with number of seconds since the Epoch.
  """
  return str( time.time() )

def generateUniqueID():
  """Generates a unique identifier based on uuid1 function
  Returns:
    str: containing uuid
  """
  return str( uuid1() )

def getUniqueIDAndSaveToFile( filename = 'PilotAgentUUID' ):
  """Generates the unique id and writes it to a file
     of given name.
     First, we try to receive the UUID from the OS, if that fails
     the local uuid is generated.
  Args:
    filename(str): file to which the UUID will be saved
  Returns:
    bool: True if everything went ok False if there was an error with the file
  """
  myId = getUniqueIDFromOS()
  if not myId:
    myId = generateUniqueID()
  try:
    with open ( filename, 'w' ) as myFile:
      myFile.write( myId )
    return True
  except IOError:
    print 'could not open file'
    return False

def getUniqueIDFromOS():
  """Retrieves unique identifier based on specific OS.
    The OS type is identified based on some predefined
    environmental variables that should contain identifiers
    for given node. For VM the combination of 3 variables is used to
    create the identifier. Only the first found identifier is returned
  Returns:
    str: If variable(s)  found the generated identifier is returned. Empty
          string is returned if all checks fails. If there are more than one
          valid identifier, only the first one is returned.
  """
  #VM case: vm://$CE_NAME/$CE_NAME:$VMTYPE:$VM_UUID
  vmEnvVars = ['CE_NAME', 'VMTYPE', 'VM_UUID']
  if all ( var in os.environ for var in vmEnvVars):
    ce_name = os.environ.get('CE_NAME')
    partial_id = ':'.join((os.environ.get(var) for var in vmEnvVars))
    return  'vm://'+ ce_name + '/' + partial_id
  #Other cases: $envVar
  envVars = ['CREAM_JOBID', 'GRID_GLOBAL_JOBID']
  ids = ( os.environ.get(var) for var in envVars if var in os.environ)
  return next(ids, '')

def main():
  """Is used to generate the pilot uuid
     and save it to a file even
     before any DIRAC related part is installed.
  """
  filename = ' '.join( sys.argv[1:] )
  if not filename:
    getUniqueIDAndSaveToFile()
  else:
    getUniqueIDAndSaveToFile( filename )

if __name__ == '__main__':
  main()

