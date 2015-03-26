""" A set of tools for the remote pilot agent logging system
"""

__RCSID__ = "$Id$"

import time
import json
from uuid import uuid1
import sys

def generateDict( pilotUUID, pilotID, status, minorStatus, timestamp, source ):
  """
  Helper function that returs a dictionnary based on the
  set of input values
  @return : dict
  """

  keys = [
      'pilotUUID', 
      'pilotID',  
      'status', 
      'minorStatus', 
      'timestamp', 
      'source'
      ]
  values = [
      pilotUUID,
      pilotID,
      status,
      minorStatus,
      timestamp,
      source
      ]
  return dict( zip( keys, values ) )

def encodeMessage( content ):
  """
  Method encodes the message in form of the serialized JSON string
  see https://docs.python.org/2/library/json.html#py-to-json-table
  @type input : dict
  @return : string in the JSON format or rise TypeError exception
  """
  return json.dumps( content )

def decodeMessage( msgJSON ):
  """
  Method decodes the message from the serialized JSON string
  See https://docs.python.org/2/library/json.html#py-to-json-table
  @type input : string in JSON format
  @return : decoded object or raises TypeError exception
  """
  return json.loads( msgJSON )

def isMessageFormatCorrect( content ):
  """
  Function checks if the input format is a dictionnary
  in the following format:
  0) content is a dictionary
  1) it contains only those keys of string types:
  'pilotUUID', 'pilotId', 'status', 'minorStatus', 'timestamp', 'source'
  2) it contains only values of string types
  e.g.
  {"status": "DIRAC Installation",
   "timestamp": "1427121370.7",
    "minorStatus": "Uname = Linux localhost 3.10.64-85.cernvm.x86_64",
    "pilotID": "1",
    "pilotUUID": "eda78924-d169-11e4-bfd2-0800275d1a0a",
    "source": "pilot"
    }
  3)pilotID can be empty, other values must be non-empty 
  @return : True if message format is correct, False otherwise
  """
  if not isinstance( content, dict ):
    return False
  refKeys = [
      'pilotUUID',
      'pilotID',
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
  # if any value is not of string type
  if any( not isinstance( val, str ) for val in values ):
    return False
  #checking if not empty for all except pilotID
  contentCopy = content.copy()
  contentCopy.pop('pilotID',None)
  values = contentCopy.values()
  if any( not val for val in values ):
    return False
  return True

def generateTimeStamp():
  """
  Function generates the current timestamp in Epoch format
  @return : string with number of seconds since the Epoch
  """

  return str( time.time() )

def generateUniqueID():
  """
  Function generates a unique identifier based on uuid1 function
  @return : string containing uuid
  """

  return str( uuid1() )

def generateUniqueIDAndSaveToFile( filename = 'PilotAgentUUID' ):
  """
  Function generates the unique id and writes it to a file
  of given name: filename
  @return: True if everything went ok False if there was an error with the file
  """
  myId = generateUniqueID()
  try:
    myFile = open( filename, 'w' )
    myFile.write( myId )
  except IOError:
    print 'could not open file'
    return False
  else:
    myFile.close()
    return True

def main():
  """ main() function is used to generate the pilot uuid
  and save it to a file even
  before any DIRAC related part is installed
  """
  filename = ' '.join( sys.argv[1:] )
  if not filename:
    generateUniqueIDAndSaveToFile()
  else:
    generateUniqueIDAndSaveToFile( filename )

if __name__ == '__main__':
  main()

