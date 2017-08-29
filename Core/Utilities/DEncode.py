###############################################################################
#                                DEncode.py                                   #
###############################################################################
__RCSID__ = "$Id$"

import json
import datetime

_dateTimeObject = datetime.datetime.utcnow()
_dateTimeType = type( _dateTimeObject )
_dateType = type( _dateTimeObject.date() )
_timeType = type( _dateTimeObject.time() )

################################################################################
#                  New code for JSON (un)marshalling                           #
################################################################################
def hintParticularTypes( objectToEncode ):
  """This function detects tuples and longs and replaces them with dictionaries.
  This allows us to prserve these data types. By default, 'json.dumps()' encodes
  tuples into arrays, (like python lists) and longs into int numbers
  (like python ints). By using directly 'json.loads()', without
  'DEncode.hintParticularTypes()', arrays are decoded into lists (so we
  lose our tuples) and int numbers into ints (then we also lose long ints)."""

  if isinstance( objectToEncode, tuple ):
    L = []
    for i in objectToEncode:
      L.append( hintParticularTypes( i ) )
    newTuple = tuple( L )
    return {'__tuple__': True, 'items': newTuple}
  elif isinstance( objectToEncode, long ):
    return {'__long__': True, 'value': objectToEncode}
  elif isinstance( objectToEncode, list ):
    return [hintParticularTypes( e ) for e in objectToEncode]
  elif isinstance( objectToEncode, dict ):
    newDict = {}
    for key in objectToEncode:
      newDict[ key ] = hintParticularTypes( objectToEncode[ key ] )
    return newDict
  elif isinstance( objectToEncode, _dateTimeType ):
    dateTimeTuple = ( objectToEncode.year, objectToEncode.month, objectToEncode.day, objectToEncode.hour,
                      objectToEncode.minute, objectToEncode.second,
                      objectToEncode.microsecond, objectToEncode.tzinfo )
    return {'__dateTime__':True, 'items':dateTimeTuple}
  elif isinstance(objectToEncode, _dateType):
    dateTuple = ( objectToEncode.year, objectToEncode.month, objectToEncode. day )
    return {"__date__":True, 'items':dateTuple}
  elif isinstance(objectToEncode, _timeType):
    timeTuple = ( objectToEncode.hour, objectToEncode.minute, objectToEncode.second, objectToEncode.microsecond, objectToEncode.tzinfo )
    return {"__time__":True, 'items':timeTuple}
  else:
    return objectToEncode

def DetectHintedParticularTypes( objectToEncode ):
  """This function detecs dictionaries encoding tuples and longs and replaces
  them with the correct data structures. """
  newTuple = tuple()
  if isinstance(objectToEncode, list):
    return [DetectHintedParticularTypes(e) for e in objectToEncode]
  elif isinstance( objectToEncode, dict ):
    if '__tuple__' in objectToEncode:
      newTuple = DetectHintedParticularTypes( objectToEncode['items'] )
      return tuple(newTuple)
    elif '__long__' in objectToEncode:
      return long( objectToEncode['value'] )
    elif '__dateTime__' in objectToEncode:
      L = list()
      for i in objectToEncode['items']:
        L.append(i)
      newTuple = tuple(L)
      return datetime.datetime(*newTuple)
    elif '__date__' in objectToEncode:
      L = list()
      for i in objectToEncode['items']:
        L.append(i)
      newTuple = tuple(L)
      return datetime.date(*newTuple)
    elif '__time__' in objectToEncode:
      L = list()
      for i in objectToEncode['items']:
        L.append(i)
      newTuple = tuple(L)
      return datetime.time(*newTuple)
    else:
      newDict = {}
      for key in objectToEncode:
        newDict[key] = DetectHintedParticularTypes( objectToEncode[key] )
      return newDict
  elif isinstance(objectToEncode, tuple):
    L = list()
    for i in objectToEncode:
      L.append( DetectHintedParticularTypes( i ) )
    newTuple = tuple( L )
    return newTuple
  else:
    return objectToEncode

class newEncoder(json.JSONEncoder):
  def encode( self, objectToEncode ):
    return super( newEncoder, self ).encode( hintParticularTypes( objectToEncode ) )

#################################################################################
#################################################################################
def encode( uObject ):
  """This function turns the uObject data into serialized data.
  the serialized data is written in JSON format."""

  coding = newEncoder()
  jsonString = coding.encode( uObject )
  return jsonString

def decode( data ):
  """This function turns a serialized string into a data structure."""

  return json.loads( data, object_hook =  DetectHintedParticularTypes )
