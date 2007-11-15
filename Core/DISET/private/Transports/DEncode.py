# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/Attic/DEncode.py,v 1.5 2007/11/15 11:10:42 acasajus Exp $
__RCSID__ = "$Id: DEncode.py,v 1.5 2007/11/15 11:10:42 acasajus Exp $"

# Encoding and decoding for dirac
#
# Ids
# i -> int
# I -> long
# f -> float
# b -> bool
# s -> string
# z -> datetime
# n -> none
# l -> list
# t -> tuple
# d -> dictionary


import types
import struct
import datetime

_dateTimeObject = datetime.datetime.utcnow()
_dateTimeType = type( _dateTimeObject )
_dateType     = type( _dateTimeObject.date() )
_timeType     = type( _dateTimeObject.time() )

g_dEncodeFunctions = {}
g_dDecodeFunctions = {}

#Encoding and decoding ints
def encodeInt( iValue, eList ):
  eList.extend( ( "i", str( iValue ), "e" ) )

def decodeInt( buffer, i ):
  i += 1
  end  = buffer.index( 'e', i )
  n = int( buffer[i:end] )
  return ( n, end + 1 )

g_dEncodeFunctions[ types.IntType ] = encodeInt
g_dDecodeFunctions[ "i" ] = decodeInt

#Encoding and decoding longs
def encodeLong( iValue, eList ):
  eList.extend( ( "l", str( iValue ), "e" ) )

def decodeLong( buffer, i ):
  i += 1
  end  = buffer.index( 'e', i )
  n = long( buffer[i:end] )
  return ( n, end + 1 )

g_dEncodeFunctions[ types.LongType ] = encodeLong
g_dDecodeFunctions[ "I" ] = decodeLong

#Encoding and decoding floats
def encodeFloat( iValue, eList ):
  eList.extend( ( "f", str( iValue ), "e" ) )

def decodeFloat( buffer, i ):
  i += 1
  end  = buffer.index( 'e', i )
  n = float( buffer[i:end] )
  return ( n, end + 1 )

g_dEncodeFunctions[ types.FloatType ] = encodeFloat
g_dDecodeFunctions[ "f" ] = decodeFloat

#Encoding and decoding booleand
def encodeBool( bValue, eList ):
  if bValue:
    eList.append( "b1" )
  else:
    eList.append( "b0" )	

def decodeBool( buffer, i ):
  if buffer[ i + 1 ] == "0":
    return ( False, i + 2 )
  else:
    return ( True, i + 2 )

g_dEncodeFunctions[ types.BooleanType ] = encodeBool
g_dDecodeFunctions[ "b" ] = decodeBool

#Encoding and decoding strings
def encodeString( sValue, eList ):
  eList.extend( ( 's', str( len( sValue ) ), ':', sValue ) ) 

def decodeString( buffer, i ):
  i += 1
  colon = buffer.index( ":", i )
  n = int( buffer[ i : colon ] )
  colon += 1
  end = colon + n
  return ( buffer[ colon : end] , end )

g_dEncodeFunctions[ types.StringType ] = encodeString
g_dDecodeFunctions[ "s" ] = decodeString

#Encoding and decoding datetime
def encodeDateTime( oValue, eList ):
  prefix = "z"
  if type( oValue ) == _dateTimeType:
    tDateTime = ( oValue.year, oValue.month, oValue.day, \
                      oValue.hour, oValue.minute, oValue.second, \
                      oValue.microsecond, oValue.tzinfo )
    eList.append( "za" )
    encode( tDateTime, eList )
  elif type( oValue ) == _dateType:
    tData = ( oValue.year, oValue.month, oValue. day )
    eList.append( "zd" )
    encode( tData, eList )
  elif type( oValue ) == _timeType:
    tTime = ( oValue.hour, oValue.minute, oValue.second, oValue.microsecond, oValue.tzinfo )
    eList.append( "zt" )
    encode( tTime, eList )
  else:
    raise Exception( "Unexpected type %s while encoding a datetime object" % str( type( oValue ) ) )

def decodeDateTime( buffer, i ):
  i += 1
  type = buffer[i]
  tupleObject, i = decode( buffer, i + 1 )
  if type == 'a':
    dtObject = datetime.datetime( *tupleObject )
  elif stype == 'd':
    dtObject = datetime.date( *tupleObject )
  elif type == 't':
    dtObject = datetime.time( *tupleObject )
  else:
    raise Exception( "Unexpected type %s while decoding a datetime object" % type )
  return ( dtObject, i )

g_dEncodeFunctions[ _dateTimeType ] = encodeDateTime
g_dEncodeFunctions[ _dateType ] = encodeDateTime
g_dEncodeFunctions[ _timeType ] = encodeDateTime
g_dDecodeFunctions[ 'z' ] = decodeDateTime

#Encoding and decoding None
def encodeNone( oValue, eList ):
  eList.append( "n" )

def decodeNone( buffer, i ):
  return ( None, i + 1 )

g_dEncodeFunctions[ types.NoneType ] = encodeNone
g_dDecodeFunctions[ 'n' ] = decodeNone

#Encode and decode a list
def encodeList( lValue, eList ):
  eList.append( "l" )
  for uObject in lValue:
    g_dEncodeFunctions[ type( uObject ) ]( uObject, eList ) 
  eList.append( "e" )

def decodeList( buffer, i ):
  oL = []
  i += 1
  while buffer[ i ] != "e":
    ob, i = g_dDecodeFunctions[ buffer[ i ] ]( buffer, i )
    oL.append( ob )
  return( oL, i + 1 )

g_dEncodeFunctions[ types.ListType ] = encodeList
g_dDecodeFunctions[ "l" ] = decodeList

#Encode and decode a tuple
def encodeTuple( lValue, eList ):
  eList.append( "t" )
  for uObject in lValue:
    g_dEncodeFunctions[ type( uObject ) ]( uObject, eList )
  eList.append( "e" )

def decodeTuple( buffer, i ):
  oL, i = decodeList( buffer, i )
  return ( tuple( oL ), i )

g_dEncodeFunctions[ types.TupleType ] = encodeTuple
g_dDecodeFunctions[ "t" ] = decodeTuple

#Encode and decode a dictionary
def encodeDict( dValue, eList ):
  eList.append( "d" )
  for key, value in dValue.items():
    g_dEncodeFunctions[ type( key ) ]( key, eList )
    g_dEncodeFunctions[ type( value ) ]( value, eList )	
  eList.append( "e" )

def decodeDict( buffer, i ):
  oD = {}
  i += 1
  while buffer[ i ] != "e":
    k, i = g_dDecodeFunctions[ buffer[ i ] ]( buffer, i )
    oD[ k ], i = g_dDecodeFunctions[ buffer[ i ] ]( buffer, i )
  return ( oD, i + 1 )

g_dEncodeFunctions[ types.DictType ] = encodeDict
g_dDecodeFunctions[ "d" ] = decodeDict


#Encode function
def encode( uObject ):
  try:
    eList = []
    #print "ENCODE FUNCTION : %s" % g_dEncodeFunctions[ type( uObject ) ]
    g_dEncodeFunctions[ type( uObject ) ]( uObject, eList )
    return "".join( eList )
  except Exception, e:
    raise

def decode( buffer ):
  try:
    #print "DECODE FUNCTION : %s" % g_dDecodeFunctions[ sStream [ iIndex ] ]
    return g_dDecodeFunctions[ buffer[ 0 ] ]( buffer, 0 )
  except Exception, e:
    raise


if __name__=="__main__":
  uObject = {2:"3", True : (3,None) }
  print "Initial: %s" % uObject
  sData = encode( uObject )
  print "Encoded: %s" % sData
  print "Decoded: %s, [%s]" % decode( sData )


