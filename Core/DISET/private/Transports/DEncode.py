# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/Attic/DEncode.py,v 1.1 2007/03/09 15:27:45 rgracian Exp $
__RCSID__ = "$Id: DEncode.py,v 1.1 2007/03/09 15:27:45 rgracian Exp $"

import types
import struct
import datetime

g_dEncodeFunctions = {}
g_dDecodeFunctions = {}

#Encoding and decoding ints
def encodeInt( iValue ):
  return "i%se" % iValue

def decodeInt( sStream, iIndex ):
  iEndPos = sStream[ iIndex: ].find( "e" )
  sNumberString = sStream[ iIndex + 1: iIndex + iEndPos ]
  return ( int( sNumberString ), iIndex + iEndPos + 1 )

g_dEncodeFunctions[ types.IntType ] = encodeInt
g_dDecodeFunctions[ "i" ] = decodeInt
  
#Encoding and decoding longs
def encodeLong( iValue ):
  return "I%se" % iValue

def decodeLong( sStream, iIndex ):
  iEndPos = sStream[ iIndex: ].find( "e" )
  sNumberString = sStream[ iIndex + 1: iIndex + iEndPos ] 
  return ( long( sNumberString ), iIndex + iEndPos + 1 )

g_dEncodeFunctions[ types.LongType ] = encodeLong
g_dDecodeFunctions[ "I" ] = decodeLong

#Encoding and decoding floats
def encodeFloat( fValue ):
  return "f%se" % fValue

def decodeFloat( sStream, iIndex ):
  iEndPos = sStream[ iIndex: ].find( "e" )
  sNumberString = sStream[ iIndex + 1: iIndex + iEndPos ] 
  return ( float( sNumberString ), iIndex + iEndPos + 1 )

g_dEncodeFunctions[ types.FloatType ] = encodeFloat
g_dDecodeFunctions[ "f" ] = decodeFloat

#Encoding and decoding booleand
def encodeBool( bValue ):
  if bValue:
    return "b1"
  else:
    return "b0"
  
def decodeBool( sStream, iIndex ):
  if sStream[ iIndex + 1 ] == "0":
    return ( False, iIndex + 2 )
  else:
    return ( True, iIndex + 2 )
  
g_dEncodeFunctions[ types.BooleanType ] = encodeBool
g_dDecodeFunctions[ "b" ] = decodeBool

#Encoding and decoding strings
def encodeString( sValue ):
  return "s%s:%s" % ( len( sValue), sValue )

def decodeString( sStream, iIndex ):
    iSeparatorPosition = sStream[ iIndex + 1: ].find( ":" )
    iLength = int( sStream[ iIndex + 1 : iIndex + 1 + iSeparatorPosition ] )
    iStringStart = iIndex + 2 + iSeparatorPosition
    iStringEnd = iIndex + 2 + iSeparatorPosition + iLength
    return ( sStream[ iStringStart: iStringEnd ], iStringEnd )

g_dEncodeFunctions[ types.StringType ] = encodeString
g_dDecodeFunctions[ "s" ] = decodeString

#Encode and decode a list
def encodeList( lValue ):
    sListString = "l"
    for uObject in lValue:
      sListString += encode( uObject )
    return "%se" % sListString
  
def decodeList( sStream, iIndex ):
  lObjects = []
  iIndex += 1
  while sStream[ iIndex ] != "e":
    uObject, iIndex = decode( sStream, iIndex )
    lObjects.append( uObject )
  return( lObjects, iIndex + 1 )


g_dEncodeFunctions[ types.ListType ] = encodeList
g_dDecodeFunctions[ "l" ] = decodeList


#Encode and decode a tuple
def encodeTuple( lValue ):
    sTupleString = "t"
    for uObject in lValue:
      sTupleString += encode( uObject )
    return "%se" % sTupleString
  
def decodeTuple( sStream, iIndex ):
  lObjects, iIndex = decodeList( sStream, iIndex )
  return ( tuple( lObjects ), iIndex )

g_dEncodeFunctions[ types.TupleType ] = encodeTuple
g_dDecodeFunctions[ "t" ] = decodeTuple

#Encode and decode a dictionary
def encodeDict( dValue ):
  sDictString = "d"
  for uKey in dValue.keys():
    sDictString += encode( uKey )
    sDictString += encode( dValue[ uKey ] )
  return "%se" % sDictString

def decodeDict( sStream, iIndex ):
  dObjects = {}
  iIndex += 1
  while sStream[ iIndex ] != "e":
    uKey, iIndex = decode( sStream, iIndex )
    uValue, iIndex = decode( sStream, iIndex )
    dObjects[ uKey ] = uValue
  return ( dObjects, iIndex )

g_dEncodeFunctions[ types.DictType ] = encodeDict
g_dDecodeFunctions[ "d" ] = decodeDict


#Encode function
def encode( uObject ):
  try:
    #print "ENCODE FUNCTION : %s" % g_dEncodeFunctions[ type( uObject ) ]
    return g_dEncodeFunctions[ type( uObject ) ]( uObject )
  except Exception, e:
    raise
  
def decode( sStream, iIndex = 0 ):
  try:
    #print "DECODE FUNCTION : %s" % g_dDecodeFunctions[ sStream [ iIndex ] ]
    return g_dDecodeFunctions[ sStream[ iIndex ] ]( sStream, iIndex )
  except Exception, e:
    raise
  
  
if __name__=="__main__":
  uObject = {
  "algo": [ 10,2,3,4,5,6,7,8,9,0.123456789 ],
  105:2123123123123213,
  223423.324:"asfasf",
  "bools": [ False, True ]
   }
  sData = encode( uObject )  
  print sData
  print decode( sData )
  
