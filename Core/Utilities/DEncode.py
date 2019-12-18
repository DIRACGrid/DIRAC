# $HeadURL$
"""
Encoding and decoding for dirac, Ids:
 i -> int
 I -> long
 f -> float
 b -> bool
 s -> string
 z -> datetime
 n -> none
 l -> list
 t -> tuple
 d -> dictionary
"""
from __future__ import print_function
__RCSID__ = "$Id$"

from past.builtins import long
import six
import types
import datetime
import os

import inspect
import traceback
from pprint import pprint


# Setting this environment variable to any value will enable the dump of the debugging
# call stack
DIRAC_DEBUG_DENCODE_CALLSTACK = bool(os.environ.get('DIRAC_DEBUG_DENCODE_CALLSTACK', False))

# Depth of the stack to look for with inspect
CONTEXT_DEPTH = 100


def printDebugCallstack(headerMessage):
  """ Prints information about the current stack as well as the caller parameters.
      The purpose of this method is to track down all the places in DIRAC that might
      not survive the change to JSON encoding.

      :param headerMessage: message to be displayed first
      :returns: None

  """
  def stripArgs(frame):
    """ Keeps only the parameters and their values from a frame

        :param frame: frame object

        :returns: dict {param name: value}
    """
    # Get all the arguments of the call
    allArgs = inspect.getargvalues(frame)
    # Keep only the arguments that are parameters of the call, as well as their value
    return dict([(argName, allArgs.locals[argName]) for argName in allArgs.args])

  tb = traceback.format_stack()
  frames = inspect.stack(context=CONTEXT_DEPTH)

  # The datetime are encoded as tuple. Since datetime are taken care of
  # in JSerializer, just don't print a warning here
  # Note: -3 because we have to go past (de/encodeTuple and the Traceback module)
  if 'encodeDateTime' in tb[-3] or 'decodeDateTime' in tb[-3]:
    return

  # The accountingDB stores a encoding of the bucketsLength
  # this is ok for now, so silent all the AccountingDB error
  if any(['AccountingDB' in tr for tr in reversed(tb)]):
    return

  print('=' * 45, headerMessage, '=' * 45)

  # print the traceback that leads us here
  # remove the last element which is the traceback module call
  for line in tb[:-1]:
    print(line)

  # Now we try to navigate up to the caller of dEncode.
  # For this, we find the frame in which we enter dEncode.
  # We keep the parameters to display it.
  # Then we navigate to the parent frame, and we display the file
  # and line number where this call was done
  try:
    framesIter = iter(frames)
    for frame in framesIter:
      # First check that we are using either 'encode' or 'decode' function
      if frame[3] in ('encode', 'decode'):
        # Then check it is the good file
        if frame[1].endswith('DIRAC/Core/Utilities/DEncode.py'):
          # Keep the arguments of the DEncode call
          dencArgs = stripArgs(frame[0])
          # Take the calling frame
          frame = next(framesIter)
          print("Calling frame: %s" % (frame[1:3],))
          print("With arguments ", end=' ')
          pprint(dencArgs)
          break

  except BaseException:
    pass
  print("=" * 100)
  print()
  print()


_dateTimeObject = datetime.datetime.utcnow()
_dateTimeType = type(_dateTimeObject)
_dateType = type(_dateTimeObject.date())
_timeType = type(_dateTimeObject.time())

g_dEncodeFunctions = {}
g_dDecodeFunctions = {}


def encodeInt(iValue, eList):
  """Encoding ints """

  eList.extend(("i", str(iValue), "e"))


def decodeInt(data, i):
  """Decoding ints """

  i += 1
  end = data.index('e', i)
  value = int(data[i:end])
  return (value, end + 1)


g_dEncodeFunctions[types.IntType] = encodeInt
g_dDecodeFunctions["i"] = decodeInt


def encodeLong(iValue, eList):
  """ Encoding longs """

  # corrected by KGG   eList.extend( ( "l", str( iValue ), "e" ) )
  eList.extend(("I", str(iValue), "e"))


def decodeLong(data, i):
  """ Decoding longs """

  i += 1
  end = data.index('e', i)
  value = long(data[i:end])
  return (value, end + 1)


g_dEncodeFunctions[types.LongType] = encodeLong
g_dDecodeFunctions["I"] = decodeLong


def encodeFloat(iValue, eList):
  """ Encoding floats """

  eList.extend(("f", str(iValue), "e"))


def decodeFloat(data, i):
  """ Decoding floats """

  i += 1
  end = data.index('e', i)
  if end + 1 < len(data) and data[end + 1] in ('+', '-'):
    eI = end
    end = data.index('e', end + 1)
    value = float(data[i:eI]) * 10 ** int(data[eI + 1:end])
  else:
    value = float(data[i:end])
  return (value, end + 1)


g_dEncodeFunctions[types.FloatType] = encodeFloat
g_dDecodeFunctions["f"] = decodeFloat


def encodeBool(bValue, eList):
  """ Encoding booleans """

  if bValue:
    eList.append("b1")
  else:
    eList.append("b0")


def decodeBool(data, i):
  """ Decoding booleans """

  if data[i + 1] == "0":
    return (False, i + 2)
  else:
    return (True, i + 2)


g_dEncodeFunctions[types.BooleanType] = encodeBool
g_dDecodeFunctions["b"] = decodeBool


def encodeString(sValue, eList):
  """ Encoding strings """
  eList.extend(('s', str(len(sValue)), ':', sValue))


def decodeString(data, i):
  """ Decoding strings """
  i += 1
  colon = data.index(":", i)
  value = int(data[i: colon])
  colon += 1
  end = colon + value
  return (data[colon: end], end)


g_dEncodeFunctions[types.StringType] = encodeString
g_dDecodeFunctions["s"] = decodeString


def encodeUnicode(sValue, eList):
  """ Encoding unicode strings """
  valueStr = sValue.encode('utf-8')
  eList.extend(('u', str(len(valueStr)), ':', valueStr))


def decodeUnicode(data, i):
  """ Decoding unicode strings """

  i += 1
  colon = data.index(":", i)
  value = int(data[i: colon])
  colon += 1
  end = colon + value
  return (unicode(data[colon: end], 'utf-8'), end)


g_dEncodeFunctions[types.UnicodeType] = encodeUnicode
g_dDecodeFunctions["u"] = decodeUnicode


def encodeDateTime(oValue, eList):
  """ Encoding datetime """

  if isinstance(oValue, _dateTimeType):
    tDateTime = (oValue.year, oValue.month, oValue.day,
                 oValue.hour, oValue.minute, oValue.second,
                 oValue.microsecond, oValue.tzinfo)
    eList.append("za")
    # corrected by KGG encode( tDateTime, eList )
    g_dEncodeFunctions[type(tDateTime)](tDateTime, eList)
  elif isinstance(oValue, _dateType):
    tData = (oValue.year, oValue.month, oValue. day)
    eList.append("zd")
    # corrected by KGG encode( tData, eList )
    g_dEncodeFunctions[type(tData)](tData, eList)
  elif isinstance(oValue, _timeType):
    tTime = (oValue.hour, oValue.minute, oValue.second, oValue.microsecond, oValue.tzinfo)
    eList.append("zt")
    # corrected by KGG encode( tTime, eList )
    g_dEncodeFunctions[type(tTime)](tTime, eList)
  else:
    raise Exception("Unexpected type %s while encoding a datetime object" % str(type(oValue)))


def decodeDateTime(data, i):
  """ Decoding datetime """

  i += 1
  dataType = data[i]
  # corrected by KGG tupleObject, i = decode( data, i + 1 )
  tupleObject, i = g_dDecodeFunctions[data[i + 1]](data, i + 1)
  if dataType == 'a':
    dtObject = datetime.datetime(*tupleObject)
  elif dataType == 'd':
    dtObject = datetime.date(*tupleObject)
  elif dataType == 't':
    dtObject = datetime.time(*tupleObject)
  else:
    raise Exception("Unexpected type %s while decoding a datetime object" % dataType)
  return (dtObject, i)


g_dEncodeFunctions[_dateTimeType] = encodeDateTime
g_dEncodeFunctions[_dateType] = encodeDateTime
g_dEncodeFunctions[_timeType] = encodeDateTime
g_dDecodeFunctions['z'] = decodeDateTime


def encodeNone(_oValue, eList):
  """ Encoding None """

  eList.append("n")


def decodeNone(_data, i):
  """ Decoding None """

  return (None, i + 1)


g_dEncodeFunctions[types.NoneType] = encodeNone
g_dDecodeFunctions['n'] = decodeNone


def encodeList(lValue, eList):
  """ Encoding list """

  eList.append("l")
  for uObject in lValue:
    g_dEncodeFunctions[type(uObject)](uObject, eList)
  eList.append("e")


def decodeList(data, i):
  """ Decoding list """

  oL = []
  i += 1
  while data[i] != "e":
    ob, i = g_dDecodeFunctions[data[i]](data, i)
    oL.append(ob)
  return(oL, i + 1)


g_dEncodeFunctions[types.ListType] = encodeList
g_dDecodeFunctions["l"] = decodeList


def encodeTuple(lValue, eList):
  """ Encoding tuple """

  if DIRAC_DEBUG_DENCODE_CALLSTACK:
    printDebugCallstack('Encoding tuples')

  eList.append("t")
  for uObject in lValue:
    g_dEncodeFunctions[type(uObject)](uObject, eList)
  eList.append("e")


def decodeTuple(data, i):
  """ Decoding tuple """

  if DIRAC_DEBUG_DENCODE_CALLSTACK:
    printDebugCallstack('Decoding tuples')

  oL, i = decodeList(data, i)
  return (tuple(oL), i)


g_dEncodeFunctions[types.TupleType] = encodeTuple
g_dDecodeFunctions["t"] = decodeTuple


def encodeDict(dValue, eList):
  """ Encoding dictionary """

  if DIRAC_DEBUG_DENCODE_CALLSTACK:
    # If we have numbers as keys
    if any([isinstance(x, six.integer_types + (float,)) for x in dValue]):
      printDebugCallstack("Encoding dict with numeric keys")

  eList.append("d")
  for key in sorted(dValue):
    g_dEncodeFunctions[type(key)](key, eList)
    g_dEncodeFunctions[type(dValue[key])](dValue[key], eList)
  eList.append("e")


def decodeDict(data, i):
  """ Decoding dictionary """

  oD = {}
  i += 1
  while data[i] != "e":

    if DIRAC_DEBUG_DENCODE_CALLSTACK:
      # If we have numbers as keys
      if data[i] in ('i', 'I', 'f'):
        printDebugCallstack("Decoding dict with numeric keys")

    k, i = g_dDecodeFunctions[data[i]](data, i)
    oD[k], i = g_dDecodeFunctions[data[i]](data, i)
  return (oD, i + 1)


g_dEncodeFunctions[types.DictType] = encodeDict
g_dDecodeFunctions["d"] = decodeDict


# Encode function
def encode(uObject):
  """ Generic encoding function """

  try:
    eList = []
    # print "ENCODE FUNCTION : %s" % g_dEncodeFunctions[ type( uObject ) ]
    g_dEncodeFunctions[type(uObject)](uObject, eList)
    return "".join(eList)
  except Exception:
    raise


def decode(data):
  """ Generic decoding function """
  if not data:
    return data
  try:
    # print "DECODE FUNCTION : %s" % g_dDecodeFunctions[ sStream [ iIndex ] ]
    return g_dDecodeFunctions[data[0]](data, 0)
  except Exception:
    raise


if __name__ == "__main__":
  gObject = {2: "3", True: (3, None), 2.0 * 10 ** 20: 2.0 * 10 ** -10}
  print("Initial: %s" % gObject)
  gData = encode(gObject)
  print("Encoded: %s" % gData)
  print("Decoded: %s, [%s]" % decode(gData))
