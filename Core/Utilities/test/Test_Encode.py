""" Test Encoding function of DIRAC
It contains tests for DISET and JSON.
Some tests can be passed by both, while some can only be passed by one.

Typically, we know JSON cannot serialize tuples, or integers as dictionary keys.
On the other hand, it can serialize some objects, while DISET cannot.

"""


from string import printable
import datetime
import sys


from DIRAC.Core.Utilities.DEncode import encode as disetEncode, decode as disetDecode, g_dEncodeFunctions
from DIRAC.Core.Utilities.JEncode import encode as jsonEncode, decode as jsonDecode, JSerializable

from hypothesis import given
from hypothesis.strategies import builds, integers, lists, recursive, floats, text,\
    booleans, none, dictionaries, tuples, datetimes

from pytest import mark, approx, raises, fixture
parametrize = mark.parametrize


# List of couple (encoding, decoding) functions
# In order to test a new library, import the encode/decode
# function, and add the tuple here

disetTuple = (disetEncode, disetDecode)
jsonTuple = (jsonEncode, jsonDecode)

enc_dec_imp = (disetTuple, jsonTuple)
enc_dec_ids = ('disetTuple', 'jsonTuple')


def myDatetimes():
  """We define a custom datetime strategy in order
     to pull date after 1900 (limitation of strftime)
     and without microseconds
  """
  # Build a strategy by removing the microsecond from a datetimes strategy
  # https://hypothesis.readthedocs.io/en/latest/data.html#hypothesis.strategies.builds
  return builds(lambda inDt: inDt.replace(microsecond=0), datetimes(
      min_value=datetime.datetime(1900, 1, 1, 0, 0),
      max_value=datetime.datetime.max,
      timezones=none()))


# These initial strategies are the basic types supported by the original dEncode
# Unfortuately we cannot make nested structure with floats because as the floats
# are not stable, the result is approximative, and it becomes extremely difficult
# to compare
# Datetime also starts only at 1900 because earlier date can't be dumped with strftime
initialStrategies = none() | booleans() | text() | integers() | myDatetimes()
initialJsonStrategies = none() | booleans() | text() | myDatetimes()


# From a strategy (x), make a new strategy
# We basically use that to make nested structures
# see http://hypothesis.readthedocs.io/en/latest/data.html#recursive-data

nestedStrategy = recursive(
    initialStrategies,
    lambda x: lists(x) | dictionaries(
        text(),
        x) | tuples(x))

# This strategy does not return tuples
nestedStrategyJson = recursive(
    initialJsonStrategies,
    lambda x: lists(x) | dictionaries(
        text(),
        x))


def test_everyBaseTypeIsTested():
  """ Make sure that each supported base type in the original
      DEncode module are tested here.
      We rely on the fact that the test function will be called
      "test_BaseType"
  """
  current_module = sys.modules[__name__]

  for encodeFunc in g_dEncodeFunctions.itervalues():
    testFuncName = ('test_BaseType_%s' % encodeFunc.__name__).replace('encode', '')
    getattr(current_module, testFuncName)


def agnosticTestFunction(enc_dec, data):
  """ Function called by all the other to test that
      decode(encode) returns the original data

      :param enc_dec: tuple of function (encoding, decoding)
      :param data: data to be worked on
  """
  encode, decode = enc_dec
  encodedData = encode(data)
  decodedData, lenData = decode(encodedData)

  assert data == decodedData
  assert lenData == len(encodedData)

  return decodedData


@fixture(scope="function", params=enc_dec_imp, ids=enc_dec_ids)
def enc_dec(request):
  """ Fixture to generate the (encoding, decoding) tuple """

  return request.param


# @parametrize('enc_dec', enc_dec_imp)
@given(data=booleans())
def test_BaseType_Bool(enc_dec, data):
  """ Test for boolean"""
  agnosticTestFunction(enc_dec, data)


# @parametrize('enc_dec', enc_dec_imp)
@given(data=myDatetimes())
def test_BaseType_DateTime(enc_dec, data):
  """ Test for data time"""

  agnosticTestFunction(enc_dec, data)


# Json does not serialize keys as integers but as string
@parametrize('enc_dec', [disetTuple])
@given(data=dictionaries(integers(), integers()))
def test_BaseType_Dict(enc_dec, data):
  """ Test for basic dict"""
  agnosticTestFunction(enc_dec, data)


# @parametrize('enc_dec', enc_dec_imp)
@given(data=integers(max_value=sys.maxsize))
def test_BaseType_Int(enc_dec, data):
  """ Test for integer"""
  agnosticTestFunction(enc_dec, data)

# CAUTION: DEncode is not precise for floats !!


# @parametrize('enc_dec', enc_dec_imp)
@given(data=floats(allow_nan=False))
def test_BaseType_Float(enc_dec, data):
  """ Test that float is approximatly stable"""
  encode, decode = enc_dec
  encodedData = encode(data)
  decodedData, lenData = decode(encodedData)
  assert data == approx(decodedData)
  assert lenData == len(encodedData)


# @parametrize('enc_dec', enc_dec_imp)
@given(data=lists(integers()))
def test_BaseType_List(enc_dec, data):
  """ Test for List """
  agnosticTestFunction(enc_dec, data)


# @parametrize('enc_dec', enc_dec_imp)
@given(data=integers(min_value=sys.maxsize + 1))
def test_BaseType_Long(enc_dec, data):
  """ Test long type"""
  agnosticTestFunction(enc_dec, data)


# @parametrize('enc_dec', enc_dec_imp)
def test_BaseType_None(enc_dec, ):
  """ Test None case """
  agnosticTestFunction(enc_dec, None)


# @parametrize('enc_dec', enc_dec_imp)
@given(data=text(printable))
def test_BaseType_String(enc_dec, data):
  """ Test basic strings"""
  # we need to cast to str because text() returns unicode
  data = str(data)
  agnosticTestFunction(enc_dec, data)


# Tuple are not serialized in JSON
@parametrize('enc_dec', [disetTuple])
@given(data=tuples(integers()))
def test_BaseType_Tuple(enc_dec, data):
  """ Test basic tuple """
  agnosticTestFunction(enc_dec, data)


# @parametrize('enc_dec', enc_dec_imp)
@given(data=text())
def test_BaseType_Unicode(enc_dec, data):
  """ Test unicode data """
  agnosticTestFunction(enc_dec, data)


# Json will not pass this because of tuples and integers as dict keys
@parametrize('enc_dec', [disetTuple])
@given(data=nestedStrategy)
def test_nestedStructure(enc_dec, data):
  """ Test nested structure """
  agnosticTestFunction(enc_dec, data)


# DEncode raises KeyError.....
# Others raise TypeError
# @parametrize('enc_dec', enc_dec_imp)
def test_NonSerializable(enc_dec):
  """ Test that a class that does not inherit from the serializable class
      raises TypeError
  """

  class NonSerializable(object):
    """ Dummy class not serializable"""
    pass

  data = NonSerializable()
  with raises((TypeError, KeyError)):
    agnosticTestFunction(enc_dec, data)


class Serializable(JSerializable):
  """ Dummy class inheriting from JSerializable"""

  _attrToSerialize = ['instAttr']

  def __init__(self, instAttr=None):
    self.instAttr = instAttr

  def __eq__(self, other):
    return all([getattr(self, attr) == getattr(other, attr) for attr in self._attrToSerialize])


@given(data=nestedStrategyJson)
def test_Serializable(data):
  """ Test if a simple serializable class with one random argument
      can be serialized
  """

  objData = Serializable(instAttr=data)

  agnosticTestFunction(jsonTuple, objData)


def test_nonDeclaredAttr():
  """ Tests that an argument not in the list of arguments to serialized
      is not serialized
  """

  objData = Serializable()
  objData.notToBeSerialized = 1

  encodedData = jsonEncode(objData)
  decodedData, _lenData = jsonDecode(encodedData)

  assert not hasattr(decodedData, 'notToBeSerialized')


class BadSerializable(JSerializable):
  """ Missing _attrToSerialize attribute """
  pass


def test_missingAttrToSerialize():
  """ Tests that an argument not in the list of arguments to serialized
      is not serialized
  """

  objData = BadSerializable()

  with raises(TypeError):
    agnosticTestFunction(jsonTuple, objData)


@given(data=nestedStrategyJson)
def test_nestedSerializable(data):
  """ Test that a serializable containing a serializable class
      can be serialized
  """

  subObj = Serializable(instAttr=data)
  objData = Serializable(instAttr=subObj)
  agnosticTestFunction(jsonTuple, objData)
