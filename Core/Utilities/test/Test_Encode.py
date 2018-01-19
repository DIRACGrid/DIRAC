"""Test Encoding function of DIRAC
Any library that would pretend replacing diset should pass this tests.

"""


from string import printable
import sys

from DIRAC.Core.Utilities.DEncode import encode as disetEncode, decode as disetDecode, g_dEncodeFunctions

from hypothesis import given
from hypothesis.strategies import integers, lists, recursive, floats, text,\
    booleans, none, dictionaries, tuples, datetimes

from pytest import mark, approx
parametrize = mark.parametrize


# List of couple (encoding, decoding) functions
# In order to test a new library, import the encode/decode
# function, and add the tuple here

enc_dec_imp = ((disetEncode, disetDecode),)


# These initial strategies are the basic types supported by the original dEncode
# Unfortuately we cannot make nested structure with floats because as the floats
# are not stable, the result is approximative, and it becomes extremely difficult
# to compare
initialStrategies = none() | booleans() | text() | integers() | datetimes()

# From a strategy (x), make a new strategy
# We basically use that to make nested structures
# see http://hypothesis.readthedocs.io/en/latest/data.html#recursive-data

nestedStrategy = recursive(
    initialStrategies,
    lambda x: lists(x) | dictionaries(
        text(),
        x) | tuples(x))


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


@parametrize('enc_dec', enc_dec_imp)
@given(data=booleans())
def test_BaseType_Bool(enc_dec, data):
  """ Test for boolean"""
  agnosticTestFunction(enc_dec, data)


@parametrize('enc_dec', enc_dec_imp)
@given(data=datetimes())
def test_BaseType_DateTime(enc_dec, data):
  """ Test for data time"""
  agnosticTestFunction(enc_dec, data)


@parametrize('enc_dec', enc_dec_imp)
@given(data=dictionaries(integers(), integers()))
def test_BaseType_Dict(enc_dec, data):
  """ Test for basic dict"""
  agnosticTestFunction(enc_dec, data)


@parametrize('enc_dec', enc_dec_imp)
@given(data=integers(max_value=sys.maxsize))
def test_BaseType_Int(enc_dec, data):
  """ Test for integer"""
  agnosticTestFunction(enc_dec, data)

# CAUTION: DEncode is not precise for floats !!


@parametrize('enc_dec', enc_dec_imp)
@given(data=floats(allow_nan=False))
def test_BaseType_Float(enc_dec, data):
  """ Test that float is approximatly stable"""
  encode, decode = enc_dec
  encodedData = encode(data)
  decodedData, lenData = decode(encodedData)
  assert data == approx(decodedData)
  assert lenData == len(encodedData)


@parametrize('enc_dec', enc_dec_imp)
@given(data=lists(integers()))
def test_BaseType_List(enc_dec, data):
  """ Test for List """
  agnosticTestFunction(enc_dec, data)


@parametrize('enc_dec', enc_dec_imp)
@given(data=integers(min_value=sys.maxsize + 1))
def test_BaseType_Long(enc_dec, data):
  """ Test long type"""
  agnosticTestFunction(enc_dec, data)


@parametrize('enc_dec', enc_dec_imp)
def test_BaseType_None(enc_dec, ):
  """ Test None case """
  agnosticTestFunction(enc_dec, None)


@parametrize('enc_dec', enc_dec_imp)
@given(data=text(printable))
def test_BaseType_String(enc_dec, data):
  """ Test basic strings"""
  # we need to cast to str because text() returns unicode
  data = str(data)
  agnosticTestFunction(enc_dec, data)


@parametrize('enc_dec', enc_dec_imp)
@given(data=tuples(integers()))
def test_BaseType_Tuple(enc_dec, data):
  """ Test basic tuple """
  agnosticTestFunction(enc_dec, data)


@parametrize('enc_dec', enc_dec_imp)
@given(data=text())
def test_BaseType_Unicode(enc_dec, data):
  """ Test unicode data """
  agnosticTestFunction(enc_dec, data)


@parametrize('enc_dec', enc_dec_imp)
@given(data=nestedStrategy)
def test_nestedStructure(enc_dec, data):
  """ Test nested structure """
  agnosticTestFunction(enc_dec, data)
