=============
Serialization
=============

The serialization mechanism currently used in DIRAC is called DEncode. It is a custom serialization mechanism.

The aim in the medium term is to replace it with standard JSON serialization.

We will describe here these two.

*******
DEncode
*******

DEncode (:py:mod:`~DIRAC.Core.Utilities.DEncode`) contains two functions:

  * encode: returns the string representation of the input.
  * decode: returns the decoded information from the string input, as well as the length of the information decoded.


.. code-block:: python

  from DIRAC.Core.Utilities.DEncode import encode, decode

  myData = {'a' : [1,2,3], 2 : 'toto' }

  # Encode the structure
  myEncodedData = encode(myData)

  # myEncodedData is the string 'di2es4:totos1:ali1ei2ei3eee'

  # Decode the data back
  decode(myEncodedData)

  # returns a tuple containing the decoded data
  # and the length decoded
  # ({2: 'toto', 'a': [1, 2, 3]}, 27)

DEncode supports the following type:

  * boolean
  * datetime
  * dict
  * int
  * float (CAUTION, see below)
  * list
  * long
  * none
  * string
  * tuple
  * unicode

It is a know fact that DEncode is not stable for floats:

.. code-block:: python

  from DIRAC.Core.Utilities.DEncode import encode, decode

  d = 133143986190.0

  import sys

  sys.maxint > d
  # True

  encode(d)
  # 'f1.3314398619e+11e'

  decode(encode(d))
  # (133143986190.00002, 18)

Notice that 133143986190.0 != 133143986190.00002


*******
JEncode
*******

.. warning:: This serialization is not in use yet

JEncode (:py:mod:`~DIRAC.Core.Utilities.JEncode`) is based on JSON, but exposes the same interface as DEncode, that is an `encode` and a `decode` functions.

However, because of the nature of JSON (https://tools.ietf.org/html/rfc7159), there are some limitations and changes with respect to DEncode:

  * all UTF-8 by default. Non default would be other UTF encoding
  * Tuples are converted to arrays
  * the keys of dictionaries are always strings. This means that any other type of key will be cast to a string (including numbers !). As a consequence, it is up to the sender/receiver to cast that in whatever type is desired.

JEncode contains a special serializer and deserializer which enhance the default one with:

  * Support for datetime: the serialization format is hardcoded and corresponds to `%Y-%m-%d %H:%M:%S` (see https://docs.python.org/2/library/datetime.html#strftime-strptime-behavior). This means that milliseconds are not kept. Note as well that only dates starting after 01-01-1900 are serializable.
  * Support for custom object serialization inheriting from `JSerializable` (:py:class:`~DIRAC.Core.Utilities.JEncode.JSerializable`). See the Code documentation for more details on the restrinctions and how to use it.
