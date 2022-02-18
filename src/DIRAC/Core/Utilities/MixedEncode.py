""" Transition methods to allow to move from DEncode to JEncode

"""
import os
from DIRAC.Core.Utilities import DEncode, JEncode


def encode(inData):
    """Encode the input data

    :param inData: data to be encoded

    :return: an encoded string
    """
    if os.getenv("DIRAC_USE_JSON_ENCODE", "NO").lower() in ("yes", "true"):
        return JEncode.encode(inData)
    return DEncode.encode(inData)


def decode(encodedData):
    """Decode the encoded string

    :param encodedData: encoded string

    :return: the decoded objects, encoded object length

    """
    if os.getenv("DIRAC_USE_JSON_DECODE", "Yes").lower() in ("yes", "true"):
        try:
            # 'null' is a special case.
            # None is encoded as 'null' as JSON
            # that DEncode would understand as None, since it starts with 'n'
            # but the length of the decoded string will not be correct
            if encodedData == "null":
                raise Exception
            return DEncode.decode(encodedData)
        except Exception:
            return JEncode.decode(encodedData)
    return DEncode.decode(encodedData)
