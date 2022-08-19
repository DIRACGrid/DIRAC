""" DIRAC Encoding utilities based on json
"""
from base64 import b64encode, b64decode
import datetime
import json


# Describes the way date time will be transmetted
# We do not keep miliseconds
DATETIME_DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S"
DATETIME_DEFAULT_DATE_FORMAT = "%Y-%m-%d"


class JSerializable:
    """
    Base class to define a serializable object by DIRAC.

    An object that ought to be serialized throught DISET shoud:
      * inherit from this class
      * define the _attrToSerialize list as class member. It is a list of
        strings containing the name of the attributes that should be serialized
      * have a constructor that takes no arguments, or only keywords arguments

    Exemple:

      class Serializable(JSerializable):

        _attrToSerialize = ['myAttr']

        def __init__(self, myAttr = None):
          self.myAttr = myAttr

    Limitations:
      * This will not work for classes defined inside classes. The class definition shoud be
        visible from the global scope
      * Class attributes cannot be serialized as such. They are converted to instance attributes.


    """

    def _toJSON(self):
        """Translates the objct into a dictionary.
        It is meant to be called by JSONDecoder only.

        It relies on the attribute _attrToSerialize to know which attributes to
        serialize.

        The returned dictionary contains the attributes serialized as well as
        hints for reconstructing the object upon receive.

        :raises TypeError: If the object is not serializable (no _attrToSerialize defined)

        :returns: a dictionary representing the object


        """

        # If the object does not have _attrToSerialize defined
        # Raise TypeError
        if not hasattr(self, "_attrToSerialize"):
            raise TypeError("Object not serializable. _attrToSerialize not defined")

        jsonData = {}

        # Store the class name and the module name
        jsonData["__dCls"] = self.__class__.__name__
        jsonData["__dMod"] = self.__module__

        # self._attrToSerialize will be defined by the child class
        for attr in self._attrToSerialize:  # pylint: disable=no-member
            # If an argument to serialize is not defined,
            # we continue. This is handy for arguments that
            # are defined dynamicaly ,like SQLAlchemy does.
            if not hasattr(self, attr):
                continue
            attrValue = getattr(self, attr)
            if attrValue is not None:
                jsonData[attr] = attrValue

        return jsonData


class DJSONEncoder(json.JSONEncoder):
    """This custom encoder is to add support to json for
    tuple, datetime, and any object inheriting from JSerializable
    """

    def default(self, obj):  # pylint: disable=method-hidden
        """Add supports for datetime and JSerializable class to default json

        :param obj: object to serialize

        :return: json string of the serialized objects

        """

        # If we have a datetime object, dumps its string representation
        if isinstance(obj, datetime.datetime):
            return {"__dCls": "dt", "obj": obj.strftime(DATETIME_DEFAULT_FORMAT)}
        elif isinstance(obj, datetime.date):
            return {"__dCls": "date", "obj": obj.strftime(DATETIME_DEFAULT_DATE_FORMAT)}
        # if the object inherits from JSJerializable, try to serialize it
        elif isinstance(obj, JSerializable):
            return obj._toJSON()  # pylint: disable=protected-access
        elif isinstance(obj, bytes):
            return {"__dCls": "b64", "obj": b64encode(obj).decode()}
        # otherwise, let the parent do
        return super().default(obj)


class DJSONDecoder(json.JSONDecoder):
    """This custom decoder is to add support to json for
    tuple, datetime, and any object inheriting from JSerializable
    """

    def __init__(self, *args, **kargs):
        """
        Init method needed in order to give the object_hook to have special
        deserialization method.

        """
        super().__init__(object_hook=self.dict_to_object, *args, **kargs)

    @staticmethod
    def dict_to_object(dataDict):
        """Convert the dictionary into an object.
        Adds deserialization support for datetype and JSerializable

        :param dataDict: json dictionary representing the data

        :returns: deserialized object
        """

        className = dataDict.pop("__dCls", None)

        # If the class is of type dt (datetime)
        if className == "dt":
            return datetime.datetime.strptime(dataDict["obj"], DATETIME_DEFAULT_FORMAT)
        elif className == "date":
            return datetime.datetime.strptime(dataDict["obj"], DATETIME_DEFAULT_DATE_FORMAT).date()
        elif className == "b64":
            return b64decode(dataDict["obj"])
        elif className:
            import importlib

            # Get the module
            modName = dataDict.pop("__dMod")

            # Load the module
            mod = importlib.import_module(modName)
            # import the class
            cl = getattr(mod, className)

            # Check that cl is a subclass of JSerializable,
            # and that we are not putting ourselves in trouble...
            if not (isinstance(cl, type) and issubclass(cl, JSerializable)):
                raise TypeError("Only subclasses of JSerializable can be decoded")

            # Instantiate the object
            obj = cl()

            # Set each attribute
            for attrName, attrValue in dataDict.items():
                # If the value is None, do not set it
                # This is needed to play along well with SQLalchemy
                if attrValue is None:
                    continue

                setattr(obj, attrName, attrValue)

            return obj

        # If we do not know how to serialize, just return the dictionary
        return dataDict


def encode(inData):
    """Encode the input data into a JSON string

    :param inData: anything that can be serialized.
                   Namely, anything that can be serialized by standard json package,
                   datetime object, tuples,  and any class that inherits from JSerializable

    :return: a json string
    """
    return json.dumps(inData, cls=DJSONEncoder)


def decode(encodedData):
    """Decode the json encoded string

    :param encodedData: json encoded string

    :return: the decoded objects, encoded object length

    Arguably, the length of the encodedData is useless,
    but it is for compatibility
    """
    return json.loads(encodedData, cls=DJSONDecoder), len(encodedData)


def strToIntDict(inDict):
    """Because JSON will transform dict with int keys to str keys,
    this utility method is just to cast it back.
    This shows useful in cases when sending dict indexed on
    jobID or requestID for example

    :param inDict: dictionary with strings as keys e.g. {'1': 1, '2': 2}

    :returns: dictionary with int as keys e.g. {1: 1, 2: 2}
    """
    return {int(key): value for key, value in inDict.items()}


def strToFloatDict(inDict):
    """Because JSON will transform dict with float keys to str keys,
    this utility method is just to cast it back.

    :param inDict: dictionary with strings as keys e.g. {'1.0': 1, '2.1': 2}

    :returns: dictionary with float as keys e.g. {1.0: 1, 2.1: 2}
    """
    return {float(key): value for key, value in inDict.items()}
