########################################################################
# File: File.py
# Date: 2012/08/03 15:02:53
########################################################################
"""
:mod: File

.. module: File
  :synopsis: RMS operation file

operation file
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
# Disable invalid names warning
# pylint: disable=invalid-name

__RCSID__ = "$Id$"

# # imports
import datetime
import os
import json
import six

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.File import checkGuid
from DIRAC.RequestManagementSystem.private.JSONUtils import RMSEncoder


########################################################################
class File(object):

  """
   A bag object holding Operation file attributes.

  :param Operation.Operation _parent: reference to parent Operation
  :param dict __data__: attrs dict


  It is managed by SQLAlchemy, so the OperationID, FileID should never be set by hand
  (except when constructed from JSON of course...)
  In principle, the _parent attribute could be totally managed by SQLAlchemy. However, it is
  set only when inserted into the DB, this is why I manually set it in the Operation

  .. warning::

    You cannot add a File object to multiple Operations. They are different entry in the DB, so they must be different
    objects

  """

  _datetimeFormat = '%Y-%m-%d %H:%M:%S'

  def __init__(self, fromDict=None):
    """c'tor

    :param self: self reference
    :param dict fromDict: property dict
    """
    self._parent = None
    self._Status = 'Waiting'
    self._LFN = None
    self.PFN = None
    self._ChecksumType = None
    self.Checksum = None
    self._GUID = None
    self.Attempt = 0
    self.Size = 0
    self.Error = None
    self._duration = 0

    # This variable is used in the setter to know whether they are called
    # because of the json initialization or not
    self.initialLoading = True

    fromDict = fromDict if isinstance(fromDict, dict)\
        else json.loads(fromDict) if isinstance(fromDict, six.string_types)\
        else {}

    for attrName, attrValue in fromDict.items():
      # The JSON module forces the use of UTF-8, which is not properly
      # taken into account in DIRAC.
      # One would need to replace all the '== str' with 'in six.string_types'
      # This is converting `unicode` to `str` and doesn't make sense in Python 3
      if six.PY2 and isinstance(attrValue, six.string_types):
        attrValue = attrValue.encode()
      if attrValue:
        setattr(self, attrName, attrValue)

    self.initialLoading = False

  @property
  def LFN(self):
    """ LFN prop """
    return self._LFN

  @LFN.setter
  def LFN(self, value):
    """ lfn setter """
    if not isinstance(value, six.string_types):
      raise TypeError("LFN has to be a string!")
    if not os.path.isabs(value):
      raise ValueError("LFN should be an absolute path!")
    self._LFN = value

  @property
  def GUID(self):
    """ GUID prop """
    return self._GUID

  @GUID.setter
  def GUID(self, value):
    """ GUID setter """
    if value:
      if not isinstance(value, six.string_types):
        raise TypeError("GUID should be a string!")
      if not checkGuid(value):
        raise ValueError("'%s' is not a valid GUID!" % str(value))
    self._GUID = value

  @property
  def ChecksumType(self):
    """ checksum type prop """
    return self._ChecksumType

  @ChecksumType.setter
  def ChecksumType(self, value):
    """ checksum type setter """
    if not value:
      self._ChecksumType = ""
    elif value and str(value).strip().upper() not in ("ADLER32", "MD5", "SHA1"):
      if str(value).strip().upper() == 'AD':
        self._ChecksumType = 'ADLER32'
      else:
        raise ValueError("unknown checksum type: %s" % value)
    else:
      self._ChecksumType = str(value).strip().upper()

  @property
  def Status(self):
    """ status prop """
    if not self._Status:
      self._Status = 'Waiting'
    return self._Status

  @Status.setter
  def Status(self, value):
    """ status setter """
    if value not in ("Waiting", "Failed", "Done", "Scheduled"):
      raise ValueError("Unknown Status: %s!" % str(value))

    if value == 'Done':
      self.Error = ''

    updateTime = (self._Status != value)
    if updateTime and self._parent:
      self._parent.LastUpdate = datetime.datetime.utcnow().replace(microsecond=0)

    self._Status = value

    if self._parent:
      self._parent._notify()

  def __str__(self):
    """ str operator """
    return self.toJSON()['Value']

  def toJSON(self):
    """ Returns the json formated string that describes the File """
    try:
      jsonStr = json.dumps(self, cls=RMSEncoder)
      return S_OK(jsonStr)
    except Exception as e:
      return S_ERROR(str(e))

  def _getJSONData(self):
    """ Returns the data that have to be serialized by JSON """
    attrNames = ['FileID', 'OperationID', "Status", "LFN",
                 "PFN", "ChecksumType", "Checksum", "GUID", "Attempt",
                 "Size", "Error"]

    jsonData = {}

    for attrName in attrNames:

      # FileID and OperationID might not be set since they are managed by SQLAlchemy
      if not hasattr(self, attrName):
        continue

      jsonData[attrName] = getattr(self, attrName)
      value = getattr(self, attrName)

      if isinstance(value, datetime.datetime):
        # We convert date time to a string
        jsonData[attrName] = value.strftime(self._datetimeFormat)  # pylint: disable=no-member
      else:
        jsonData[attrName] = value

    return jsonData
