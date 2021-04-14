########################################################################
# File: Operation.py
# Date: 2012/07/24 12:12:05
########################################################################

"""
:mod: Operation

.. module: Operation
  :synopsis: Operation implementation

Operation implementation
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
# Disable invalid names warning
# pylint: disable=invalid-name
__RCSID__ = "$Id$"

import datetime
import json
import six
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.private.JSONUtils import RMSEncoder


########################################################################
class Operation(object):
  """
  :param int OperationID: OperationID as read from DB backend
  :param int RequestID: parent RequestID
  :param str Status: execution status
  :param str Type: operation to perform
  :param str Arguments: additional arguments
  :param str SourceSE: source SE name
  :param str TargetSE: target SE names as comma separated list
  :param str Catalog: catalog to use as comma separated list
  :param str Error: error string if any
  :param Request.Request parent: parent Request instance


  It is managed by SQLAlchemy, so the RequestID, OperationID should never be set by hand
  (except when constructed from JSON of course...)
  In principle, the _parent attribute could be totally managed by SQLAlchemy. However, it is
  set only when inserted into the DB, this is why I manually set it in the Request _notify

  """
  # # max files in a single operation
  MAX_FILES = 10000

  # # all states
  ALL_STATES = ("Queued", "Waiting", "Scheduled", "Assigned", "Failed", "Done", "Canceled")
  # # final states
  FINAL_STATES = ("Failed", "Done", "Canceled")
  # # valid attributes
  ATTRIBUTE_NAMES = ['OperationID', 'RequestID', "Type", "Status", "Arguments",
                     "Order", "SourceSE", "TargetSE", "Catalog", "Error",
                     "CreationTime", "SubmitTime", "LastUpdate"]

  _datetimeFormat = '%Y-%m-%d %H:%M:%S'

  def __init__(self, fromDict=None):
    """ c'tor

    :param self: self reference
    :param dict fromDict: attributes dictionary
    """
    self._parent = None

    now = datetime.datetime.utcnow().replace(microsecond=0)
    self._SubmitTime = now
    self._LastUpdate = now
    self._CreationTime = now

    self._Status = "Queued"
    self._Order = 0
    self.__files__ = []

    self.TargetSE = None
    self.SourceSE = None
    self._Arguments = None
    self.Error = None
    self.Type = None
    self._Catalog = None

    if isinstance(fromDict, six.string_types):
      fromDict = json.loads(fromDict)
    elif not isinstance(fromDict, dict):
      fromDict = {}

    if "Files" in fromDict:
      for fileDict in fromDict.get("Files", []):
        self.addFile(File(fileDict))

      del fromDict["Files"]

    for key, value in fromDict.items():
      # The JSON module forces the use of UTF-8, which is not properly
      # taken into account in DIRAC.
      # One would need to replace all the '== str' with 'in six.string_types'
      # This is converting `unicode` to `str` and doesn't make sense in Python 3
      if six.PY2 and isinstance(value, six.string_types):
        value = value.encode()
      if value:
        setattr(self, key, value)

  # # protected methods for parent only
  def _notify(self):
    """ notify self about file status change """
    fStatus = set(self.fileStatusList())
    if fStatus == set(['Failed']):
      # All files Failed -> Failed
      newStatus = 'Failed'
    elif 'Scheduled' in fStatus:
      newStatus = 'Scheduled'
    elif "Waiting" in fStatus:
      newStatus = 'Queued'
    elif 'Failed' in fStatus:
      newStatus = 'Failed'
    else:
      self.Error = ''
      newStatus = 'Done'

    # If the status moved to Failed or Done, update the lastUpdate time
    if newStatus in ('Failed', 'Done', 'Scheduled'):
      if self._Status != newStatus:
        self._LastUpdate = datetime.datetime.utcnow().replace(microsecond=0)

    self._Status = newStatus
    if self._parent:
      self._parent._notify()

  def _setQueued(self, caller):
    """ don't touch """
    if caller == self._parent:
      self._Status = "Queued"

  def _setWaiting(self, caller):
    """ don't touch as well """
    if caller == self._parent:
      self._Status = "Waiting"

  # # Files arithmetics
  def __contains__(self, opFile):
    """ in operator """
    return opFile in self.__files__

  def __iadd__(self, opFile):
    """ += operator """
    if len(self) >= Operation.MAX_FILES:
      raise RuntimeError("too many Files in a single Operation")
    self.addFile(opFile)
    return self

  def addFile(self, opFile):
    """ add :opFile: to operation

        .. warning::

          You cannot add a File object that has already been added to another operation. They must be different objects
    """
    if len(self) >= Operation.MAX_FILES:
      raise RuntimeError("too many Files in a single Operation")
    if opFile not in self:
      self.__files__.append(opFile)
      opFile._parent = self
    self._notify()

  # # helpers for looping
  def __iter__(self):
    """ files iterator """
    return self.__files__.__iter__()

  def __getitem__(self, i):
    """ [] op for opFiles """
    return self.__files__.__getitem__(i)

  def __delitem__(self, i):
    """ remove file from op, only if OperationID is NOT set """
    self.__files__.__delitem__(i)
    self._notify()

  def __setitem__(self, i, opFile):
    """ overwrite opFile """
    self.__files__.__setitem__(i, opFile)
    opFile._parent = self
    self._notify()

  def fileStatusList(self):
    """ get list of files statuses """
    return [subFile.Status for subFile in self]

  def __bool__(self):
    """ for comparisons
    """
    return True

  # For Python 2 compatibility
  __nonzero__ = __bool__

  def __len__(self):
    """ nb of subFiles """
    return len(self.__files__)

  @property
  def sourceSEList(self):
    """ helper property returning source SEs as a list"""
    return self.SourceSE.split(",") if self.SourceSE else ['']

  @property
  def targetSEList(self):
    """ helper property returning target SEs as a list"""
    return self.TargetSE.split(",") if self.TargetSE else ['']

  @property
  def Arguments(self):
    return self._Arguments

  @Arguments.setter
  def Arguments(self, value):
    if isinstance(value, six.text_type):
      value = value.encode()
    if not isinstance(value, bytes):
      raise TypeError("Arguments should be bytes!")
    self._Arguments = value

  @property
  def Catalog(self):
    """ catalog prop """
    return self._Catalog

  @Catalog.setter
  def Catalog(self, value):
    """ catalog setter """
    if not isinstance(value, six.string_types + (list,)):
      raise TypeError("wrong type for value")
    if isinstance(value, six.string_types):
      value = value.split(',')

    value = ",".join(list(set([str(item).strip() for item in value if str(item).strip()])))

    if len(value) > 255:
      raise ValueError("Catalog list too long")
    self._Catalog = value if value else ""

  @property
  def catalogList(self):
    """ helper property returning catalogs as list """
    return self._Catalog.split(",") if self._Catalog else []

  @property
  def Status(self):
    """ Status prop """
    return self._Status

  @Status.setter
  def Status(self, value):
    """ Status setter """
    if value not in Operation.ALL_STATES:
      raise ValueError("unknown Status '%s'" % str(value))
    if self.__files__:
      self._notify()
    else:
      # If the status moved to Failed or Done, update the lastUpdate time
      if value in ('Failed', 'Done'):
        if self._Status != value:
          self._LastUpdate = datetime.datetime.utcnow().replace(microsecond=0)

      self._Status = value
      if self._parent:
        self._parent._notify()
    if self._Status == 'Done':
      self.Error = ''

  @property
  def Order(self):
    """ order prop """
    if self._parent:
      self._Order = self._parent.indexOf(self) if self._parent else -1
    return self._Order

  @Order.setter
  def Order(self, value):
    """ order prop """
    self._Order = value

  @property
  def CreationTime(self):
    """ operation creation time prop """
    return self._CreationTime

  @CreationTime.setter
  def CreationTime(self, value=None):
    """ creation time setter """
    if not isinstance(value, (datetime.datetime,) + six.string_types):
      raise TypeError("CreationTime should be a datetime.datetime!")
    if isinstance(value, six.string_types):
      value = datetime.datetime.strptime(value.split(".")[0], self._datetimeFormat)
    self._CreationTime = value

  @property
  def SubmitTime(self):
    """ subrequest's submit time prop """
    return self._SubmitTime

  @SubmitTime.setter
  def SubmitTime(self, value=None):
    """ submit time setter """
    if not isinstance(value, (datetime.datetime,) + six.string_types):
      raise TypeError("SubmitTime should be a datetime.datetime!")
    if isinstance(value, six.string_types):
      value = datetime.datetime.strptime(value.split(".")[0], self._datetimeFormat)
    self._SubmitTime = value

  @property
  def LastUpdate(self):
    """ last update prop """
    return self._LastUpdate

  @LastUpdate.setter
  def LastUpdate(self, value=None):
    """ last update setter """
    if not isinstance(value, (datetime.datetime,) + six.string_types):
      raise TypeError("LastUpdate should be a datetime.datetime!")
    if isinstance(value, six.string_types):
      value = datetime.datetime.strptime(value.split(".")[0], self._datetimeFormat)
    self._LastUpdate = value
    if self._parent:
      self._parent.LastUpdate = value

  def __str__(self):
    """ str operator """
    return self.toJSON()['Value']

  def toJSON(self):
    """ Returns the JSON description string of the Operation """
    try:
      jsonStr = json.dumps(self, cls=RMSEncoder)
      return S_OK(jsonStr)
    except Exception as e:
      return S_ERROR(str(e))

  def _getJSONData(self):
    """ Returns the data that have to be serialized by JSON """

    jsonData = {}

    for attrName in Operation.ATTRIBUTE_NAMES:

      # RequestID and OperationID might not be set since they are managed by SQLAlchemy
      if not hasattr(self, attrName):
        continue

      value = getattr(self, attrName)

      if isinstance(value, datetime.datetime):
        # We convert date time to a string
        jsonData[attrName] = value.strftime(self._datetimeFormat)  # pylint: disable=no-member
      else:
        jsonData[attrName] = value

    jsonData['Files'] = self.__files__

    return jsonData
