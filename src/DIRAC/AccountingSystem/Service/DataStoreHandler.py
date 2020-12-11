""" DataStore is the service for inserting accounting reports (rows) in the Accounting DB

    This service CAN be duplicated iff the first is a "master" and all the others are slaves.
    See the information about :ref:`datastorehelpers`.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN DataStore
  :end-before: ##END
  :dedent: 2
  :caption: DataStore options
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import datetime
import six

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.AccountingSystem.DB.MultiAccountingDB import MultiAccountingDB
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.DISET.RPCClient import RPCClient

__RCSID__ = "$Id$"


class DataStoreHandler(RequestHandler):
  """ DISET implementation of service for inserting records in accountingDB.
  """

  __acDB = None

  @classmethod
  def initializeHandler(cls, svcInfoDict):
    multiPath = PathFinder.getDatabaseSection("Accounting/MultiDB")
    cls.__acDB = MultiAccountingDB(multiPath)
    # we can run multiple services in read only mode. In that case we do not bucket
    cls.runBucketing = getServiceOption(svcInfoDict, 'RunBucketing', True)
    if cls.runBucketing:
      cls.__acDB.autoCompactDB()  # pylint: disable=no-member
      result = cls.__acDB.markAllPendingRecordsAsNotTaken()  # pylint: disable=no-member
      if not result['OK']:
        return result
      gThreadScheduler.addPeriodicTask(60, cls.__acDB.loadPendingRecords)  # pylint: disable=no-member
    return S_OK()

  types_registerType = [six.string_types, list, list, list]

  def export_registerType(self, typeName, definitionKeyFields, definitionAccountingFields, bucketsLength):
    """
      Register a new type. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections("/DIRAC/Setups")
    if not retVal['OK']:
      return retVal
    errorsList = []
    for setup in retVal['Value']:
      retVal = self.__acDB.registerType(  # pylint: disable=no-member
          setup,
          typeName,
          definitionKeyFields,
          definitionAccountingFields,
          bucketsLength)
      if not retVal['OK']:
        errorsList.append(retVal['Message'])
    if errorsList:
      return S_ERROR("Error while registering type:\n %s" % "\n ".join(errorsList))
    return S_OK()

  types_setBucketsLength = [six.string_types, list]

  def export_setBucketsLength(self, typeName, bucketsLength):
    """
      Change the buckets Length. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections("/DIRAC/Setups")
    if not retVal['OK']:
      return retVal
    errorsList = []
    for setup in retVal['Value']:
      retVal = self.__acDB.changeBucketsLength(  # pylint: disable=no-member
          setup, typeName, bucketsLength)
      if not retVal['OK']:
        errorsList.append(retVal['Message'])
    if errorsList:
      return S_ERROR("Error while changing bucketsLength type:\n %s" % "\n ".join(errorsList))
    return S_OK()

  types_regenerateBuckets = [six.string_types]

  def export_regenerateBuckets(self, typeName):
    """
      Recalculate buckets. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections("/DIRAC/Setups")
    if not retVal['OK']:
      return retVal
    errorsList = []
    for setup in retVal['Value']:
      retVal = self.__acDB.regenerateBuckets(setup, typeName)  # pylint: disable=no-member
      if not retVal['OK']:
        errorsList.append(retVal['Message'])
    if errorsList:
      return S_ERROR("Error while recalculating buckets for type:\n %s" % "\n ".join(errorsList))
    return S_OK()

  types_getRegisteredTypes = []

  def export_getRegisteredTypes(self):
    """
      Get a list of registered types (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    return self.__acDB.getRegisteredTypes()  # pylint: disable=no-member

  types_deleteType = [six.string_types]

  def export_deleteType(self, typeName):
    """
      Delete accounting type and ALL its contents. VERY DANGEROUS! (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections("/DIRAC/Setups")
    if not retVal['OK']:
      return retVal
    errorsList = []
    for setup in retVal['Value']:
      retVal = self.__acDB.deleteType(setup, typeName)  # pylint: disable=too-many-function-args,no-member
      if not retVal['OK']:
        errorsList.append(retVal['Message'])
    if errorsList:
      return S_ERROR("Error while deleting type:\n %s" % "\n ".join(errorsList))
    return S_OK()

  types_commit = [six.string_types, datetime.datetime, datetime.datetime, list]

  def export_commit(self, typeName, startTime, endTime, valuesList):
    """
      Add a record for a type
    """
    setup = self.serviceInfoDict['clientSetup']
    startTime = int(Time.toEpoch(startTime))
    endTime = int(Time.toEpoch(endTime))
    return self.__acDB.insertRecordThroughQueue(  # pylint: disable=no-member
        setup,
        typeName,
        startTime,
        endTime,
        valuesList)

  types_commitRegisters = [list]

  def export_commitRegisters(self, entriesList):
    """
      Add a record for a type
    """
    setup = self.serviceInfoDict['clientSetup']
    expectedTypes = [six.string_types, datetime.datetime, datetime.datetime, list]
    for entry in entriesList:
      if len(entry) != 4:
        return S_ERROR("Invalid records")
      for i, _ in enumerate(entry):
        if not isinstance(entry[i], expectedTypes[i]):
          self.log.error("Unexpected type in report",
                         ": field %d in the records should be %s (and it is %s)" % (i, expectedTypes[i],
                                                                                    type(entry[i])))
          return S_ERROR("Unexpected type in report")
    records = []
    for entry in entriesList:
      startTime = int(Time.toEpoch(entry[1]))
      endTime = int(Time.toEpoch(entry[2]))
      self.log.debug("inserting", entry)
      records.append((setup, entry[0], startTime, endTime, entry[3]))
    return self.__acDB.insertRecordBundleThroughQueue(records)

  types_compactDB = []

  def export_compactDB(self):
    """
    Compact the db by grouping buckets
    """
    # if we are running slaves (not only one service) we can redirect the request to the master
    # For more information please read the Administrative guide Accounting part!
    # ADVICE: If you want to trigger the bucketing, please make sure the bucketing is not running!!!!
    if self.runBucketing:
      return self.__acDB.compactBuckets()  # pylint: disable=no-member

    return RPCClient('Accounting/DataStoreMaster').compactDB()

  types_remove = [six.string_types, datetime.datetime, datetime.datetime, list]

  def export_remove(self, typeName, startTime, endTime, valuesList):
    """
      Remove a record for a type
    """
    setup = self.serviceInfoDict['clientSetup']
    startTime = int(Time.toEpoch(startTime))
    endTime = int(Time.toEpoch(endTime))
    return self.__acDB.deleteRecord(  # pylint: disable=no-member
        setup,
        typeName,
        startTime,
        endTime,
        valuesList)

  types_removeRegisters = [list]

  def export_removeRegisters(self, entriesList):
    """
      Remove a record for a type
    """
    setup = self.serviceInfoDict['clientSetup']
    expectedTypes = [six.string_types, datetime.datetime, datetime.datetime, list]
    for entry in entriesList:
      if len(entry) != 4:
        return S_ERROR("Invalid records")
      for i in range(len(entry)):
        if not isinstance(entry[i], expectedTypes[i]):
          return S_ERROR("%s field in the records should be %s" % (i, expectedTypes[i]))
    ok = 0
    for entry in entriesList:
      startTime = int(Time.toEpoch(entry[1]))
      endTime = int(Time.toEpoch(entry[2]))
      record = entry[3]
      result = self.__acDB.deleteRecord(  # pylint: disable=no-member
          setup,
          entry[0],
          startTime,
          endTime,
          record)
      if not result['OK']:
        return S_OK(ok)
      ok += 1

    return S_OK(ok)
