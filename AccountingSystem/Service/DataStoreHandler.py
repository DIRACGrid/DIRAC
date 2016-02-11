""" DataStore is the service for inserting accounting reports (rows) in the Accounting DB
"""

__RCSID__ = "$Id$"

import datetime

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.AccountingSystem.DB.AccountingDB import AccountingDB
from DIRAC.AccountingSystem.DB.MultiAccountingDB import MultiAccountingDB
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler

class DataStoreHandler( RequestHandler ):

  __acDB = AccountingDB()

  @classmethod
  def initializeHandler( cls, svcInfoDict ):
    multiPath = PathFinder.getDatabaseSection( "Accounting/MultiDB" )
    cls.__acDB = MultiAccountingDB( multiPath )
    cls.__acDB.autoCompactDB()
    result = cls.__acDB.markAllPendingRecordsAsNotTaken()
    if not result[ 'OK' ]:
      return result
    gThreadScheduler.addPeriodicTask( 60, cls.__acDB.loadPendingRecords )
    return S_OK()

  types_registerType = [ basestring, list, list, list ]
  def export_registerType( self, typeName, definitionKeyFields, definitionAccountingFields, bucketsLength ):
    """
      Register a new type. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections( "/DIRAC/Setups" )
    if not retVal[ 'OK' ]:
      return retVal
    errorsList = []
    for setup in retVal[ 'Value' ]:
      retVal = self.__acDB.registerType( setup, typeName, definitionKeyFields, definitionAccountingFields, bucketsLength ) #pylint: disable=E1121
      if not retVal[ 'OK' ]:
        errorsList.append( retVal[ 'Message' ] )
    if errorsList:
      return S_ERROR( "Error while registering type:\n %s" % "\n ".join( errorsList ) )
    return S_OK()

  types_setBucketsLength = [ basestring, list ]
  def export_setBucketsLength( self, typeName, bucketsLength ):
    """
      Change the buckets Length. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections( "/DIRAC/Setups" )
    if not retVal[ 'OK' ]:
      return retVal
    errorsList = []
    for setup in retVal[ 'Value' ]:
      retVal = self.__acDB.changeBucketsLength( setup, typeName, bucketsLength ) #pylint: disable=E1121
      if not retVal[ 'OK' ]:
        errorsList.append( retVal[ 'Message' ] )
    if errorsList:
      return S_ERROR( "Error while changing bucketsLength type:\n %s" % "\n ".join( errorsList ) )
    return S_OK()

  types_regenerateBuckets = [ basestring ]
  def export_regenerateBuckets( self, typeName ):
    """
      Recalculate buckets. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections( "/DIRAC/Setups" )
    if not retVal[ 'OK' ]:
      return retVal
    errorsList = []
    for setup in retVal[ 'Value' ]:
      retVal = self.__acDB.regenerateBuckets( setup, typeName ) #pylint: disable=E1121
      if not retVal[ 'OK' ]:
        errorsList.append( retVal[ 'Message' ] )
    if errorsList:
      return S_ERROR( "Error while recalculating buckets for type:\n %s" % "\n ".join( errorsList ) )
    return S_OK()

  types_getRegisteredTypes = []
  def export_getRegisteredTypes( self ):
    """
      Get a list of registered types (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    return self.__acDB.getRegisteredTypes()

  types_deleteType = [ basestring ]
  def export_deleteType( self, typeName ):
    """
      Delete accounting type and ALL its contents. VERY DANGEROUS! (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections( "/DIRAC/Setups" )
    if not retVal[ 'OK' ]:
      return retVal
    errorsList = []
    for setup in retVal[ 'Value' ]:
      retVal = self.__acDB.deleteType( setup, typeName ) #pylint: disable=E1121
      if not retVal[ 'OK' ]:
        errorsList.append( retVal[ 'Message' ] )
    if errorsList:
      return S_ERROR( "Error while deleting type:\n %s" % "\n ".join( errorsList ) )
    return S_OK()

  types_commit = [ basestring, datetime.datetime, datetime.datetime, list ]
  def export_commit( self, typeName, startTime, endTime, valuesList ):
    """
      Add a record for a type
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    startTime = int( Time.toEpoch( startTime ) )
    endTime = int( Time.toEpoch( endTime ) )
    return self.__acDB.insertRecordThroughQueue( setup, typeName, startTime, endTime, valuesList ) #pylint: disable=E1121

  types_commitRegisters = [ list ]
  def export_commitRegisters( self, entriesList ):
    """
      Add a record for a type
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    expectedTypes = [ basestring, datetime.datetime, datetime.datetime, list ]
    for entry in entriesList:
      if len( entry ) != 4:
        return S_ERROR( "Invalid records" )
      for i in range( len( entry ) ):
        if not isinstance(entry[i], expectedTypes[i]):
          gLogger.error( "Unexpected type in report",
                         ": field %d in the records should be %s (and it is %s)" % ( i, expectedTypes[i], type(entry[i])) )
          return S_ERROR( "Unexpected type in report" )
    records = []
    for entry in entriesList:
      startTime = int( Time.toEpoch( entry[1] ) )
      endTime = int( Time.toEpoch( entry[2] ) )
      records.append( ( setup, entry[0], startTime, endTime, entry[3] ) )
    return self.__acDB.insertRecordBundleThroughQueue( records )


  types_compactDB = []
  def export_compactDB( self ):
    """
    Compact the db by grouping buckets
    """
    return self.__acDB.compactBuckets()

  types_remove = [ basestring, datetime.datetime, datetime.datetime, list ]
  def export_remove( self, typeName, startTime, endTime, valuesList ):
    """
      Remove a record for a type
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    startTime = int( Time.toEpoch( startTime ) )
    endTime = int( Time.toEpoch( endTime ) )
    return self.__acDB.deleteRecord( setup, typeName, startTime, endTime, valuesList ) #pylint: disable=E1121

  types_removeRegisters = [ list ]
  def export_removeRegisters( self, entriesList ):
    """
      Remove a record for a type
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    expectedTypes = [ basestring, datetime.datetime, datetime.datetime, list ]
    for entry in entriesList:
      if len( entry ) != 4:
        return S_ERROR( "Invalid records" )
      for i in range( len( entry ) ):
        if not isinstance( entry[i], expectedTypes[i] ):
          return S_ERROR( "%s field in the records should be %s" % ( i, expectedTypes[i] ) )
    ok = 0
    for entry in entriesList:
      startTime = int( Time.toEpoch( entry[1] ) )
      endTime = int( Time.toEpoch( entry[2] ) )
      record = entry[3]
      result = self.__acDB.deleteRecord( setup, entry[0], startTime, endTime, record ) #pylint: disable=E1121
      if not result[ 'OK' ]:
        return S_OK( ok )
      ok += 1

    return S_OK( ok )
