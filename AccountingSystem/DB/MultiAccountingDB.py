# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC import gConfig, S_OK, gLogger
from DIRAC.AccountingSystem.DB.AccountingDB import AccountingDB
from DIRAC.AccountingSystem.private.TypeLoader import TypeLoader

class MultiAccountingDB( object ):

  def __init__( self, csPath, maxQueueSize = 10, readOnly = False ):
    self.__csPath = "%s/TypeDB" % csPath
    self.__readOnly = readOnly
    self.__maxQueueSize = maxQueueSize
    self.__dbByType = {}
    self.__defaultDB = 'AccountingDB/AccountingDB'
    self.__log = gLogger.getSubLogger( "MultiAccDB" )
    self.__generateDBs()
    self.__registerMethods()

  def __generateDBs( self ):
    self.__log.notice( "Creating default AccountingDB..." )
    self.__allDBs = { self.__defaultDB: AccountingDB( maxQueueSize = self.__maxQueueSize, readOnly = self.__readOnly ) }
    types = self.__allDBs[ self.__defaultDB ].getRegisteredTypes()
    result = gConfig.getOptionsDict( self.__csPath )
    if not result[ 'OK' ]:
      gLogger.verbose( "No extra databases defined in %s" % self.__csPath )
      return
    validTypes = TypeLoader().getTypes()
    opts = result[ 'Value' ]
    for acType in opts:
      if acType not in validTypes:
        msg =  "Oops... %s defined in %s is not a known accounting type" % ( acType, self.__csPath )
        self.__log.fatal( msg )
        raise RuntimeError( msg )
      dbName = opts[ acType ]
      gLogger.notice( "%s type will be assigned to %s" % ( acType, dbName ) )
      if dbName not in self.__allDBs:
        fields = dbName.split( "/" )
        if len( fields ) == 1:
          dbName = "Accounting/%s" % dbName
        gLogger.notice( "Creating DB %s" % dbName )
        self.__allDBs[ dbName ] = AccountingDB( dbName, maxQueueSize = self.__maxQueueSize, readOnly = self.__readOnly )
      self.__dbByType[ acType ] = self.__allDBs[ dbName ]

  def __registerMethods( self ):
    for methodName in ( 'registerType', 'changeBucketsLength', 'regenerateBuckets',
                        'deleteType', 'insertRecordThroughQueue', 'deleteRecord',
                        'getKeyValues', 'retrieveBucketedData', 'calculateBuckets',
                        'calculateBucketLengthForTime' ):
      setattr( self, methodName, lambda *x: self.__mimeTypeMethod( methodName, *x ) )
    for methodName in ( 'autoCompactDB', 'compactBuckets', 'markAllPendingRecordsAsNotTaken',
                        'loadPendingRecords', 'getRegisteredTypes' ):
      setattr( self, methodName, lambda *x: self.__mimeMethod( methodName, *x ) )

  def __mimeTypeMethod( self, methodName, setup, acType, *args ):
    return getattr( self.__db( acType ), methodName )( "%s_%s" % ( setup, acType ), *args )

  def __mimeMethod( self, methodName, *args ):
    end = S_OK()
    for dbName in self.__allDBs:
      res = getattr( self.__allDBs[ dbName ], methodName )( *args )
      if res and not res[ 'OK' ]:
        end = res
    return end

  def __db( acType ):
    return self.__allDBs[ self.__dbByType.get( acType, self.__defaultDB ) ]

