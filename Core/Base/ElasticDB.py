""" DB is a base class for multiple DIRAC databases that are based on MySQL.
    It uniforms the way the database objects are constructed
"""

__RCSID__ = "$Id$"

from DIRAC                                       import gLogger, gConfig
from DIRAC.Core.Utilities.ElasticSearchDB        import ElasticSearchDB
from DIRAC.ConfigurationSystem.Client.Utilities  import getElasticDBParameters

class ElasticDB( ElasticSearchDB ):

  def __init__( self, dbname, fullName, debug = False ):

    database_name = dbname
    self.log = gLogger.getSubLogger( database_name )

    result = getElasticDBParameters( fullName )
    if not result['OK'] :
      raise RuntimeError( 'Cannot get database parameters: %s' % result['Message'] )

    dbParameters = result[ 'Value' ]
    self.__dbHost = dbParameters[ 'Host' ]
    self.__dbPort = dbParameters[ 'Port' ]
    self.__dbName = dbParameters[ 'DBName' ]

    ElasticSearchDB.__init__( self, self.__dbHost, self.__dbPort, debug = debug )

    if not self._connected:
      raise RuntimeError( 'Can not connect to DB %s, exiting...' % self.dbName )


    self.log.info( "==================================================" )
    self.log.info( "Host:           " + self.__dbHost )
    self.log.info( "Port:           " + str( self.__dbPort ) )
    self.log.info( "DBName:         " + self.__dbName )
    self.log.info( "==================================================" )

