""" DB is a base class for multiple DIRAC databases that are based on MySQL.
    It uniforms the way the database objects are constructed
"""

__RCSID__ = "$Id$"

from DIRAC                                       import gLogger, gConfig
from DIRAC.Core.Utilities.MySQL                  import MySQL
from DIRAC.ConfigurationSystem.Client.Utilities  import getDBParameters
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection

class DB( MySQL ):

  def __init__( self, dbname, fullname, maxQueueSize, debug = False ):

    self.fullname = fullname
    database_name = dbname
    self.log = gLogger.getSubLogger( database_name )

    result = getDBParameters( fullname, defaultQueueSize = maxQueueSize )
    if( not result[ 'OK' ] ):
      raise Exception \
                  ( 'Cannot get the Database parameters' % result( 'Message' ) )

    dbParameters = result[ 'Value' ]
    self.dbHost = dbParameters[ 'host' ]
    self.dbPort = dbParameters[ 'port' ]
    self.dbUser = dbParameters[ 'user' ]
    self.dbPass = dbParameters[ 'password' ]
    self.dbName = dbParameters[ 'db' ]
    self.maxQueueSize = dbParameters[ 'queueSize' ]

    MySQL.__init__( self, self.dbHost, self.dbUser, self.dbPass,
                   self.dbName, self.dbPort, maxQueueSize = maxQueueSize, debug = debug )

    if not self._connected:
      raise RuntimeError( 'Can not connect to DB %s, exiting...' % self.dbName )


    self.log.info( "==================================================" )
    #self.log.info("SystemInstance: "+self.system)
    self.log.info( "User:           " + self.dbUser )
    self.log.info( "Host:           " + self.dbHost )
    self.log.info( "Port:           " + str( self.dbPort ) )
    #self.log.info("Password:       "+self.dbPass)
    self.log.info( "DBName:         " + self.dbName )
    self.log.info( "MaxQueue:       " + str( self.maxQueueSize ) )
    self.log.info( "==================================================" )

#############################################################################
  def getCSOption( self, optionName, defaultValue = None ):
    cs_path = getDatabaseSection( self.fullname )
    return gConfig.getValue( "/%s/%s" % ( cs_path, optionName ), defaultValue )
