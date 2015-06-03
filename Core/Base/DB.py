""" DB is a base class for multiple DIRAC databases that are based on MySQL.
    It uniforms the way the database objects are constructed
"""

__RCSID__ = "$Id$"

from DIRAC                                       import gLogger, gConfig
from DIRAC.Core.Utilities.MySQL                  import MySQL
from DIRAC.ConfigurationSystem.Client.Utilities  import getDBParameters
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection

class DB( MySQL ):

  def __init__( self, dbname, fullname, debug = False ):

    self.fullname = fullname
    database_name = dbname
    self.log = gLogger.getSubLogger( database_name )

    result = getDBParameters( fullname )
    if not result['OK'] :
      raise RuntimeError( 'Cannot get database parameters: %s' % result['Message'] )

    dbParameters = result[ 'Value' ]
    self.dbHost = dbParameters[ 'Host' ]
    self.dbPort = dbParameters[ 'Port' ]
    self.dbUser = dbParameters[ 'User' ]
    self.dbPass = dbParameters[ 'Password' ]
    self.dbName = dbParameters[ 'DBName' ]

    MySQL.__init__( self, self.dbHost, self.dbUser, self.dbPass,
                   self.dbName, self.dbPort, debug = debug )

    if not self._connected:
      raise RuntimeError( 'Can not connect to DB %s, exiting...' % self.dbName )


    self.log.info( "==================================================" )
    #self.log.info("SystemInstance: "+self.system)
    self.log.info( "User:           " + self.dbUser )
    self.log.info( "Host:           " + self.dbHost )
    self.log.info( "Port:           " + str( self.dbPort ) )
    #self.log.info("Password:       "+self.dbPass)
    self.log.info( "DBName:         " + self.dbName )
    self.log.info( "==================================================" )

#############################################################################
  def getCSOption( self, optionName, defaultValue = None ):
    cs_path = getDatabaseSection( self.fullname )
    return gConfig.getValue( "/%s/%s" % ( cs_path, optionName ), defaultValue )
