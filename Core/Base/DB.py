########################################################################
# $HeadURL$
########################################################################

""" BaseDB is the base class for multiple DIRAC databases. It uniforms the
    way how the database objects are constructed
"""

__RCSID__ = "$Id$"

import sys, types, socket
from DIRAC                           import gLogger, gConfig, S_OK
from DIRAC.Core.Utilities.MySQL      import MySQL
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection


########################################################################
class DB( MySQL ):

  @classmethod
  def getDBParameters( cls, fullname, maxQueueSize ):
    """
    Retrieve Database parameters from CS
    fullname should be of the form <System>/<DBname>
    maxQueueSize is the default value to give to maxQueueSize if it is not 
    available in the CS
    """

    fullname = fullname
    cs_path = getDatabaseSection( fullname )

    dbHost = ''
    result = gConfig.getOption( cs_path + '/Host' )
    if not result['OK']:
      raise RuntimeError( 'Failed to get the configuration parameters: Host' )
    dbHost = result['Value']
    # Check if the host is the local one and then set it to 'localhost' to use
    # a socket connection
    if dbHost != 'localhost':
      localHostName = socket.getfqdn()
      if localHostName == dbHost:
        dbHost = 'localhost'

    dbPort = 3306
    result = gConfig.getOption( cs_path + '/Port' )
    if not result['OK']:
      # No individual port number found, try at the common place
      result = gConfig.getOption( '/Systems/Databases/Port' )
      if result['OK']:
        dbPort = int( result['Value'] )
    else:
      dbPort = int( result['Value'] )

    dbUser = ''
    result = gConfig.getOption( cs_path + '/User' )
    if not result['OK']:
      # No individual user name found, try at the common place
      result = gConfig.getOption( '/Systems/Databases/User' )
      if not result['OK']:
        raise RuntimeError( 'Failed to get the configuration parameters: User' )
    dbUser = result['Value']

    dbPass = ''
    result = gConfig.getOption( cs_path + '/Password' )
    if not result['OK']:
      # No individual password found, try at the common place
      result = gConfig.getOption( '/Systems/Databases/Password' )
      if not result['OK']:
        raise RuntimeError \
                      ( 'Failed to get the configuration parameters: Password' )
    dbPass = result['Value']

    dbName = ''
    result = gConfig.getOption( cs_path + '/DBName' )
    if not result['OK']:
      raise RuntimeError( 'Failed to get the configuration parameters: DBName' )
    dbName = result['Value']

    qSize = maxQueueSize
    result = gConfig.getOption( cs_path + '/MaxQueueSize' )
    if result['OK']:
      qSize = int( result['Value'] )

    return S_OK( [ dbHost, dbPort, dbUser, dbPass, dbName, qSize ] )

  def __init__( self, dbname, fullname, maxQueueSize, debug = False ):

    database_name = dbname
    self.log = gLogger.getSubLogger( database_name )

    result = DB.getDBParameters( fullname, maxQueueSize )
    if( not result[ 'OK' ] ):
      raise Exception \
                  ( 'Cannot get the Database parameters' % result( 'Message' ) )
    self.dbHost, \
      self.dbPort, \
      self.dbUser, \
      self.dbPass, \
      self.dbName, \
      self.maxQueueSize = result[ 'Value' ]

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
    return gConfig.getValue( "/%s/%s" % ( self.cs_path, optionName ), defaultValue )
