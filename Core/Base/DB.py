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

  def __init__( self, dbname, fullname, maxQueueSize, debug = False ):

    self.database_name = dbname
    self.fullname = fullname
    self.cs_path = getDatabaseSection( fullname )

    self.log = gLogger.getSubLogger( self.database_name )

    self.dbHost = ''
    result = gConfig.getOption( self.cs_path + '/Host' )
    if not result['OK']:
      raise RuntimeError( 'Failed to get the configuration parameters: Host' )
    self.dbHost = result['Value']
    # Check if the host is the local one and then set it to 'localhost' to use
    # a socket connection
    if self.dbHost != 'localhost':
      localHostName = socket.getfqdn()
      if localHostName == self.dbHost:
        self.dbHost = 'localhost'

    self.dbPort = 3306
    result = gConfig.getOption( self.cs_path + '/Port' )
    if not result['OK']:
      # No individual port number found, try at the common place
      result = gConfig.getOption( '/Systems/Databases/Port' )
      if result['OK']:
        self.dbPort = int( result['Value'] )
    else:
      self.dbPort = int( result['Value'] )

    self.dbUser = ''
    result = gConfig.getOption( self.cs_path + '/User' )
    if not result['OK']:
      # No individual user name found, try at the common place
      result = gConfig.getOption( '/Systems/Databases/User' )
      if not result['OK']:
        raise RuntimeError( 'Failed to get the configuration parameters: User' )
    self.dbUser = result['Value']
    self.dbPass = ''
    result = gConfig.getOption( self.cs_path + '/Password' )
    if not result['OK']:
      # No individual password found, try at the common place
      result = gConfig.getOption( '/Systems/Databases/Password' )
      if not result['OK']:
        raise RuntimeError( 'Failed to get the configuration parameters: Password' )
    self.dbPass = result['Value']
    self.dbName = ''
    result = gConfig.getOption( self.cs_path + '/DBName' )
    if not result['OK']:
      raise RuntimeError( 'Failed to get the configuration parameters: DBName' )
    self.dbName = result['Value']
    self.maxQueueSize = maxQueueSize
    result = gConfig.getOption( self.cs_path + '/MaxQueueSize' )
    if result['OK']:
      self.maxQueueSize = int( result['Value'] )

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
