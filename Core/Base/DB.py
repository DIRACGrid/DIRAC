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

  def __init__( self, dbname, fullname, maxQueueSize ):

    self.database_name = dbname
    self.fullname = fullname
    self.cs_path = getDatabaseSection( fullname )

    self.log = gLogger.getSubLogger( self.database_name )

    self.dbHost = ''
    result = gConfig.getOption( self.cs_path + '/Host' )
    if not result['OK']:
      self.log.fatal( 'Failed to get the configuration parameters: Host' )
      return
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
        self.log.fatal( 'Failed to get the configuration parameters: User' )
        return
    self.dbUser = result['Value']
    self.dbPass = ''
    result = gConfig.getOption( self.cs_path + '/Password' )
    if not result['OK']:
      # No individual password found, try at the common place
      result = gConfig.getOption( '/Systems/Databases/Password' )
      if not result['OK']:
        self.log.fatal( 'Failed to get the configuration parameters: Password' )
        return
    self.dbPass = result['Value']
    self.dbName = ''
    result = gConfig.getOption( self.cs_path + '/DBName' )
    if not result['OK']:
      self.log.fatal( 'Failed to get the configuration parameters: DBName' )
      return
    self.dbName = result['Value']
    self.maxQueueSize = maxQueueSize
    result = gConfig.getOption( self.cs_path + '/MaxQueueSize' )
    if result['OK']:
      self.maxQueueSize = int( result['Value'] )

    MySQL.__init__( self, self.dbHost, self.dbUser, self.dbPass,
                   self.dbName, self.dbPort, maxQueueSize = maxQueueSize )

    if not self._connected:
      err = 'Can not connect to DB, exiting...'
      self.log.fatal( err )
      sys.exit( err )


    self.log.info( "==================================================" )
    #self.log.info("SystemInstance: "+self.system)
    self.log.info( "User:           " + self.dbUser )
    self.log.info( "Host:           " + self.dbHost )
    self.log.info( "Port:           " + str( self.dbPort ) )
    #self.log.info("Password:       "+self.dbPass)
    self.log.info( "DBName:         " + self.dbName )
    self.log.info( "MaxQueue:       " + str( self.maxQueueSize ) )
    self.log.info( "==================================================" )

#########################################################################################
  def getCounters( self, table, attrList, condDict, older = None, newer = None, timeStamp = None, connection = False ):
    """ Count the number of records on each distinct combination of AttrList, selected
        with condition defined by condDict and time stamps
    """

    cond = self.buildCondition( condDict, older, newer, timeStamp )
    attrNames = ','.join( [ str( x ) for x in attrList ] )
    # attrNames = ','.join( map( lambda x: str( x ), attrList ) )
    cmd = 'SELECT %s,COUNT(*) FROM %s %s GROUP BY %s ORDER BY %s' % ( attrNames, table, cond, attrNames, attrNames )
    result = self._query( cmd , connection )
    if not result['OK']:
      return result

    resultList = []
    for raw in result['Value']:
      attrDict = {}
      for i in range( len( attrList ) ):
        attrDict[attrList[i]] = raw[i]
      item = ( attrDict, raw[len( attrList )] )
      resultList.append( item )
    return S_OK( resultList )

#############################################################################
  def getDistinctAttributeValues( self, table, attribute, condDict = None, older = None,
                                  newer = None, timeStamp = None, connection = False ):
    """ Get distinct values of a table attribute under specified conditions
    """
    cond = self.buildCondition( condDict, older = older, newer = newer, timeStamp = timeStamp )
    cmd = 'SELECT  DISTINCT(%s) FROM %s %s ORDER BY %s' % ( attribute, table, cond, attribute )
    result = self._query( cmd, connection )
    if not result['OK']:
      return result
    attr_list = [ x[0] for x in result['Value'] ]
    return S_OK( attr_list )

#############################################################################
  def getCSOption( self, optionName, defaultValue = None ):
    return gConfig.getValue( "/%s/%s" % ( self.cs_path, optionName ), defaultValue )
