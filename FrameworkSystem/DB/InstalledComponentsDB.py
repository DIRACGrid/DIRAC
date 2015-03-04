"""
Classes and functions for easier management of the InstalledComponents database
"""

__RCSID__ = "$Id$"

import datetime
from sqlalchemy import *
from sqlalchemy.orm import session, sessionmaker, mapper, relationship
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import *
from sqlalchemy.engine.reflection import Inspector
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection

metadata = MetaData()
Base = declarative_base()

class Component( Base ):
  """
  This class defines the schema of the Components table in the
  InstalledComponentsDB database
  """

  __tablename__ = 'Components'
  __table_args__ = {
    'mysql_engine': 'InnoDB',
    'mysql_charset': 'utf8'
  }

  componentID = Column( 'ComponentID', Integer, primary_key = True )
  system = Column( 'System', String( 32 ), nullable = False )
  module = Column( 'Module', String( 32 ), nullable = False )
  cType = Column( 'Type', String( 32 ), nullable = False )

  def __init__( self, system = null(), module = null(), cType = null() ):
    self.system = system
    self.module = module
    self.cType = cType

  def fromDict( self, dictionary ):
    """
    Fill the fields of the Component object from a dictionary
    The dictionary may contain the keys: ComponentID, System, Module, Type
    """

    self.componentID = dictionary.get( 'ComponentID', self.componentID )
    self.system = dictionary.get( 'System', self.system )
    self.module = dictionary.get( 'Module', self.module )
    self.cType = dictionary.get( 'Type', self.cType )

    return S_OK( 'Successfully read from dictionary' )

  def toDict( self, includeInstallations = False, includeHosts = False ):
    """
    Return the object as a dictionary
    If includeInstallations is True, the dictionary returned will also include
    information about the installations in which this Component is included
    If includeHosts is also True, further information about the Hosts where the
    installations are is included
    """

    dictionary = {
                  'ComponentID': self.componentID,
                  'System': self.system,
                  'Module': self.module,
                  'Type': self.cType
                  }

    if includeInstallations:
      installations = []
      for installation in self.installationList:
        relationshipDict = installation.toDict( False, includeHosts )[ 'Value' ]
        installations.append( relationshipDict )
      dictionary[ 'Installations' ] = installations

    return S_OK( dictionary )

class Host( Base ):
  """
  This class defines the schema of the Hosts table in the
  InstalledComponentsDB database
  """

  __tablename__ = 'Hosts'
  __table_args__ = {
    'mysql_engine': 'InnoDB',
    'mysql_charset': 'utf8'
  }

  hostID = Column( 'HostID', Integer, primary_key = True )
  hostName = Column( 'HostName', String( 32 ), nullable = False )
  cpu = Column( 'CPU', String( 64 ), nullable = False )
  installationList = relationship( 'InstalledComponent',
                                    backref = 'installationHost' )

  def __init__( self, host = null(), cpu = null() ):
    self.hostName = host
    self.cpu = cpu

  def fromDict( self, dictionary ):
    """
    Fill the fields of the Host object from a dictionary
    The dictionary may contain the keys: HostID, HostName, CPU
    """

    self.hostID = dictionary.get( 'HostID', self.hostID )
    self.hostName = dictionary.get( 'HostName', self.hostName )
    self.cpu = dictionary.get( 'CPU', self.cpu )

    return S_OK( 'Successfully read from dictionary' )

  def toDict( self, includeInstallations = False, includeComponents = False ):
    """
    Return the object as a dictionary
    If includeInstallations is True, the dictionary returned will also include
    information about the installations in this Host
    If includeComponents is also True, further information about which
    Components where installed is included
    """

    dictionary = {
                  'HostID': self.hostID,
                  'HostName': self.hostName,
                  'CPU': self.cpu
                  }

    if includeInstallations:
      installations = []
      for installation in self.installationList:
        relationshipDict = \
                      installation.toDict( includeComponents, False )[ 'Value' ]
        installations.append( relationshipDict )
      dictionary[ 'Installations' ] = installations

    return S_OK( dictionary )

class InstalledComponent( Base ):
  """
  This class defines the schema of the InstalledComponents table in the
  InstalledComponentsDB database
  """

  __tablename__ = 'InstalledComponents'
  __table_args__ = {
    'mysql_engine': 'InnoDB',
    'mysql_charset': 'utf8'
  }

  componentID = Column( 'ComponentID', 
                        Integer,
                        ForeignKey( 'Components.ComponentID' ),
                        primary_key = True )
  hostID = Column( 'HostID',
                    Integer,
                    ForeignKey( 'Hosts.HostID' ),
                    primary_key = True )
  instance = Column( 'Instance',
                      String( 32 ),
                      primary_key = True )
  installationTime = Column( 'InstallationTime',
                              DateTime,
                              primary_key = True )
  unInstallationTime = Column( 'UnInstallationTime',
                                DateTime )
  installationComponent = relationship( 'Component',
                                        backref = 'installationList' )

  def __init__( self, instance = null(),
                      installationTime = null(),
                      unInstallationTime = null() ):
    self.instance = instance
    self.installationTime = installationTime
    self.unInstallationTime = unInstallationTime

  def fromDict( self, dictionary ):
    """
    Fill the fields of the InstalledComponent object from a dictionary
    The dictionary may contain the keys: ComponentID, HostID, Instance,
    InstallationTime, UnInstallationTime
    """

    self.componentID = dictionary.get( 'ComponentID', self.componentID )
    self.hostID = dictionary.get( 'HostID', self.hostID )
    self.instance = dictionary.get( 'Instance', self.instance )
    self.installationTime = dictionary.get( 'InstallationTime',
                                            self.installationTime )
    self.unInstallationTime = dictionary.get( 'UnInstallationTime',
                                              self.unInstallationTime )

    return S_OK( 'Successfully read from dictionary' )

  def toDict( self, includeComponents = False, includeHosts = False ):
    """
    Return the object as a dictionary
    If includeComponents is True, information about which Components where
    installed is included
    If includeHosts is True, information about the Hosts where the
    installations are is included
    """

    dictionary = {
                  'Instance': self.instance,
                  'InstallationTime': self.installationTime,
                  'UnInstallationTime': self.unInstallationTime
                  }

    if includeComponents:
      dictionary[ 'Component' ] = self.installationComponent.toDict()[ 'Value' ]
    else:
      dictionary[ 'ComponentID' ] = self.componentID

    if includeHosts:
      dictionary[ 'Host' ] = self.installationHost.toDict()[ 'Value' ]
    else:
      dictionary[ 'HostID' ] = self.hostID

    return S_OK( dictionary )

class InstalledComponentsDB( object ):
  """
  Class used to work with the InstalledComponentsDB database.
  It creates the tables on initialization and allows inserting, querying,
  deleting from/to the tables
  """

  def __init__( self ):
    self.__initializeConnection( 'Framework/InstalledComponentsDB' )
    result = self.__initializeDB()
    if not result[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % result[ 'Message' ] )

    self.componentFields = [ 'ComponentId', 'System', 'Module', 'Type' ]

  def __initializeConnection( self, dbPath ):
    self.dbs = getDatabaseSection( dbPath )

    self.host = gConfig.getOption( self.dbs + '/Host' )
    if not self.host[ 'OK' ]:
      raise Exception( "Cannot retrieve the host: %s" % self.host[ 'Message' ] )
    else:
      self.host = self.host[ 'Value' ]

    self.user = gConfig.getOption( '/Systems/Databases/User' )
    if not self.user[ 'OK' ]:
      raise Exception( "Cannot retrieve the user: %s" % self.user[ 'Message' ] )
    else:
      self.user = self.user[ 'Value' ]

    self.password = gConfig.getOption( '/Systems/Databases/Password' )
    if not self.password[ 'OK' ]:
      raise Exception( "Cannot retrieve the password: %s" 
                          % self.password[ 'Message' ] )
    else:
      self.password = self.password[ 'Value' ]

    self.db = gConfig.getOption( self.dbs + '/DBName' )
    if not self.db[ 'OK' ]:
      raise Exception( "Cannot retrieve the DB name: %s"
                          % self.db[ 'Message' ] )
    else:
      self.db = self.db[ 'Value' ]

    self.engine = create_engine( 'mysql://%s:%s@%s/%s' %
                    ( self.user, self.password, self.host, self.db ),
                    pool_recycle = 3600 )
    Session = sessionmaker( bind = self.engine )
    self.session = Session()
    self.inspector = Inspector.from_engine( self.engine )

  def __initializeDB( self ):
    """
    Create the tables
    """

    tablesInDB = self.inspector.get_table_names()

    # Components
    if not 'Components' in tablesInDB:
      try:
        Component.__table__.create( self.engine )
      except Exception, e:
        return S_ERROR( e )
    else:
      gLogger.debug( 'Table \'Components\' already exists' )

    # Hosts
    if not 'Hosts' in tablesInDB:
      try:
        Host.__table__.create( self.engine )
      except Exception, e:
        return S_ERROR( e )
    else:
      gLogger.debug( 'Table \'Hosts\' already exists' )

    # InstalledComponents
    if not 'InstalledComponents' in tablesInDB:
      try:
        InstalledComponent.__table__.create( self.engine )
      except Exception, e:
        return S_ERROR( e )
    else:
      gLogger.debug( 'Table \'InstalledComponents\' already exists' )

    return S_OK( 'Tables created' )

  def __filterFields( self, table, matchFields = {} ):
    """
    Filters instances of a selection by finding matches on the given fields
    table argument must be one the following three: Component, Host,
    InstalledComponent
    matchFields argument should be a dictionary with the fields to match.
    matchFields accepts fields of the form <Field.bigger> and <Field.smaller>
    to filter using > and < relationships.
    If matchFields is empty, no filtering will be done
    """

    filtered = self.session.query(table)

    for key in matchFields.keys():
      actualKey = key

      comparison = '='
      if '.bigger' in key:
        comparison = '>'
        actualKey = key.replace( '.bigger', '' )
      elif '.smaller' in key:
        comparison = '<'
        actualKey = key.replace( '.smaller', '' )

      if matchFields[ key ] == None:
        sql = '%s IS NULL' % ( actualKey )
      elif type( matchFields[ key ] ) == list:
        if len( matchFields[ key ] ) > 0 and not None in matchFields[ key ]:
          sql = '%s IN ( ' % ( actualKey )
          for i, element in enumerate( matchFields[ key ] ):
            toAppend = element
            if type( toAppend ) == datetime.datetime:
              toAppend = toAppend.strftime( "%Y-%m-%d %H:%M:%S" )
            if type( toAppend in [ datetime.datetime, str ] ):
              toAppend = '\'%s\'' % ( toAppend )
            if i == 0:
              sql = '%s%s' % ( sql, toAppend )
            else:
              sql = '%s, %s' % ( sql, toAppend )
          sql = '%s )' % ( sql )
        else:
          continue
      elif type( matchFields[ key ] ) == str:
        sql = '%s %s \'%s\'' % ( actualKey, comparison, matchFields[ key ] )
      elif type( matchFields[ key ] ) == datetime.datetime:
        sql = '%s %s \'%s\'' % \
                        ( actualKey,
                          comparison,
                          matchFields[ key ].strftime( "%Y-%m-%d %H:%M:%S" ) )
      else:
        sql = '%s %s %s' % ( actualKey, comparison, matchFields[ key ] )

      filteredTemp = filtered.filter( sql )
      try:
        self.session.execute( filteredTemp )
      except Exception, e:
        return S_ERROR( 'Could not filter the fields: %s' % ( e ) )
      filtered = filteredTemp

    return S_OK( filtered )

  def __filterInstalledComponentsFields( self, matchFields = {} ):
    """
    Filters instances by finding matches on the given fields in the same way
    as the '__filterFields' function
    The main difference with '__filterFields' is that this function is
    targeted towards the InstalledComponents table
    and accepts fields of the form <Component.Field> and <Host.Field>
    ( e.g., 'Component.System' ) to filter installations using attributes
    from their associated Components and Hosts.
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships.
    matchFields argument should be a dictionary with the fields to match.
    If matchFields is empty, no filtering will be done
    """

    componentKeys = {}
    for ( key, val ) in matchFields.items():
      if 'Component.' in key:
        componentKeys[ key.replace( 'Component.', '' ) ] = val

    hostKeys = {}
    for ( key, val ) in matchFields.items():
      if 'Host.' in key:
        hostKeys[ key.replace( 'Host.', '' ) ] = val

    selfKeys = {}
    for ( key, val ) in matchFields.items():
      if not 'Component.' in key and not 'Host.' in key:
        selfKeys[ key ] = val

    # Get the matching components
    result = self.__filterFields( Component, componentKeys )
    if not result[ 'OK' ]:
      return result

    componentIDs = []
    for component in result[ 'Value' ]:
      componentIDs.append( component.componentID )

    # Get the matching hosts
    result = self.__filterFields( Host, hostKeys )
    if not result[ 'OK' ]:
      return result

    hostIDs = []
    for host in result[ 'Value' ]:
      hostIDs.append( host.hostID )

    # Get the matching InstalledComponents
    result = self.__filterFields( InstalledComponent, selfKeys )
    if not result[ 'OK' ]:
      return result

    # And use the Component and Host IDs to filter them as well
    installations = result[ 'Value' ]\
                  .filter( InstalledComponent.componentID.in_( componentIDs ) )\
                  .filter( InstalledComponent.hostID.in_( hostIDs ) )

    return S_OK( installations )

  def exists( self, table, matchFields ):
    """
    Checks whether an instance matching the given criteria exists
    table argument must be one the following three: Component, Host,
    InstalledComponent
    matchFields argument should be a dictionary with the fields to match.
    If matchFields is empty, no filtering will be done
    matchFields may contain entries of the form 'Component.attribute' or
    'Host.attribute' if table equals InstalledComponent
    """

    if table == InstalledComponent:
      result = self.__filterInstalledComponentsFields( matchFields )
    else:
      result = self.__filterFields( table, matchFields )
    if not result[ 'OK' ]:
      return result
    query = result[ 'Value' ]

    if query.count() == 0:
      return S_OK( False )
    else:
      return S_OK( True )

  def addComponent( self, component ):
    """
    Add a new component to the database
    component argument should be of class Component

    NOTE: The addition of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    if type( component ) != Component:
      return S_ERROR( 'Only a component can be added by addComponent' )

    try:
      self.session.add( component )
    except Exception, e:
      return S_ERROR( 'Could not add Component: %s' % ( e ) )

    return S_OK( 'Component successfully added' )

  def removeComponents( self, matchFields = {} ):
    """
    Removes components with matches in the given fields
    matchFields argument should be a dictionary with the fields and values
    to match
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    matchFields argument can be empty to remove all the components

    NOTE: The removal of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    result = self.__filterFields( Component, matchFields )
    if not result[ 'OK' ]:
      return result

    for component in result[ 'Value' ]:
      self.session.delete( component )

    return S_OK( 'Components successfully removed' )

  def getComponents( self, matchFields = {} ):
    """
    Returns a list with all the components with matches in the given fields
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    """

    result = self.__filterFields( Component, matchFields )
    if not result[ 'OK' ]:
      return result

    components = result[ 'Value' ]
    if not components:
      return S_ERROR( 'No matching Components were found' )

    return S_OK( components )

  def getComponentByID( self, id ):
    """
    Returns a component given its id
    """

    result = self.getComponents( matchFields = { 'ComponentID': id } )
    if not result[ 'OK' ]:
      return result

    component = result[ 'Value' ]
    if component.count() == 0:
      return S_ERROR( 'Component with ID %s does not exist' % ( id ) )

    return S_OK( component[0] )

  def componentExists( self, component ):
    """
    Checks whether the given component exists in the database or not
    """

    try:
      query = self.session.query( Component )\
                          .filter( Component.system == component.system )\
                          .filter( Component.module == component.module )\
                          .filter( Component.cType == component.cType )
    except Exception, e:
      return S_ERROR( 
                  'Could\'t check the existence of the component: %s' % ( e ) )

    if query.count() == 0:
      return S_OK( False )
    else:
      return S_OK( True )

  def addHost( self, host ):
    """
    Add a new host to the database
    host argument should be of class Host

    NOTE: The addition of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    if type( host ) != Host:
      return S_ERROR( 'Only a host can be added by addHost' )

    try:
      self.session.add( host )
    except Exception, e:
      return S_ERROR( 'Could not add Host: %s' % ( e ) )

    return S_OK( 'Host successfully added' )

  def removeHosts( self, matchFields = {} ):
    """
    Removes hosts with matches in the given fields
    matchFields argument should be a dictionary with the fields and values
    to match
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    matchFields argument can be empty to remove all the hosts

    NOTE: The removal of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    result = self.__filterFields( Host, matchFields )
    if not result[ 'OK' ]:
      return result

    for host in result[ 'Value' ]:
      self.session.delete( host )

    return S_OK( 'Hosts successfully removed' )

  def getHosts( self, matchFields = {} ):
    """
    Returns a list with all the hosts with matches in the given fields
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    """

    result = self.__filterFields( Host, matchFields )
    if not result[ 'OK' ]:
      return result

    hosts = result[ 'Value' ]
    if not hosts:
      return S_ERROR( 'No matching Hosts were found' )

    return S_OK( hosts )

  def getHostByID( self, id ):
    """
    Returns a host given its id
    """

    result = self.getHosts( matchFields = { 'HostID': id } )
    if not result[ 'OK' ]:
      return result

    host = result[ 'Value' ]
    if host.count() == 0:
      return S_ERROR( 'Host with ID %s does not exist' % ( id ) )

    return S_OK( host[0] )

  def hostExists( self, host ):
    """
    Checks whether the given host exists in the database or not
    """

    try:
      query = self.session.query( Host )\
                          .filter( Host.hostName == host.hostName )\
                          .filter( Host.cpu == host.cpu )
    except Exception, e:
      return S_ERROR( 'Could\'t check the existence of the host: %s' % ( e ) )

    if query.count() == 0:
      return S_OK( False )
    else:
      return S_OK( True )

  def addInstalledComponent( self,
                                installation,
                                component,
                                host,
                                forceCreate = False ):
    """
    Add a new installation of a component to the database
    installation argument should be of class InstalledComponent
    component argument should be of class Component
    host argument should be of class Host
    If forceCreate is set to True, both the component and the host will be
    created if they do not exist

    NOTE: The addition of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    if type( component ) != Component:
      return S_ERROR( 'A Component must be provided to addInstalledComponent' )
    if type( host ) != Host:
      return S_ERROR( 'A Host must be provided to addInstalledComponent' )

    result = self.componentExists( component )
    if not result[ 'OK' ]:
      return result
    if not result[ 'Value' ]:
      if forceCreate:
        result = self.addComponent( component )
        if not result[ 'OK' ]:
          return result
      else:
        return S_ERROR( 'Given Component does not exist' )

    result = self.hostExists( host )
    if not result[ 'OK' ]:
      return result
    if not result[ 'Value' ]:
      if forceCreate:
        result = self.addHost( host )
        if not result[ 'OK' ]:
          return result
      else:
        return S_ERROR( 'Given Host does not exist' )

    installation.installationComponent = component
    installation.installationHost = host

    return S_OK( 'InstalledComponent successfully added' )

  def removeInstalledComponents( self, matchFields = {} ):
    """
    Removes InstalledComponents with matches in the given fields
    matchFields argument should be a dictionary with the fields and values
    to match and may contain entries of the form 'Component.attribute'
    or 'Host.attribute'
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships.
    matchFields argument can be empty to remove all the hosts

    NOTE: The removal of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    result = self.__filterInstalledComponentsFields( matchFields )
    if not result[ 'OK' ]:
      return result

    installations = result[ 'Value' ]

    for installation in installations:
      self.session.delete( installation )

    return S_OK( 'InstalledComponents successfully removed' )

  def getInstalledComponents( self, matchFields = {} ):
    """
    Returns a list with all the InstalledComponents with matches in the given
    fields
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances and may contain entries of the form
    'Component.attribute' or 'Host.attribute'
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    """

    result = self.__filterInstalledComponentsFields( matchFields )
    if not result[ 'OK' ]:
      return result

    installations = result[ 'Value' ]
    if not installations:
      return S_ERROR( 'No matching Installations were found' )

    return S_OK( installations )

  def flushChanges( self ):
    """
    Flushes all the previous changes.

    NOTE: This will not keep the data in the database after execution
    """

    try:
      self.session.flush()
    except Exception, e:
      self.session.rollback()
      return S_ERROR( 'Could not flush the changes: %s' % ( e ) )

    return S_OK( 'Changes successfully flushed' )

  def commitChanges( self ):
    """
    Commits all the previous changes to the database
    """

    try:
      self.session.commit()
    except Exception, e:
      self.session.rollback()
      return S_ERROR( 'Could not commit the changes: %s' % ( e ) )

    return S_OK( 'Changes successfully committed' )
