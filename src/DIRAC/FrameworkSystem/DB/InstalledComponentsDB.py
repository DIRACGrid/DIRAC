"""
Classes and functions for easier management of the InstalledComponents database
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import re
import datetime
from sqlalchemy import MetaData, Column, Integer, String, DateTime, create_engine, text
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, \
    scoped_session, \
    relationship
from sqlalchemy.schema import ForeignKey
from sqlalchemy.sql.expression import null

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters

__RCSID__ = "$Id$"

metadata = MetaData()
componentsBase = declarative_base()


class Component(componentsBase):
  """
  This class defines the schema of the Components table in the
  InstalledComponentsDB database
  """

  __tablename__ = 'Components'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  componentID = Column('ComponentID', Integer, primary_key=True)
  system = Column('DIRACSystem', String(32), nullable=False)
  module = Column('DIRACModule', String(32), nullable=False)
  cType = Column('Type', String(32), nullable=False)

  def __init__(self, system=null(), module=null(), cType=null()):
    """ just defines some instance members
    """
    self.system = system
    self.module = module
    self.cType = cType
    self.installationList = []

  def fromDict(self, dictionary):
    """
    Fill the fields of the Component object from a dictionary
    The dictionary may contain the keys: ComponentID, System, Module, Type
    """

    self.componentID = dictionary.get('ComponentID', self.componentID)
    self.system = dictionary.get('DIRACSystem', self.system)
    self.module = dictionary.get('DIRACModule', self.module)
    self.cType = dictionary.get('Type', self.cType)

    return S_OK('Successfully read from dictionary')

  def toDict(self, includeInstallations=False, includeHosts=False):
    """
    Return the object as a dictionary
    If includeInstallations is True, the dictionary returned will also include
    information about the installations in which this Component is included
    If includeHosts is also True, further information about the Hosts where the
    installations are is included
    """

    dictionary = {'ComponentID': self.componentID,
                  'DIRACSystem': self.system,
                  'DIRACModule': self.module,
                  'Type': self.cType}

    if includeInstallations:
      installations = []
      for installation in self.installationList:
        relationshipDict = installation.toDict(False, includeHosts)['Value']
        installations.append(relationshipDict)
      dictionary['Installations'] = installations

    return S_OK(dictionary)


class Host(componentsBase):
  """
  This class defines the schema of the Hosts table in the
  InstalledComponentsDB database
  """

  __tablename__ = 'Hosts'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  hostID = Column('HostID', Integer, primary_key=True)
  hostName = Column('HostName', String(32), nullable=False)
  cpu = Column('CPU', String(64), nullable=False)
  installationList = relationship('InstalledComponent',
                                  backref='installationHost')

  def __init__(self, host=null(), cpu=null()):
    self.hostName = host
    self.cpu = cpu

  def fromDict(self, dictionary):
    """
    Fill the fields of the Host object from a dictionary
    The dictionary may contain the keys: HostID, HostName, CPU
    """

    self.hostID = dictionary.get('HostID', self.hostID)
    self.hostName = dictionary.get('HostName', self.hostName)
    self.cpu = dictionary.get('CPU', self.cpu)

    return S_OK('Successfully read from dictionary')

  def toDict(self, includeInstallations=False, includeComponents=False):
    """
    Return the object as a dictionary
    If includeInstallations is True, the dictionary returned will also include
    information about the installations in this Host
    If includeComponents is also True, further information about which
    Components where installed is included
    """

    dictionary = {'HostID': self.hostID,
                  'HostName': self.hostName,
                  'CPU': self.cpu}

    if includeInstallations:
      installations = []
      for installation in self.installationList:
        relationshipDict = \
            installation.toDict(includeComponents, False)['Value']
        installations.append(relationshipDict)
      dictionary['Installations'] = installations

    return S_OK(dictionary)


class InstalledComponent(componentsBase):
  """
  This class defines the schema of the InstalledComponents table in the
  InstalledComponentsDB database
  """

  __tablename__ = 'InstalledComponents'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  componentID = Column('ComponentID',
                       Integer,
                       ForeignKey('Components.ComponentID'),
                       primary_key=True)
  hostID = Column('HostID',
                  Integer,
                  ForeignKey('Hosts.HostID'),
                  primary_key=True)
  instance = Column('Instance',
                    String(64),
                    primary_key=True)
  installationTime = Column('InstallationTime',
                            DateTime,
                            primary_key=True)
  unInstallationTime = Column('UnInstallationTime',
                              DateTime)
  installedBy = Column('InstalledBy', String(32))
  unInstalledBy = Column('UnInstalledBy', String(32))
  installationComponent = relationship('Component',
                                       backref='installationList')

  def __init__(self, instance=null(),
               installationTime=null(),
               unInstallationTime=null(),
               installedBy=null(),
               unInstalledBy=null()):
    self.instance = instance
    self.installationTime = installationTime
    self.unInstallationTime = unInstallationTime
    self.installedBy = installedBy
    self.unInstalledBy = unInstalledBy

  def fromDict(self, dictionary):
    """
    Fill the fields of the InstalledComponent object from a dictionary
    The dictionary may contain the keys: ComponentID, HostID, Instance,
    InstallationTime, UnInstallationTime
    """

    self.componentID = dictionary.get('ComponentID', self.componentID)
    self.hostID = dictionary.get('HostID', self.hostID)
    self.instance = dictionary.get('Instance', self.instance)
    self.installationTime = dictionary.get('InstallationTime',
                                           self.installationTime)
    self.unInstallationTime = dictionary.get('UnInstallationTime',
                                             self.unInstallationTime)
    self.installedBy = dictionary.get('InstalledBy',
                                      self.installedBy)
    self.unInstalledBy = dictionary.get('UnInstalledBy',
                                        self.unInstalledBy)

    return S_OK('Successfully read from dictionary')

  def toDict(self, includeComponents=False, includeHosts=False):
    """
    Return the object as a dictionary
    If includeComponents is True, information about which Components where
    installed is included
    If includeHosts is True, information about the Hosts where the
    installations are is included
    """

    dictionary = {'Instance': self.instance,
                  'InstallationTime': self.installationTime,
                  'UnInstallationTime': self.unInstallationTime,
                  'InstalledBy': self.installedBy,
                  'UnInstalledBy': self.unInstalledBy}

    if includeComponents:
      dictionary['Component'] = self.installationComponent.toDict()['Value']
    else:
      dictionary['ComponentID'] = self.componentID

    if includeHosts:
      dictionary['Host'] = self.installationHost.toDict()['Value']
    else:
      dictionary['HostID'] = self.hostID

    return S_OK(dictionary)


class HostLogging(componentsBase):
  """
  This class defines the schema of the HostLogging table in the
  InstalledComponentsDB database
  """

  __tablename__ = 'HostLogging'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  hostName = Column('HostName', String(32), nullable=False, primary_key=True)
  # status
  DIRAC = Column('DIRACVersion', String(64))
  Extension = Column('Extension', String(64))
  Load1 = Column('Load1', String(32))  # float
  Load5 = Column('Load5', String(32))  # float
  Load15 = Column('Load15', String(32))  # float
  Memory = Column('Memory', String(32))
  DiskOccupancy = Column('DiskOccupancy', String(512))
  Swap = Column('Swap', String(32))
  CPUClock = Column('CPUClock', String(32))  # float
  CPUModel = Column('CPUModel', String(64))
  CertificateDN = Column('CertificateDN', String(128))
  CertificateIssuer = Column('CertificateIssuer', String(128))
  CertificateValidity = Column('CertificateValidity', String(64))
  Cores = Column('Cores', Integer)
  PhysicalCores = Column('PhysicalCores', Integer)
  OpenFiles = Column('OpenFiles', Integer)
  OpenPipes = Column('OpenPipes', Integer)
  OpenSockets = Column('OpenSockets', Integer)
  Setup = Column('Setup', String(32))
  Uptime = Column('Uptime', String(64))
  Timestamp = Column('Timestamp', DateTime)

  def __init__(self, host=null(), **kwargs):
    self.hostName = host
    fields = dir(self)

    for key, value in kwargs.items():
      if key in fields and not re.match('_.*', key):
        setattr(self, key, value)

  def fromDict(self, dictionary):
    """
    Fill the fields of the HostLogging object from a dictionary
    """
    fields = dir(self)

    if dictionary.get('DIRACVersion'):
      dictionary['DIRAC'] = dictionary.get('DIRACVersion')

    try:
      for key, value in dictionary.items():
        if key in fields and not re.match('_.*', key):
          setattr(self, key, value)
    except Exception as e:
      return S_ERROR(e)

    return S_OK('Successfully read from dictionary')

  def toDict(self):
    """
    Return the object as a dictionary
    """

    dictionary = {'HostName': self.hostName,
                  'DIRACVersion': self.DIRAC,
                  'Extension': self.Extension,
                  'Load1': self.Load1,
                  'Load5': self.Load5,
                  'Load15': self.Load15,
                  'Memory': self.Memory,
                  'DiskOccupancy': self.DiskOccupancy,
                  'Swap': self.Swap,
                  'CPUClock': self.CPUClock,
                  'CPUModel': self.CPUModel,
                  'CertificateDN': self.CertificateDN,
                  'CertificateIssuer': self.CertificateIssuer,
                  'CertificateValidity': self.CertificateValidity,
                  'Cores': self.Cores,
                  'PhysicalCores': self.PhysicalCores,
                  'OpenFiles': self.OpenFiles,
                  'OpenPipes': self.OpenPipes,
                  'OpenSockets': self.OpenSockets,
                  'Setup': self.Setup,
                  'Uptime': self.Uptime,
                  'Timestamp': self.Timestamp}

    return S_OK(dictionary)


class InstalledComponentsDB(object):
  """
  Class used to work with the InstalledComponentsDB database.
  It creates the tables on initialization and allows inserting, querying,
  deleting from/to the tables
  """

  def __init__(self):
    self.__initializeConnection('Framework/InstalledComponentsDB')
    result = self.__initializeDB()
    if not result['OK']:
      raise Exception("Can't create tables: %s" % result['Message'])

  def __initializeConnection(self, dbPath):

    result = getDBParameters(dbPath)
    if not result['OK']:
      raise Exception('Cannot get database parameters: %s' % result['Message'])

    dbParameters = result['Value']
    self.host = dbParameters['Host']
    self.port = dbParameters['Port']
    self.user = dbParameters['User']
    self.password = dbParameters['Password']
    self.dbName = dbParameters['DBName']

    self.engine = create_engine('mysql://%s:%s@%s:%s/%s' % (self.user,
                                                            self.password,
                                                            self.host,
                                                            self.port,
                                                            self.dbName),
                                pool_recycle=3600, echo_pool=True)
    self.session = scoped_session(sessionmaker(bind=self.engine))
    self.inspector = Inspector.from_engine(self.engine)

  def __initializeDB(self):
    """
    Create the tables
    """

    tablesInDB = self.inspector.get_table_names()

    # Components
    if 'Components' not in tablesInDB:
      try:
        Component.__table__.create(self.engine)  # pylint: disable=no-member
      except Exception as e:
        return S_ERROR(e)
    else:
      gLogger.debug('Table \'Components\' already exists')

    # Hosts
    if 'Hosts' not in tablesInDB:
      try:
        Host.__table__.create(self.engine)  # pylint: disable=no-member
      except Exception as e:
        return S_ERROR(e)
    else:
      gLogger.debug('Table \'Hosts\' already exists')

    # InstalledComponents
    if 'InstalledComponents' not in tablesInDB:
      try:
        InstalledComponent.__table__.create(self.engine)  # pylint: disable=no-member
      except Exception as e:
        return S_ERROR(e)
    else:
      gLogger.debug('Table \'InstalledComponents\' already exists')

    # HostLogging
    if 'HostLogging' not in tablesInDB:
      try:
        HostLogging.__table__.create(self.engine)  # pylint: disable=no-member
      except Exception as e:
        return S_ERROR(e)
    else:
      gLogger.debug('Table \'HostLogging\' already exists')

    return S_OK('Tables created')

  def __filterFields(self, session, table, matchFields=None):
    """
    Filters instances of a selection by finding matches on the given fields
    session argument is a Session instance used to retrieve the items
    table argument must be one the following three: Component, Host,
    InstalledComponent
    matchFields argument should be a dictionary with the fields to match.
    matchFields accepts fields of the form <Field.bigger> and <Field.smaller>
    to filter using > and < relationships.
    If matchFields is empty, no filtering will be done
    """

    if matchFields is None:
      matchFields = {}

    filtered = session.query(table)

    for key in matchFields:
      actualKey = key

      comparison = '='
      if '.bigger' in key:
        comparison = '>'
        actualKey = key.replace('.bigger', '')
      elif '.smaller' in key:
        comparison = '<'
        actualKey = key.replace('.smaller', '')

      if matchFields[key] is None:
        sql = '`%s` IS NULL' % (actualKey)
      elif isinstance(matchFields[key], list):
        if len(matchFields[key]) > 0 and None not in matchFields[key]:
          sql = '`%s` IN ( ' % (actualKey)
          for i, element in enumerate(matchFields[key]):
            toAppend = element
            if isinstance(toAppend, datetime.datetime):
              toAppend = toAppend.strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(toAppend, six.string_types):
              toAppend = '\'%s\'' % (toAppend)
            if i == 0:
              sql = '%s%s' % (sql, toAppend)
            else:
              sql = '%s, %s' % (sql, toAppend)
          sql = '%s )' % (sql)
        else:
          continue
      elif isinstance(matchFields[key], six.string_types):
        sql = '`%s` %s \'%s\'' % (actualKey, comparison, matchFields[key])
      elif isinstance(matchFields[key], datetime.datetime):
        sql = '%s %s \'%s\'' % \
            (actualKey,
             comparison,
             matchFields[key].strftime("%Y-%m-%d %H:%M:%S"))
      else:
        sql = '`%s` %s %s' % (actualKey, comparison, matchFields[key])

      filteredTemp = filtered.filter(text(sql))
      try:
        session.execute(filteredTemp)
        session.commit()
      except Exception as e:
        return S_ERROR('Could not filter the fields: %s' % (e))
      filtered = filteredTemp

    return S_OK(filtered)

  def __filterInstalledComponentsFields(self, session, matchFields=None):
    """
    Filters instances by finding matches on the given fields in the same way
    as the '__filterFields' function
    The main difference with '__filterFields' is that this function is
    targeted towards the InstalledComponents table
    and accepts fields of the form <Component.Field> and <Host.Field>
    ( e.g., 'Component.DIRACSystem' ) to filter installations using attributes
    from their associated Components and Hosts.
    session argument is a Session instance used to retrieve the items
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships.
    matchFields argument should be a dictionary with the fields to match.
    If matchFields is empty, no filtering will be done
    """
    if matchFields is None:
      matchFields = {}
    componentKeys = {}

    for (key, val) in matchFields.items():
      if 'Component.' in key:
        componentKeys[key.replace('Component.', '')] = val

    hostKeys = {}
    for (key, val) in matchFields.items():
      if 'Host.' in key:
        hostKeys[key.replace('Host.', '')] = val

    selfKeys = {}
    for (key, val) in matchFields.items():
      if 'Component.' not in key and 'Host.' not in key:
        selfKeys[key] = val

    # Get the matching components
    result = self.__filterFields(session, Component, componentKeys)
    if not result['OK']:
      return result

    componentIDs = []
    for component in result['Value']:
      componentIDs.append(component.componentID)

    # Get the matching hosts
    result = self.__filterFields(session, Host, hostKeys)
    if not result['OK']:
      return result

    hostIDs = []
    for host in result['Value']:
      hostIDs.append(host.hostID)

    # Get the matching InstalledComponents
    result = self.__filterFields(session, InstalledComponent, selfKeys)
    if not result['OK']:
      return result

    # And use the Component and Host IDs to filter them as well
    installations = result['Value'].filter(InstalledComponent.componentID.in_(componentIDs)).filter(
        InstalledComponent.hostID.in_(hostIDs))

    return S_OK(installations)

  def exists(self, table, matchFields):
    """
    Checks whether an instance matching the given criteria exists
    table argument must be one the following three: Component, Host,
    InstalledComponent
    matchFields argument should be a dictionary with the fields to match.
    If matchFields is empty, no filtering will be done
    matchFields may contain entries of the form 'Component.attribute' or
    'Host.attribute' if table equals InstalledComponent
    """

    session = self.session()

    if table == InstalledComponent:
      result = self.__filterInstalledComponentsFields(session, matchFields)
    else:
      result = self.__filterFields(session, table, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result
    query = result['Value']

    session.commit()
    session.close()

    if query.count() == 0:
      return S_OK(False)
    else:
      return S_OK(True)

  def addComponent(self, newComponent):
    """
    Add a new component to the database
    newComponent argument should be a dictionary with the Component fields and
    its values

    NOTE: The addition of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    session = self.session()

    component = Component()
    component.fromDict(newComponent)

    try:
      session.add(component)
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not add Component: %s' % (e))

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('Component successfully added')

  def removeComponents(self, matchFields=None):
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
    if matchFields is None:
      matchFields = {}

    session = self.session()

    result = self.__filterFields(session, Component, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    for component in result['Value']:
      session.delete(component)

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('Components successfully removed')

  def getComponents(self,
                    matchFields={},
                    includeInstallations=False,
                    includeHosts=False):
    """
    Returns a list with all the components with matches in the given fields
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    includeInstallations indicates whether data about the installations in
    which the components takes part is to be retrieved
    includeHosts (only if includeInstallations is set to True) indicates
    whether data about the host in which there are instances of this component
    is to be retrieved
    """

    session = self.session()

    result = self.__filterFields(session, Component, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    components = result['Value']
    if not components:
      session.rollback()
      session.close()
      return S_ERROR('No matching Components were found')

    dictComponents = []
    for component in components:
      dictComponents.append(component.toDict(includeInstallations, includeHosts)['Value'])

    session.commit()
    session.close()
    return S_OK(dictComponents)

  def getComponentByID(self, cId):
    """
    Returns a component given its id
    """

    result = self.getComponents(matchFields={'ComponentID': cId})
    if not result['OK']:
      return result

    component = result['Value']
    if component.count() == 0:
      return S_ERROR('Component with ID %s does not exist' % (cId))

    return S_OK(component[0])

  def componentExists(self, component):
    """
    Checks whether the given component exists in the database or not
    """

    session = self.session()

    try:
      query = session.query(Component).filter(text(Component.system == component.system)).filter(
          text(Component.module == component.module)).filter(text(Component.cType == component.cType))
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Couldn\'t check the existence of the component: %s' % (e))

    session.commit()
    session.close()
    if query.count() == 0:
      return S_OK(False)
    else:
      return S_OK(True)

  def updateComponents(self, matchFields={}, updates={}):
    """
    Updates Components objects on the database
    matchFields argument should be a dictionary with the fields to match
    (instances matching the fields will be updated) or empty to update all
    the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships updates argument
    should be a dictionary with the Component fields and their new
    updated values
    updates argument should be a dictionary with the Installation fields and
    their new updated values
    """

    session = self.session()

    result = self.__filterFields(session, Component, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    components = result['Value']

    for component in components:
      component.fromDict(updates)

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('Component(s) updated')

  def addHost(self, newHost):
    """
    Add a new host to the database
    host argument should be a dictionary with the Host fields and its values

    NOTE: The addition of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    session = self.session()

    host = Host()
    host.fromDict(newHost)

    try:
      session.add(host)
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not add Host: %s' % (e))

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('Host successfully added')

  def removeHosts(self, matchFields={}):
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

    session = self.session()

    result = self.__filterFields(session, Host, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    for host in result['Value']:
      session.delete(host)

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('Hosts successfully removed')

  def getHosts(self,
               matchFields={},
               includeInstallations=False,
               includeComponents=False):
    """
    Returns a list with all the hosts with matches in the given fields
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    includeInstallations indicates whether data about the installations in
    which the host takes part is to be retrieved
    includeComponents (only if includeInstallations is set to True) indicates
    whether data about the components installed into this host is to
    be retrieved
    """

    session = self.session()

    result = self.__filterFields(session, Host, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    hosts = result['Value']
    if not hosts:
      session.rollback()
      session.close()
      return S_ERROR('No matching Hosts were found')

    dictHosts = []
    for host in hosts:
      dictHosts.append(host.toDict(includeInstallations, includeComponents)['Value'])

    session.commit()
    session.close()

    return S_OK(dictHosts)

  def getHostByID(self, cId):
    """
    Returns a host given its id
    """

    result = self.getHosts(matchFields={'HostID': cId})
    if not result['OK']:
      return result

    host = result['Value']
    if host.count() == 0:
      return S_ERROR('Host with ID %s does not exist' % (cId))

    return S_OK(host[0])

  def hostExists(self, host):
    """
    Checks whether the given host exists in the database or not
    """

    session = self.session()

    try:
      query = session.query(Host).filter(text(Host.hostName == host.hostName)).filter(text(Host.cpu == host.cpu))
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not check the existence of the host: %s' % (e))

    session.commit()
    session.close()
    if query.count() == 0:
      return S_OK(False)
    else:
      return S_OK(True)

  def updateHosts(self, matchFields={}, updates={}):
    """
    Updates Hosts objects on the database
    matchFields argument should be a dictionary with the fields to
    match (instances matching the fields will be updated) or empty to update
    all the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships updates argument
    should be a dictionary with the Host fields and their new updated values
    updates argument should be a dictionary with the Installation fields and
    their new updated values
    """

    session = self.session()

    result = self.__filterFields(session, Host, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    hosts = result['Value']

    for host in hosts:
      host.fromDict(updates)

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('Host(s) updated')

  def addInstalledComponent(self,
                            newInstallation,
                            componentDict,
                            hostDict,
                            forceCreate=False):
    """
    Add a new installation of a component to the database
    installation argument should be a dictionary with the InstalledComponent
    fields and its values
    componentDict argument should be a dictionary with the Component fields
    and its values
    hostDict argument should be a dictionary with the Host fields and
    its values
    If forceCreate is set to True, both the component and the host will be
    created if they do not exist

    NOTE: The addition of the items is temporary. To commit the changes to
    the database it is necessary to call commitChanges()
    """

    session = self.session()

    installation = InstalledComponent()
    installation.fromDict(newInstallation)

    result = self.__filterFields(session, Component, componentDict)
    if not result['OK']:
      session.rollback()
      session.close()
      return result
    if result['Value'].count() != 1:
      if result['Value'].count() > 1:
        session.rollback()
        session.close()
        return S_ERROR('Too many Components match the criteria')
      if result['Value'].count() < 1:
        if not forceCreate:
          session.rollback()
          session.close()
          return S_ERROR('Given component does not exist')
        else:
          component = Component()
          component.fromDict(componentDict)
    else:
      component = result['Value'][0]

    result = self.__filterFields(session, Host, hostDict)
    if not result['OK']:
      session.rollback()
      session.close()
      return result
    if result['Value'].count() == 1:
      host = result['Value'][0]
    elif result['Value'].count() > 1:
      session.rollback()
      session.close()
      return S_ERROR('Too many Hosts match the criteria')
    elif result['Value'].count() == 0:
      if not forceCreate:
        session.rollback()
        session.close()
        return S_ERROR('Given host does not exist')
      # check if HostName exists with different CPU
      gLogger.verbose('Host not found, looking just for hostname')
      hostNameDict = {'HostName': hostDict['HostName']}
      result = self.__filterFields(session, Host, hostNameDict)
      if result['Value'].count() == 1:
        gLogger.verbose('HostName found, updating CPU model')
        host = result['Value'][0]
        self.updateHosts(hostNameDict, hostDict)
      elif result['Value'].count() > 1:
        session.rollback()
        session.close()
        return S_ERROR('Too many Hosts match the HostName')
      else:
        host = Host()
        host.fromDict(hostDict)

    if component:
      installation.installationComponent = component
    if host:
      installation.installationHost = host

    try:
      session.add(installation)
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not add installation: %s' % (e))

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('InstalledComponent successfully added')

  def getInstalledComponents(self, matchFields=None, installationsInfo=False):
    """
    Returns a list with all the InstalledComponents with matches in the given
    fields
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances and may contain entries of the form
    'Component.attribute' or 'Host.attribute'
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    installationsInfo indicates whether information about the components and
    host taking part in the installation is to be provided
    """
    if matchFields is None:
      matchFields = {}

    session = self.session()

    result = self.__filterInstalledComponentsFields(session, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    installations = result['Value']
    if not installations:
      session.rollback()
      session.close()
      return S_ERROR('No matching Installations were found')

    dictInstallations = []
    for installation in installations:
      dictInstallations.append(installation.toDict(installationsInfo, installationsInfo)['Value'])

    session.commit()
    session.close()

    return S_OK(dictInstallations)

  def updateInstalledComponents(self, matchFields={}, updates={}):
    """
    Updates installations matching the given criteria
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances and may contain entries of the form
    'Component.attribute' or 'Host.attribute'
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    updates argument should be a dictionary with the Installation fields and
    their new updated values
    """

    session = self.session()

    result = self.__filterInstalledComponentsFields(session, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    installations = result['Value']

    for installation in installations:
      installation.fromDict(updates)

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('InstalledComponent(s) updated')

  def removeInstalledComponents(self, matchFields={}):
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

    session = self.session()

    result = self.__filterInstalledComponentsFields(session, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    installations = result['Value']

    for installation in installations:
      session.delete(installation)

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('InstalledComponents successfully removed')

  def addLog(self, newLog):
    """
    Add a new log to the database
    newLog argument should be a dictionary with the log fields and
    its values.
    Valid keys for newLog include fields that are present in a HostLogging object:
    HostName, DIRACVersion, Load1, Load5, ... of which only HostName is mandatory
    """
    session = self.session()

    log = HostLogging()
    log.fromDict(newLog)

    try:
      session.add(log)
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not add log: %s' % (e))

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('Log successfully added')

  def removeLogs(self, matchFields={}):
    """
    Removes logs with matches in the given fields
    matchFields argument should be a dictionary with the fields and values
    to match
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    matchFields argument can be empty to remove all the logs
    """

    session = self.session()

    result = self.__filterFields(session, HostLogging, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    for log in result['Value']:
      session.delete(log)

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('Logs successfully removed')

  def getLogs(self, matchFields={}):
    """
    Returns a list with all the logs with matches in the given fields
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    """

    session = self.session()

    result = self.__filterFields(session, HostLogging, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    logs = result['Value']
    if not logs:
      session.rollback()
      session.close()
      return S_ERROR('No matching logs were found')

    dictLogs = []
    for log in logs:
      dictLogs.append(log.toDict()['Value'])

    session.commit()
    session.close()
    return S_OK(dictLogs)

  def updateLogs(self, matchFields={}, updates={}):
    """
    Updates logs matching the given criteria
    matchFields argument should be a dictionary with the fields to match or
    empty to get all the instances
    matchFields also accepts fields of the form <Field.bigger> and
    <Field.smaller> to filter using > and < relationships
    updates argument should be a dictionary with the logs fields and
    their new updated values
    """

    session = self.session()

    result = self.__filterFields(session, HostLogging, matchFields)
    if not result['OK']:
      session.rollback()
      session.close()
      return result

    logs = result['Value']

    for log in logs:
      log.fromDict(updates)

    try:
      session.commit()
    except Exception as e:
      session.rollback()
      session.close()
      return S_ERROR('Could not commit changes: %s' % (e))

    session.close()
    return S_OK('Log(s) updated')
