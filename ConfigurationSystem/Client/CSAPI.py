
import types
from DIRAC.ConfigurationSystem.private.Modificator import Modificator
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities import GridCredentials, List
from DIRAC import gLogger, gConfig

class CSAPI:

  def __init__(self):
    """
    Initialization function
    """
    proxyLocation = GridCredentials.getGridProxy()
    if not proxyLocation:
      gLogger.fatal( "No proxy found!" )
      raise Exception( "No proxy found" )
    proxy = GridCredentials.X509Certificate()
    if not proxy.loadFromFile( proxyLocation ):
      gLogger.fatal( "Can't read proxy!", proxyLocation )
      raise Exception( "Can't read proxy!" )
    retVal = proxy.getIssuerDN()
    if not retVal[ 'OK' ]:
      gLogger.error( "Can't parse proxy!", retVal[ 'Message' ] )
      raise Exception( "Can't parse proxy!" )
    self.__userDN = retVal[ 'Value' ]
    self.__userGroup = GridCredentials.getDIRACGroup()
    retVal = gConfig.getOption( "/DIRAC/Configuration/MasterServer")
    if not retVal[ 'OK' ]:
      gLogger.fatal( "Master server is not known. Is everything initialized?" )
      raise Exception( "Master server is now known" )
    self.__rpcClient = RPCClient( gConfig.getValue( "/DIRAC/Configuration/MasterServer", "" ) )
    self.__csMod = Modificator( self.__rpcClient, "%s - %s" % ( self.__userGroup, self.__userDN ) )
    retVal = self.__csMod.loadFromRemote()
    if not retVal[ 'OK' ]:
      gLogger.fatal( "Cnn not download the remote cfg. Is everything initialized?" )
      raise Exception( "Can not download the remote cfg" )

  def listUsers(self):
    return [ user for user in self.__csMod.getSections( "/Users" ) if user.find( "host-" ) == -1 ]

  def listHosts(self):
    return [ host for host in self.__csMod.getSections( "/Users" ) if host.find( "host-" ) == 0 ]

  def describeUsers( self, users = False ):
    return self.__describeEntity( users )

  def describeHosts( self, hosts = False ):
    return self.__describeEntity( hosts )

  def __describeEntity( self, mask, hosts = False ):
    if hosts:
      hostFindIndex = 0
    else:
      hostFindIndex = -1
    if mask:
      entities = [ entity for entity in self.__csMod.getSections( "/Users" ) if entity.find( "host-" ) == hostFindIndex and entity in mask ]
    else:
      entities = [ entity for entity in self.__csMod.getSections( "/Users" ) if entity.find( "host-" ) == hostFindIndex ]
    entitiesDict = {}
    groupsDict = self.describeGroups()
    for entity in entities:
      entitiesDict[ entity ] = { 'groups' : [] }
      for option in self.__csMod.getOptions( "/Users/%s" % entity ):
        entitiesDict[ entity ][ option ] = self.__csMod.getValue( "/Users/%s/%s" % ( entity, option ) )
      for group in groupsDict:
        if 'users' in groupsDict[ group ] and entity in groupsDict[ group ][ 'users' ]:
          entitiesDict[ entity ][ 'groups' ].append( group )
      entitiesDict[ entity ][ 'groups' ].sort()
    return entitiesDict

  def listGroups( self ):
    """
    List all groups
    """
    return self.__csMod.getSections( "/Groups" )

  def describeGroups( self, mask = False ):
    """
    List all groups that are in the mask (or all if no mask) with their properties
    """
    groups = [ group for group in self.__csMod.getSections( "/Groups" ) if not mask or ( mask and group in mask ) ]
    groupsDict = {}
    for group in groups:
      groupsDict[ group ] = {}
      for option in self.__csMod.getOptions( "/Groups/%s" % group ):
        groupsDict[ group ][ option ] = self.__csMod.getValue( "/Groups/%s/%s" % ( group, option ) )
        if option in ( "users", "Properties" ):
          groupsDict[ group ][ option ] = List.fromChar( groupsDict[ group ][ option ] )
    return groupsDict

  def deleteUsers( self, users ):
    """
    Delete a user/s can receive as a param either a string or a list
    """
    if type( users ) == types.StringType:
      users = [ users ]
    usersData = self.describeUsers( users )
    for username in users:
      userGroups = usersData[ username ][ 'groups' ]
      for group in userGroups:
        self.removeUserFromGroup( group, username )
        gLogger.info( "Deleted user %s from group %s" % ( username, group ) )
      if not username in usersData:
        gLogger.warning( "User %s does not exist" )
        continue
      self.__csMod.removeSection( "/Users/%s" % username )
      gLogger.info( "Deleted user %s" % username )

  def removeUserFromGroup( self, group, username ):
    """
    Remove user from a group
    """
    usersInGroup = self.__csMod.getValue( "/Groups/%s/users" % group )
    if usersInGroup:
      userList = List.fromChar( usersInGroup, "," )
      userPos = userList.index( username )
      userList.pop( userPos )
      self.__csMod.setOptionValue( "/Groups/%s/users" % group, ",".join( userList ) )

  def addUser( self, username, properties ):
    """
    Add a user to the cs
      -username
      -properties is a dict with keys:
        DN
        groups
        <extra params>
      Returns True/False
    """
    for prop in ( "DN", "groups" ):
      if prop not in properties:
        gLogger.error( "Missing %s property for user %s" % ( prop, username ) )
        return False
    if username in self.listUsers():
      gLogger.error( "User %s is already registered" % username )
      return False
    groups = self.listGroups()
    if type( properties[ 'groups' ] ) not in ( types.ListType, types.TupleType ):
      gLogger.error( "Groups for user %s have to be a list or a tuple" % username )
      return False
    for userGroup in properties[ 'groups' ]:
      if not userGroup in groups:
        gLogger.error( "User %s group %s is not a valid group" % ( username, userGroup ) )
        return False
    self.__csMod.createSection( "/Users/%s" % username )
    for prop in properties:
      if prop == "groups":
        continue
      self.__csMod.setOptionValue( "/Users/%s/%s" % ( username, prop ), properties[ prop ] )
    for userGroup in properties[ 'groups' ]:
      self.addUserToGroup( userGroup, username )
    gLogger.info( "Added user %s" % username )
    return True

  def addUserToGroup( self, group, username ):
    """
    Add user to a group
    """
    usersInGroup = self.__csMod.getValue( "/Groups/%s/users" % group )
    if usersInGroup:
      userList = List.fromChar( usersInGroup )
      try:
        userPos = userList.index( username )
      except ValueError:
        userList.append( username )
        self.__csMod.setOptionValue( "/Groups/%s/users" % group, ",".join( userList ) )
      else:
        gLogger.warning( "User %s is already in group %s" % ( username, group ) )

  def commitChanges(self):
    retVal = self.__csMod.commit()
    if not retVal[ 'OK' ]:
      gLogger.error( "Can't commit new data: %s" % retVal[ 'Message' ] )
      return False
    return True
