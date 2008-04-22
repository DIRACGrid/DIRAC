
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
    self.__csModified = False


  def listUsers(self , group = False ):
    if not group:
      return [ user for user in self.__csMod.getSections( "/Users" ) if user.find( "host-" ) == -1 ]
    else:
      users = self.__csMod.getValue( "/Groups/%s/users" % group )
      if not users:
        return []
      else:
        return List.fromChar( users )

  def listHosts(self):
    return [ host for host in self.__csMod.getSections( "/Users" ) if host.find( "host-" ) == 0 ]

  def describeUsers( self, users = False ):
    return self.__describeEntity( users )

  def describeHosts( self, hosts = False ):
    return self.__describeEntity( hosts, True )

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
      if not username in usersData:
        gLogger.warn( "User %s does not exist" )
        continue
      userGroups = usersData[ username ][ 'groups' ]
      for group in userGroups:
        self.__removeUserFromGroup( group, username )
        gLogger.info( "Deleted user %s from group %s" % ( username, group ) )
      self.__csMod.removeSection( "/Users/%s" % username )
      gLogger.info( "Deleted user %s" % username )
      self.__csModified = True
    return True

  def __removeUserFromGroup( self, group, username ):
    """
    Remove user from a group
    """
    usersInGroup = self.__csMod.getValue( "/Groups/%s/users" % group )
    if usersInGroup:
      userList = List.fromChar( usersInGroup, "," )
      userPos = userList.index( username )
      userList.pop( userPos )
      self.__csMod.setOptionValue( "/Groups/%s/users" % group, ",".join( userList ) )

  def __addUserToGroup( self, group, username ):
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
      gLogger.info( "Added user %s to group %s" % ( username, userGroup ) )
      self.__addUserToGroup( userGroup, username )
    gLogger.info( "Registered user %s" % username )
    self.__csModified = True
    return True

  def modifyUser( self, username, properties, createIfNonExistant = False ):
    """
    Modify a user
      -username
      -properties is a dict with keys:
        DN
        groups
        <extra params>
      Returns True/False
    """
    modifiedUser = False
    userData = self.describeUsers( [ username ] )
    if username not in userData:
      if createIfNonExistant:
        gLogger.info( "Registering user %s" % username )
        return self.addUser( username, properties )
      gLogger.error( "User %s is not registered" % username )
      return False
    groups = self.listGroups()
    if type( properties[ 'groups' ] ) not in ( types.ListType, types.TupleType ):
      gLogger.error( "Groups for user %s have to be a list or a tuple" % username )
      return False
    for userGroup in properties[ 'groups' ]:
      if not userGroup in groups:
        gLogger.error( "User %s group %s is not a valid group" % ( username, userGroup ) )
        return False
    for prop in properties:
      if prop == "groups":
        continue
      prevVal = self.__csMod.getValue( "/Users/%s/%s" % ( username, prop ) )
      if not prevVal or prevVal != properties[ prop ]:
        gLogger.info( "Setting %s property for user %s to %s" % ( prop, username, properties[ prop ] ) )
        self.__csMod.setOptionValue( "/Users/%s/%s" % ( username, prop ), properties[ prop ] )
        modifiedUser = True
    groupsToBeDeletedFrom = []
    groupsToBeAddedTo = []
    for prevGroup in userData[ username ][ 'groups' ]:
      if prevGroup not in properties[ 'groups' ]:
        groupsToBeDeletedFrom.append( prevGroup )
        modifiedUser = True
    for newGroup in properties[ 'groups' ]:
      if newGroup not in userData[ username ][ 'groups' ]:
        groupsToBeAddedTo.append( newGroup )
        modifiedUser = True
    for group in groupsToBeDeletedFrom:
      self.__removeUserFromGroup( group, username )
      gLogger.info( "Removed user %s from group %s" % ( username, group ) )
    for group in groupsToBeAddedTo:
      self.__addUserToGroup( group, username )
      gLogger.info( "Added user %s to group %s" % ( username, group ) )
    if modifiedUser:
      gLogger.info( "Modified user %s" % username )
      self.__csModified = True
    else:
      gLogger.info( "Nothing to modify for user %s" % username )
    return True

  def syncUsersWithCFG( self, usersCFG ):
    """
    Sync users with the cfg contents. Usernames have to be sections containing
    DN, groups, and extra properties as parameters
    """
    done = True
    for user in usersCFG.listSections():
      properties = {}
      propList = usersCFG[ user ].listOptions()
      for prop in propList:
        if prop == "groups":
          properties[ prop ] = List.fromChar( usersCFG[ user ][ prop ] )
        else:
          properties[ prop ] = usersCFG[ user ][ prop ]
      if not self.modifyUser( user, properties, createIfNonExistant = True ):
        done = False
    return done

  def commitChanges(self):
    if self.__csModified:
      retVal = self.__csMod.commit()
      if not retVal[ 'OK' ]:
        gLogger.error( "Can't commit new data: %s" % retVal[ 'Message' ] )
        return False
      self.__csModified = False
    return True
