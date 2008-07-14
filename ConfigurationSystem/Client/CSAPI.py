
import types
from DIRAC.ConfigurationSystem.private.Modificator import Modificator
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities import List
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security import Locations
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class CSAPI:

  def __init__( self ):
    """
    Initialization function
    """
    self.__csModified = False
    self.__baseSecurity = "/Security"
    self.__initialized = self.initialize()

  def initialize( self ):
    proxyLocation = Locations.getProxyLocation()
    if not proxyLocation:
      gLogger.error( "No proxy found!" )
      return False
    chain = X509Chain()
    if not chain.loadProxyFromFile( proxyLocation ):
      gLogger.error( "Can't read proxy!", proxyLocation )
      return False
    retVal = chain.getIssuerCert()
    if not retVal[ 'OK' ]:
      gLogger.error( "Can't parse proxy!", retVal[ 'Message' ] )
      return False
    issuerCert = retVal[ 'Value' ]
    self.__userDN = issuerCert.getSubjectDN()[ 'Value' ]
    self.__userGroup = issuerCert.getDIRACGroup()[ 'Value' ]
    retVal = gConfig.getOption( "/DIRAC/Configuration/MasterServer")
    if not retVal[ 'OK' ]:
      gLogger.error( "Master server is not known. Is everything initialized?" )
      return False
    self.__rpcClient = RPCClient( gConfig.getValue( "/DIRAC/Configuration/MasterServer", "" ) )
    self.__csMod = Modificator( self.__rpcClient, "%s - %s" % ( self.__userGroup, self.__userDN ) )
    retVal = self.downloadCSData()
    if not retVal[ 'OK' ]:
      gLogger.error( "Can not download the remote cfg. Is everything initialized?" )
      return False
    return True

  def downloadCSData( self ):
    return self.__csMod.loadFromRemote()

  def listUsers(self , group = False ):
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    if not group:
      return S_OK( self.__csMod.getSections( "%s/Users" % self.__baseSecurity ) )
    else:
      users = self.__csMod.getValue( "%s/Groups/%s/Users" % ( self.__baseSecurity, group ) )
      if not users:
        return S_OK( [] )
      else:
        return S_OK( List.fromChar( users ) )

  def listHosts(self):
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    return S_OK( self.__csMod.getSections( "%s/Hosts" % self.__baseSecurity ) )

  def describeUsers( self, users = False ):
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    return S_OK( self.__describeEntity( users ) )

  def describeHosts( self, hosts = False ):
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    return S_OK( self.__describeEntity( hosts, True ) )

  def __describeEntity( self, mask, hosts = False ):
    if hosts:
      csSection = "%s/Hosts" % self.__baseSecurity
    else:
      csSection = "%s/Users" % self.__baseSecurity
    if mask:
      entities = [ entity for entity in self.__csMod.getSections( csSection ) if entity in mask ]
    else:
      entities = self.__csMod.getSections( csSection )
    entitiesDict = {}
    for entity in entities:
      entitiesDict[ entity ] = {}
      for option in self.__csMod.getOptions( "%s/%s" % ( csSection, entity ) ):
        entitiesDict[ entity ][ option ] = self.__csMod.getValue( "%s/%s/%s" % ( csSection, entity, option ) )
      if not hosts:
        groupsDict = self.describeGroups()[ 'Value' ]
        entitiesDict[ entity ][ 'Groups' ] = []
        for group in groupsDict:
          if 'Users' in groupsDict[ group ] and entity in groupsDict[ group ][ 'Users' ]:
            entitiesDict[ entity ][ 'Groups' ].append( group )
        entitiesDict[ entity ][ 'Groups' ].sort()
    return entitiesDict

  def listGroups( self ):
    """
    List all groups
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    return S_OK( self.__csMod.getSections( "%s/Groups" % self.__baseSecurity ) )

  def describeGroups( self, mask = False ):
    """
    List all groups that are in the mask (or all if no mask) with their properties
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    groups = [ group for group in self.__csMod.getSections( "%s/Groups" % self.__baseSecurity ) if not mask or ( mask and group in mask ) ]
    groupsDict = {}
    for group in groups:
      groupsDict[ group ] = {}
      for option in self.__csMod.getOptions( "%s/Groups/%s" % ( self.__baseSecurity, group ) ):
        groupsDict[ group ][ option ] = self.__csMod.getValue( "%s/Groups/%s/%s" % ( self.__baseSecurity, group, option ) )
        if option in ( "Users", "Properties" ):
          groupsDict[ group ][ option ] = List.fromChar( groupsDict[ group ][ option ] )
    return S_OK( groupsDict )

  def deleteUsers( self, users ):
    """
    Delete a user/s can receive as a param either a string or a list
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    if type( users ) == types.StringType:
      users = [ users ]
    usersData = self.describeUsers( users )['Value']
    for username in users:
      if not username in usersData:
        gLogger.warn( "User %s does not exist" )
        continue
      userGroups = usersData[ username ][ 'Groups' ]
      for group in userGroups:
        self.__removeUserFromGroup( group, username )
        gLogger.info( "Deleted user %s from group %s" % ( username, group ) )
      self.__csMod.removeSection( "%s/Users/%s" % ( self.__baseSecurity, username ) )
      gLogger.info( "Deleted user %s" % username )
      self.__csModified = True
    return S_OK( True )

  def __removeUserFromGroup( self, group, username ):
    """
    Remove user from a group
    """
    usersInGroup = self.__csMod.getValue( "%s/Groups/%s/Users" % ( self.__baseSecurity, group ) )
    if usersInGroup:
      userList = List.fromChar( usersInGroup, "," )
      userPos = userList.index( username )
      userList.pop( userPos )
      self.__csMod.setOptionValue( "%s/Groups/%s/Users" % ( self.__baseSecurity, group ), ",".join( userList ) )

  def __addUserToGroup( self, group, username ):
    """
    Add user to a group
    """
    usersInGroup = self.__csMod.getValue( "%s/Groups/%s/Users" % ( self.__baseSecurity, group ) )
    if usersInGroup:
      userList = List.fromChar( usersInGroup )
      try:
        userPos = userList.index( username )
      except ValueError:
        userList.append( username )
        self.__csMod.setOptionValue( "%s/Groups/%s/Users" % ( self.__baseSecurity, group ), ",".join( userList ) )
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
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    for prop in ( "DN", "Groups" ):
      if prop not in properties:
        gLogger.error( "Missing %s property for user %s" % ( prop, username ) )
        return S_OK( False )
    if username in self.listUsers()['Value']:
      gLogger.error( "User %s is already registered" % username )
      return S_OK( False )
    groups = self.listGroups()['Value']
    for userGroup in properties[ 'Groups' ]:
      if not userGroup in groups:
        gLogger.error( "User %s group %s is not a valid group" % ( username, userGroup ) )
        return S_OK( False )
    self.__csMod.createSection( "%s/Users/%s" % ( self.__baseSecurity, username ) )
    for prop in properties:
      if prop == "Groups":
        continue
      self.__csMod.setOptionValue( "%s/Users/%s/%s" % ( self.__baseSecurity, username, prop ), properties[ prop ] )
    for userGroup in properties[ 'Groups' ]:
      gLogger.info( "Added user %s to group %s" % ( username, userGroup ) )
      self.__addUserToGroup( userGroup, username )
    gLogger.info( "Registered user %s" % username )
    self.__csModified = True
    return S_OK( True )

  def modifyUser( self, username, properties, createIfNonExistant = False ):
    """
    Modify a user
      -username
      -properties is a dict with keys:
        DN
        Groups
        <extra params>
      Returns True/False
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    modifiedUser = False
    userData = self.describeUsers( [ username ] )['Value']
    if username not in userData:
      if createIfNonExistant:
        gLogger.info( "Registering user %s" % username )
        return self.addUser( username, properties )
      gLogger.error( "User %s is not registered" % username )
      return S_OK( False )
    groups = self.listGroups()['Value']
    for userGroup in properties[ 'Groups' ]:
      if not userGroup in groups:
        gLogger.error( "User %s group %s is not a valid group" % ( username, userGroup ) )
        return S_OK( False )
    for prop in properties:
      if prop == "Groups":
        continue
      prevVal = self.__csMod.getValue( "%s/Users/%s/%s" % ( self.__baseSecurity, username, prop ) )
      if not prevVal or prevVal != properties[ prop ]:
        gLogger.info( "Setting %s property for user %s to %s" % ( prop, username, properties[ prop ] ) )
        self.__csMod.setOptionValue( "%s/Users/%s/%s" % ( self.__baseSecurity, username, prop ), properties[ prop ] )
        modifiedUser = True
    groupsToBeDeletedFrom = []
    groupsToBeAddedTo = []
    for prevGroup in userData[ username ][ 'Groups' ]:
      if prevGroup not in properties[ 'Groups' ]:
        groupsToBeDeletedFrom.append( prevGroup )
        modifiedUser = True
    for newGroup in properties[ 'Groups' ]:
      if newGroup not in userData[ username ][ 'Groups' ]:
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
    return S_OK( True )

  def syncUsersWithCFG( self, usersCFG ):
    """
    Sync users with the cfg contents. Usernames have to be sections containing
    DN, groups, and extra properties as parameters
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    done = True
    for user in usersCFG.listSections():
      properties = {}
      propList = usersCFG[ user ].listOptions()
      for prop in propList:
        if prop == "Groups":
          properties[ prop ] = List.fromChar( usersCFG[ user ][ prop ] )
        else:
          properties[ prop ] = usersCFG[ user ][ prop ]
      if not self.modifyUser( user, properties, createIfNonExistant = True ):
        done = False
    return S_OK( done )

  def sortUsersAndGroups( self ):
    self.__csMod.sortAlphabetically( "%s/Users" % self.__baseSecurity )
    self.__csMod.sortAlphabetically( "%s/Hosts" % self.__baseSecurity )
    for group in self.__csMod.getSections( "%s/Groups" % self.__baseSecurity ):
      usersOptionPath = "%s/Groups/%s/Users" % ( self.__baseSecurity, group )
      users = self.__csMod.getValue( usersOptionPath )
      usersList = List.fromChar( users )
      usersList.sort()
      sortedUsers = ", ".join( usersList )
      if users != sortedUsers:
        self.__csMod.setOptionValue( usersOptionPath, sortedUsers )

  def commitChanges( self, sortUsers = True ):
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    if self.__csModified:
      if sortUsers:
        self.sortUsersAndGroups()
      retVal = self.__csMod.commit()
      if not retVal[ 'OK' ]:
        gLogger.error( "Can't commit new data: %s" % retVal[ 'Message' ] )
        return retVal
      self.__csModified = False
    return S_OK( True )