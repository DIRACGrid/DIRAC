########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
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
    self.__baseSecurity = "/Registry"

    self.__userDN = ''
    self.__userGroup = ''
    self.__rpcClient = None
    self.__csMod = None

    self.__initialized = False
    self.__initialized = self.initialize()

  def __getProxyID( self ):
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
    idCert = retVal[ 'Value' ]
    self.__userDN = idCert.getSubjectDN()[ 'Value' ]
    self.__userGroup = idCert.getDIRACGroup()[ 'Value' ]
    return True

  def __getCertificateID( self ):
    certLocation = Locations.getHostCertificateAndKeyLocation()
    if not certLocation:
      gLogger.error( "No certificate found!" )
      return False
    chain = X509Chain()
    retVal = chain.loadChainFromFile( certLocation[ 0 ] )
    if not retVal[ 'OK' ]:
      gLogger.error( "Can't parse certificate!", retVal[ 'Message' ] )
      return False
    idCert = chain.getIssuerCert()[ 'Value' ]
    self.__userDN = idCert.getSubjectDN()[ 'Value' ]
    self.__userGroup = 'host'
    return True

  def initialize( self ):
    if self.__initialized:
      return True
    if not gConfig.useServerCertificate():
      res = self.__getProxyID()
    else:
      res = self.__getCertificateID()
    if not res:
      return False
    retVal = gConfig.getOption( "/DIRAC/Configuration/MasterServer" )
    if not retVal[ 'OK' ]:
      gLogger.warn( "Master server is not known. Is everything initialized?" )
      return False
    self.__rpcClient = RPCClient( gConfig.getValue( "/DIRAC/Configuration/MasterServer", "" ) )
    self.__csMod = Modificator( self.__rpcClient, "%s - %s" % ( self.__userGroup, self.__userDN ) )
    retVal = self.downloadCSData()
    if not retVal[ 'OK' ]:
      gLogger.error( "Can not download the remote cfg. Is everything initialized?" )
      return False
    return True

  def downloadCSData( self ):
    result = self.__csMod.loadFromRemote()
    if not result[ 'OK' ]:
      return result
    self.__csMod.updateGConfigurationData()
    return S_OK()

  def listUsers( self , group = False ):
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

  def listHosts( self ):
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
    if usersInGroup != None:
      userList = List.fromChar( usersInGroup, "," )
      userPos = userList.index( username )
      userList.pop( userPos )
      self.__csMod.setOptionValue( "%s/Groups/%s/Users" % ( self.__baseSecurity, group ), ",".join( userList ) )

  def __addUserToGroup( self, group, username ):
    """
    Add user to a group
    """
    usersInGroup = self.__csMod.getValue( "%s/Groups/%s/Users" % ( self.__baseSecurity, group ) )
    if usersInGroup != None:
      userList = List.fromChar( usersInGroup )
      if username not in userList:
        userList.append( username )
        self.__csMod.setOptionValue( "%s/Groups/%s/Users" % ( self.__baseSecurity, group ), ",".join( userList ) )
      else:
        gLogger.warn( "User %s is already in group %s" % ( username, group ) )

  def addUser( self, username, properties ):
    """
    Add a user to the cs
      - username
      - properties is a dict with keys:
        - DN
        - groups
        - <extra params>
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
      - username
      - properties is a dict with keys:
        - DN
        - Groups
        - <extra params>
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
    for prop in properties:
      if prop == "Groups":
        continue
      prevVal = self.__csMod.getValue( "%s/Users/%s/%s" % ( self.__baseSecurity, username, prop ) )
      if not prevVal or prevVal != properties[ prop ]:
        gLogger.info( "Setting %s property for user %s to %s" % ( prop, username, properties[ prop ] ) )
        self.__csMod.setOptionValue( "%s/Users/%s/%s" % ( self.__baseSecurity, username, prop ), properties[ prop ] )
        modifiedUser = True
    if 'Groups' in properties:
      groups = self.listGroups()['Value']
      for userGroup in properties[ 'Groups' ]:
        if not userGroup in groups:
          gLogger.error( "User %s group %s is not a valid group" % ( username, userGroup ) )
          return S_OK( False )
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

  def addGroup( self, groupname, properties ):
    """
    Add a group to the cs
      - groupname
      - properties is a dict with keys:
        - Users
        - Properties
        - <extra params>
    Returns True/False
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    if groupname in self.listGroups()['Value']:
      gLogger.error( "Group %s is already registered" % groupname )
      return S_OK( False )
    self.__csMod.createSection( "%s/Groups/%s" % ( self.__baseSecurity, groupname ) )
    for prop in properties:
      self.__csMod.setOptionValue( "%s/Groups/%s/%s" % ( self.__baseSecurity, groupname, prop ), properties[ prop ] )
    gLogger.info( "Registered group %s" % groupname )
    self.__csModified = True
    return S_OK( True )

  def modifyGroup( self, groupname, properties, createIfNonExistant = False ):
    """
    Modify a user
      - groupname
      - properties is a dict with keys:
        - Users
        - Properties
        - <extra params>
    Returns True/False
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    modifiedGroup = False
    groupData = self.describeGroups( [ groupname ] )['Value']
    if groupname not in groupData:
      if createIfNonExistant:
        gLogger.info( "Registering group %s" % groupname )
        return self.addGroup( groupname, properties )
      gLogger.error( "Group %s is not registered" % groupname )
      return S_OK( False )
    for prop in properties:
      prevVal = self.__csMod.getValue( "%s/Groups/%s/%s" % ( self.__baseSecurity, groupname, prop ) )
      if not prevVal or prevVal != properties[ prop ]:
        gLogger.info( "Setting %s property for group %s to %s" % ( prop, groupname, properties[ prop ] ) )
        self.__csMod.setOptionValue( "%s/Groups/%s/%s" % ( self.__baseSecurity, groupname, prop ), properties[ prop ] )
        modifiedGroup = True
    if modifiedGroup:
      gLogger.info( "Modified group %s" % groupname )
      self.__csModified = True
    else:
      gLogger.info( "Nothing to modify for group %s" % groupname )
    return S_OK( True )

  def addHost( self, hostname, properties ):
    """
    Add a host to the cs
      - hostname
      - properties is a dict with keys:
        - DN
        - Properties
        - <extra params>
    Returns True/False
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    for prop in ( "DN", ):
      if prop not in properties:
        gLogger.error( "Missing %s property for host %s" % ( prop, hostname ) )
        return S_OK( False )
    if hostname in self.listHosts()['Value']:
      gLogger.error( "Host %s is already registered" % hostname )
      return S_OK( False )
    self.__csMod.createSection( "%s/Hosts/%s" % ( self.__baseSecurity, hostname ) )
    for prop in properties:
      self.__csMod.setOptionValue( "%s/Hosts/%s/%s" % ( self.__baseSecurity, hostname, prop ), properties[ prop ] )
    gLogger.info( "Registered host %s" % hostname )
    self.__csModified = True
    return S_OK( True )

  def modifyHost( self, hostname, properties, createIfNonExistant = False ):
    """
    Modify a user
      - hostname
      - properties is a dict with keys:
        - DN
        - Properties
        - <extra params>
    Returns True/False
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    modifiedHost = False
    hostData = self.describeHosts( [ hostname ] )['Value']
    if hostname not in hostData:
      if createIfNonExistant:
        gLogger.info( "Registering host %s" % hostname )
        return self.addHost( hostname, properties )
      gLogger.error( "Host %s is not registered" % hostname )
      return S_OK( False )
    for prop in properties:
      prevVal = self.__csMod.getValue( "%s/Hosts/%s/%s" % ( self.__baseSecurity, hostname, prop ) )
      if not prevVal or prevVal != properties[ prop ]:
        gLogger.info( "Setting %s property for host %s to %s" % ( prop, hostname, properties[ prop ] ) )
        self.__csMod.setOptionValue( "%s/Hosts/%s/%s" % ( self.__baseSecurity, hostname, prop ), properties[ prop ] )
        modifiedHost = True
    if modifiedHost:
      gLogger.info( "Modified host %s" % hostname )
      self.__csModified = True
    else:
      gLogger.info( "Nothing to modify for host %s" % hostname )
    return S_OK( True )

  def syncUsersWithCFG( self, usersCFG ):
    """
    Sync users with the cfg contents. Usernames have to be sections containing
    DN, Groups, and extra properties as parameters
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

  def checkForUnexistantUsersInGroups( self ):
    allUsers = self.__csMod.getSections( "%s/Users" % self.__baseSecurity )
    allGroups = self.__csMod.getSections( "%s/Groups" % self.__baseSecurity )
    for group in allGroups:
      usersInGroup = self.__csMod.getValue( "%s/Groups/%s/Users" % ( self.__baseSecurity, group ) )
      if usersInGroup:
        filteredUsers = []
        usersInGroup = List.fromChar( usersInGroup )
        for user in usersInGroup:
          if user in allUsers:
            filteredUsers.append( user )
        self.__csMod.setOptionValue( "%s/Groups/%s/Users" % ( self.__baseSecurity, group ),
                                     ",".join( filteredUsers ) )

  def commitChanges( self, sortUsers = True ):
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    if self.__csModified:
      self.checkForUnexistantUsersInGroups()
      if sortUsers:
        self.sortUsersAndGroups()
      retVal = self.__csMod.commit()
      if not retVal[ 'OK' ]:
        gLogger.error( "Can't commit new data: %s" % retVal[ 'Message' ] )
        return retVal
      self.__csModified = False
      return self.downloadCSData()
    return S_OK()

  def commit( self ):
    """ Commit the accumulated changes to the CS server
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    if self.__csModified:
      retVal = self.__csMod.commit()
      if not retVal[ 'OK' ]:
        gLogger.error( "Can't commit new data: %s" % retVal[ 'Message' ] )
        return retVal
      self.__csModified = False
      return self.downloadCSData()
    return S_OK()

  def mergeFromCFG( self, cfg ):
    """ Merge the internal CFG data with the input
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    self.__csMod.mergeFromCFG( cfg )
    self.__csModified = True
    return S_OK()

  def modifyValue( self, optionPath, newValue ):
    """Modify an existing value at the specified options path.
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    prevVal = self.__csMod.getValue( optionPath )
    if not prevVal:
      return S_ERROR( 'Trying to set %s to %s but option does not exist' % ( optionPath, newValue ) )
    gLogger.verbose( "Changing %s from \n%s \nto \n%s" % ( optionPath, prevVal, newValue ) )
    self.__csMod.setOptionValue( optionPath, newValue )
    self.__csModified = True
    return S_OK( 'Modified %s' % optionPath )

  def setOption( self, optionPath, optionValue ):
    """Create an option at the specified path.
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    self.__csMod.setOptionValue( optionPath, optionValue )
    self.__csModified = True
    return S_OK( 'Created new option %s = %s' % ( optionPath, optionValue ) )


  def setOptionComment( self, optionPath, comment ):
    """Create an option at the specified path.
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    self.__csMod.setComment( optionPath, comment )
    self.__csModified = True
    return S_OK( 'Set option comment %s : %s' % ( optionPath, comment ) )

  def delOption( self, optionPath ):
    """ Delete an option 
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    if not self.__csMod.removeOption( optionPath ):
      return S_ERROR( "Couldn't delete option %s" % optionPath )
    self.__csModified = True
    return S_OK( 'Deleted option %s' % ( optionPath ) )

  def createSection( self, sectionPath, comment = "" ):
    """ Create a new section
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    self.__csMod.createSection( sectionPath )
    self.__csModified = True
    if comment:
      self.__csMod.setComment( sectionPath, comment )
    return S_OK()

  def delSection( self, sectionPath ):
    """ Delete a section
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    if not self.__csMod.removeSection( sectionPath ):
      return S_ERROR( "Could not delete section %s " % sectionPath )
    self.__csModified = True
    return S_OK()

  def mergeCFGUnderSection( self, sectionPath, cfg ):
    """ Merge the given cfg under a certain section
    """
    result = self.createSection( sectionPath )
    if not result[ 'OK' ]:
      return result
    if not self.__csMod.mergeSectionFromCFG( sectionPath, cfg ):
      return S_ERROR( "Could not merge cfg into section %s" % sectionPath )
    self.__csModified = True
    return S_OK()

  def mergeWithCFG( self, cfg ):
    """ Merge the given cfg with the current config
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    self.__csMod.mergeFromCFG( cfg )
    self.__csModified = True
    return S_OK()

  def getCurrentCFG( self ):
    """ Get the current CFG as it is
    """
    if not self.__initialized:
      return S_ERROR( "CSAPI didn't initialize properly" )
    return S_OK( self.__csMod.getCFG() )


