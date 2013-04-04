__RCSID__ = "$Id: UsersAndGroups.py 34413 2011-02-19 06:10:18Z rgracian $"
"""
  Update Users and Groups from VOMS on CS
"""
import os
from DIRAC.Core.Base.AgentModule                     import AgentModule
from DIRAC.ConfigurationSystem.Client.CSAPI          import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.Core.Security.VOMSService                 import VOMSService
from DIRAC.Core.Security                             import Locations, X509Chain
from DIRAC.Core.Utilities                            import List, Subprocess
from DIRAC                                           import S_OK, S_ERROR, gConfig

class UsersAndGroups( AgentModule ):

  def initialize( self ):
    self.am_setOption( "PollingTime", 3600 * 6 ) # Every 6 hours
    self.vomsSrv = VOMSService()
    self.proxyLocation = os.path.join( self.am_getWorkDirectory(), ".volatileId" )
    self.__adminMsgs = {}
    # print self.getLFCRegisteredDNs()
    return S_OK()

  def __generateProxy( self ):
    self.log.info( "Generating proxy..." )
    certLoc = Locations.getHostCertificateAndKeyLocation()
    if not certLoc:
      self.log.error( "Can not find certificate!" )
      return False
    chain = X509Chain.X509Chain()
    result = chain.loadChainFromFile( certLoc[0] )
    if not result[ 'OK' ]:
      self.log.error( "Can not load certificate file", "%s : %s" % ( certLoc[0], result[ 'Message' ] ) )
      return False
    result = chain.loadKeyFromFile( certLoc[1] )
    if not result[ 'OK' ]:
      self.log.error( "Can not load key file", "%s : %s" % ( certLoc[1], result[ 'Message' ] ) )
      return False
    result = chain.generateProxyToFile( self.proxyLocation, 3600 )
    if not result[ 'OK' ]:
      self.log.error( "Could not generate proxy file", result[ 'Message' ] )
      return False
    self.log.info( "Proxy generated" )
    return True

  def getLFCRegisteredDNs( self ):
    #Request a proxy
    if gConfig.useServerCertificate():
      if not self.__generateProxy():
        return False
    #Execute the call
    cmdEnv = dict( os.environ )
    cmdEnv['LFC_HOST'] = 'lfc-egee.in2p3.fr'
    if os.path.isfile( self.proxyLocation ):
      cmdEnv[ 'X509_USER_PROXY' ] = self.proxyLocation
    lfcDNs = []
    try:
      retlfc = Subprocess.systemCall( 30, ( 'lfc-listusrmap', ), env = cmdEnv )
      if not retlfc['OK']:
        self.log.fatal( 'Can not get LFC User List', retlfc['Message'] )
        return retlfc
      if retlfc['Value'][0]:
        self.log.fatal( 'Can not get LFC User List', retlfc['Value'][2] )
        return S_ERROR( "lfc-listusrmap failed" )
      else:
        for item in List.fromChar( retlfc['Value'][1], '\n' ):
          dn = item.split( ' ', 1 )[1]
          lfcDNs.append( dn )
      return S_OK( lfcDNs )
    finally:
      if os.path.isfile( self.proxyLocation ):
        self.log.info( "Destroying proxy..." )
        os.unlink( self.proxyLocation )

  def checkLFCRegisteredUsers( self, usersData ):
    self.log.info( "Checking LFC registered users" )
    usersToBeRegistered = {}
    result = self.getLFCRegisteredDNs()
    if not result[ 'OK' ]:
      self.log.error( "Could not get a list of registered DNs from LFC", result[ 'Message' ] )
      return result
    lfcDNs = result[ 'Value' ]
    for user in usersData:
      for userDN in usersData[ user ][ 'DN' ]:
        if userDN not in lfcDNs:
          self.log.info( 'DN "%s" need to be registered in LFC for user %s' % ( userDN, user ) )
          if user not in usersToBeRegistered:
            usersToBeRegistered[ user ] = []
          usersToBeRegistered[ user ].append( userDN )

    address = self.am_getOption( 'MailTo', 'graciani@ecm.ub.es' )
    fromAddress = self.am_getOption( 'mailFrom', 'graciani@ecm.ub.es' )
    if usersToBeRegistered:
      subject = 'New LFC Users found'
      self.log.info( subject, ", ".join( usersToBeRegistered ) )
      body = 'Command to add new entries into LFC: \n'
      body += 'login to volhcbXX and run : \n'
      body += 'source /afs/cern.ch/lhcb/software/releases/LBSCRIPTS/prod/InstallArea/scripts/LbLogin.csh \n\n'
      for lfcuser in usersToBeRegistered:
        for lfc_dn in usersToBeRegistered[lfcuser]:
          print lfc_dn
          body += 'add_DN_LFC --userDN="' + lfc_dn.strip() + '" --nickname=' + lfcuser + '\n'

      NotificationClient().sendMail( address, 'UsersAndGroupsAgent: %s' % subject, body, fromAddress )
    return S_OK()

  def execute( self ):
    result = self.__syncCSWithVOMS()
    mailMsg = ""
    if self.__adminMsgs[ 'Errors' ]:
      mailMsg += "\nErrors list:\n  %s" % "\n  ".join( self.__adminMsgs[ 'Errors' ] )
    if self.__adminMsgs[ 'Info' ]:
      mailMsg += "\nRun result:\n  %s" % "\n  ".join( self.__adminMsgs[ 'Info' ] )
    NotificationClient().sendMail( self.am_getOption( 'MailTo', 'graciani@ecm.ub.es' ),
                                   "UsersAndGroupsAgent run log", mailMsg,
                                   self.am_getOption( 'mailFrom', 'graciani@ecm.ub.es' ) )
    return result


  def __syncCSWithVOMS( self ):
    self.__adminMsgs = { 'Errors' : [], 'Info' : [] }

    #Get DIRAC VOMS Mapping
    self.log.info( "Getting DIRAC VOMS mapping" )
    mappingSection = '/Registry/VOMS/Mapping'
    ret = gConfig.getOptionsDict( mappingSection )
    if not ret['OK']:
      self.log.fatal( 'No VOMS to DIRAC Group Mapping Available' )
      return ret
    vomsMapping = ret['Value']
    self.log.info( "There are %s registered voms mappings in DIRAC" % len( vomsMapping ) )

    #Get VOMS VO name
    self.log.info( "Getting VOMS VO name" )
    result = self.vomsSrv.admGetVOName()
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve VOMS VO name' )
    voNameInVOMS = result[ 'Value' ]
    self.log.info( "VOMS VO Name is %s" % voNameInVOMS )

    #Get VOMS roles
    self.log.info( "Getting the list of registered roles in VOMS" )
    result = self.vomsSrv.admListRoles()
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve registered roles in VOMS' )
    rolesInVOMS = result[ 'Value' ]
    self.log.info( "There are %s registered roles in VOMS" % len( rolesInVOMS ) )
    print rolesInVOMS
    rolesInVOMS.append( '' )

    #Map VOMS roles
    vomsRoles = {}
    for role in rolesInVOMS:
      if role:
        role = "%s/%s" % ( voNameInVOMS, role )
      else:
        role = voNameInVOMS
      groupsForRole = []
      for group in vomsMapping:
        if vomsMapping[ group ] == role:
          groupsForRole.append( group )
      if groupsForRole:
        vomsRoles[ role ] = { 'Groups' : groupsForRole, 'Users' : [] }
    self.log.info( "DIRAC valid VOMS roles are:\n\t", "\n\t ".join( vomsRoles.keys() ) )

    #Get DIRAC users
    self.log.info( "Getting the list of registered users in DIRAC" )
    csapi = CSAPI()
    ret = csapi.listUsers()
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve current list of Users' )
      return ret
    currentUsers = ret['Value']

    ret = csapi.describeUsers( currentUsers )
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve current User description' )
      return ret
    currentUsers = ret['Value']
    self.__adminMsgs[ 'Info' ].append( "There are %s registered users in DIRAC" % len( currentUsers ) )
    self.log.info( "There are %s registered users in DIRAC" % len( currentUsers ) )

    #Get VOMS user entries
    self.log.info( "Getting the list of registered user entries in VOMS" )
    result = self.vomsSrv.admListMembers()
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve registered user entries in VOMS' )
    usersInVOMS = result[ 'Value' ]
    self.__adminMsgs[ 'Info' ].append( "There are %s registered user entries in VOMS" % len( usersInVOMS ) )
    self.log.info( "There are %s registered user entries in VOMS" % len( usersInVOMS ) )

    #Consolidate users by nickname
    usersData = {}
    newUserNames = []
    knownUserNames = []
    obsoleteUserNames = []
    self.log.info( "Retrieving usernames..." )
    usersInVOMS.sort()
    for iUPos in range( len( usersInVOMS ) ):
      userName = ''
      user = usersInVOMS[ iUPos ]
      for oldUser in currentUsers:
        if user[ 'DN' ].strip() in List.fromChar( currentUsers[oldUser][ 'DN' ] ):
          userName = oldUser
      if not userName:
        result = self.vomsSrv.attGetUserNickname( user[ 'DN' ], user[ 'CA' ] )
        if result[ 'OK' ]:
          userName = result[ 'Value' ]
        else:
          self.__adminMsgs[ 'Errors' ].append( "Could not retrieve nickname for DN %s" % user[ 'DN' ] )
          self.log.error( "Could not get nickname for DN %s" % user[ 'DN' ] )
          userName = user[ 'mail' ][:user[ 'mail' ].find( '@' )]
      if not userName:
        self.log.error( "Empty nickname for DN %s" % user[ 'DN' ] )
        self.__adminMsgs[ 'Errors' ].append( "Empty nickname for DN %s" % user[ 'DN' ] )
        continue
      self.log.info( " (%02d%%) Found username %s : %s " % ( ( iUPos * 100 / len( usersInVOMS ) ), userName, user[ 'DN' ] ) )
      if userName not in usersData:
        usersData[ userName ] = { 'DN': [], 'CA': [], 'Email': [], 'Groups' : ['user'] }
      for key in ( 'DN', 'CA', 'mail' ):
        value = user[ key ]
        if value:
          if key == "mail":
            List.appendUnique( usersData[ userName ][ 'Email' ], value )
          else:
            usersData[ userName ][ key ].append( value.strip() )
      if userName not in currentUsers:
        List.appendUnique( newUserNames, userName )
      else:
        List.appendUnique( knownUserNames, userName )
    self.log.info( "Finished retrieving usernames" )

    if newUserNames:
      self.log.info( "There are %s new users" % len( newUserNames ) )
    else:
      self.log.info( "There are no new users" )

    #Get the list of users for each group
    result = csapi.listGroups()
    if not result[ 'OK' ]:
      self.log.error( "Could not get the list of groups in DIRAC", result[ 'Message' ] )
      return result
    staticGroups = result[ 'Value' ]
    vomsGroups = []
    self.log.info( "Mapping users in VOMS to groups" )
    for vomsRole in vomsRoles:
      self.log.info( "  Getting users for role %s" % vomsRole )
      groupsForRole = vomsRoles[ vomsRole ][ 'Groups' ]
      vomsMap = vomsRole.split( "Role=" )
      for g in groupsForRole:
        if g in staticGroups:
          staticGroups.pop( staticGroups.index( g ) )
        else:
          vomsGroups.append( g )
      if len( vomsMap ) == 1:
        # no Role
        users = usersInVOMS
      else:
        vomsGroup = "Role=".join( vomsMap[:-1] )
        if vomsGroup[-1] == "/":
          vomsGroup = vomsGroup[:-1]
        vomsRole = "Role=%s" % vomsMap[-1]
        result = self.vomsSrv.admListUsersWithRole( vomsGroup, vomsRole )
        if not result[ 'OK' ]:
          errorMsg = "Could not get list of users for VOMS %s" % ( vomsMapping[ group ] )
          self.__adminMsgs[ 'Errors' ].append( errorMsg )
          self.log.error( errorMsg, result[ 'Message' ] )
          return result
        users = result['Value']
      numUsersInGroup = 0

      for vomsUser in users:
        for userName in usersData:
          if vomsUser[ 'DN' ] in usersData[ userName ][ 'DN' ]:
            numUsersInGroup += 1
            usersData[ userName ][ 'Groups' ].extend( groupsForRole )
      infoMsg = "There are %s users in group(s) %s for VOMS Role %s" % ( numUsersInGroup, ",".join( groupsForRole ), vomsRole )
      self.__adminMsgs[ 'Info' ].append( infoMsg )
      self.log.info( "  %s" % infoMsg )

    self.log.info( "Checking static groups" )
    staticUsers = []
    for group in staticGroups:
      self.log.info( "  Checking static group %s" % group )
      numUsersInGroup = 0
      result = csapi.listUsers( group )
      if not result[ 'OK' ]:
        self.log.error( "Could not get the list of users in DIRAC group %s" % group , result[ 'Message' ] )
        return result
      for userName in result[ 'Value' ]:
        if userName in usersData:
          numUsersInGroup += 1
          usersData[ userName ][ 'Groups' ].append( group )
        else:
          if group not in vomsGroups and userName not in staticUsers:
            staticUsers.append( userName )
      infoMsg = "There are %s users in group %s" % ( numUsersInGroup, group )
      self.__adminMsgs[ 'Info' ].append( infoMsg )
      self.log.info( "  %s" % infoMsg )
    if staticUsers:
      infoMsg = "There are %s static users: %s" % ( len( staticUsers ) , ', '.join( staticUsers ) )
      self.__adminMsgs[ 'Info' ].append( infoMsg )
      self.log.info( "%s" % infoMsg )

    for user in currentUsers:
      if user not in usersData and user not in staticUsers:
        self.log.info( 'User %s is no longer valid' % user )
        obsoleteUserNames.append( user )

    #Do the CS Sync
    self.log.info( "Updating CS..." )
    ret = csapi.downloadCSData()
    if not ret['OK']:
      self.log.fatal( 'Can not update from CS', ret['Message'] )
      return ret

    usersWithMoreThanOneDN = {}
    for user in usersData:
      csUserData = dict( usersData[ user ] )
      if len( csUserData[ 'DN' ] ) > 1:
        usersWithMoreThanOneDN[ user ] = csUserData[ 'DN' ]
      result = csapi.describeUsers( [ user ] )
      if result[ 'OK' ]:
        if result[ 'Value' ]:
          prevUser = result[ 'Value' ][ user ]
          prevDNs = List.fromChar( prevUser[ 'DN' ] )
          newDNs = csUserData[ 'DN' ]
          for DN in newDNs:
            if DN not in prevDNs:
              self.__adminMsgs[ 'Info' ].append( "User %s has new DN %s" % ( user, DN ) )
          for DN in prevDNs:
            if DN not in newDNs:
              self.__adminMsgs[ 'Info' ].append( "User %s has lost a DN %s" % ( user, DN ) )
        else:
          newDNs = csUserData[ 'DN' ]
          for DN in newDNs:
            self.__adminMsgs[ 'Info' ].append( "New user %s has new DN %s" % ( user, DN ) )
      for k in ( 'DN', 'CA', 'Email' ):
        csUserData[ k ] = ", ".join( csUserData[ k ] )
      result = csapi.modifyUser( user, csUserData, createIfNonExistant = True )
      if not result[ 'OK' ]:
        self.__adminMsgs[ 'Error' ].append( "Cannot modify user %s: %s" % ( user, result[ 'Message' ] ) )
        self.log.error( "Cannot modify user %s" % user )

    if usersWithMoreThanOneDN:
      self.__adminMsgs[ 'Info' ].append( "\nUsers with more than one DN:" )
      for uwmtod in sorted( usersWithMoreThanOneDN ):
        self.__adminMsgs[ 'Info' ].append( "  %s" % uwmtod )
        self.__adminMsgs[ 'Info' ].append( "    + DN list:" )
        for DN in usersWithMoreThanOneDN[uwmtod]:
          self.__adminMsgs[ 'Info' ].append( "      - %s" % DN )

    if obsoleteUserNames:
      self.__adminMsgs[ 'Info' ].append( "\nObsolete users:" )
      address = self.am_getOption( 'MailTo', 'graciani@ecm.ub.es' )
      fromAddress = self.am_getOption( 'mailFrom', 'graciani@ecm.ub.es' )
      subject = 'Obsolete LFC Users found'
      body = 'Delete entries into LFC: \n'
      for obsoleteUser in obsoleteUserNames:
        self.log.info( subject, ", ".join( obsoleteUserNames ) )
        body += 'for ' + obsoleteUser + '\n'
        self.__adminMsgs[ 'Info' ].append( "  %s" % obsoleteUser )
      self.log.info( "Deleting %s users" % len( obsoleteUserNames ) )
      NotificationClient().sendMail( address, 'UsersAndGroupsAgent: %s' % subject, body, fromAddress )
      csapi.deleteUsers( obsoleteUserNames )



    if newUserNames:
      self.__adminMsgs[ 'Info' ].append( "\nNew users:" )
      for newUser in newUserNames:
        self.__adminMsgs[ 'Info' ].append( "  %s" % newUser )
        self.__adminMsgs[ 'Info' ].append( "    + DN list:" )
        for DN in usersData[newUser][ 'DN' ]:
          self.__adminMsgs[ 'Info' ].append( "      - %s" % DN )
        self.__adminMsgs[ 'Info' ].append( "    + EMail: %s" % usersData[newUser][ 'Email' ] )


    result = csapi.commitChanges()
    if not result[ 'OK' ]:
      self.log.error( "Could not commit configuration changes", result[ 'Message' ] )
      return result
    self.log.info( "Configuration committed" )

    #LFC Check
    if self.am_getOption( "LFCCheckEnabled", True ):
      result = self.checkLFCRegisteredUsers( usersData )
      if not result[ 'OK' ]:
        return result

    return S_OK()
