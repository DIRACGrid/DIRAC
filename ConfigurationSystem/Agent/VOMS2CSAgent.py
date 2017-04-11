"""
  VOMS2CSAgent performs the following operations:

    - Adds new users for the given VO taking into account the VO VOMS information
    - Updates the data in the CS for existing users including DIRAC group membership
    -
"""

from collections import defaultdict

from DIRAC.Core.Base.AgentModule                       import AgentModule
from DIRAC.ConfigurationSystem.Client.CSAPI            import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient   import NotificationClient
from DIRAC.Core.Security.VOMSService                   import VOMSService
from DIRAC                                             import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption, getUserOption, \
                                                              getVOMSRoleGroupMapping, getUsersInVO, \
                                                              getAllUsers
from DIRAC.Core.Utilities.Proxy                        import executeWithUserProxy
from DIRAC.Core.Utilities.List                         import fromChar
from DIRAC.Core.Utilities.PrettyPrint                  import printTable
from DIRAC.Resources.Catalog.FileCatalog               import FileCatalog

__RCSID__ = "$Id$"

class VOMS2CSAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):
    """ Defines default parameters
    """
    super(VOMS2CSAgent, self).__init__( *args, **kwargs )

    self.__voDict = {}
    self.__adminMsgs = {}
    self.csapi = None
    self.voChanged = False
    self.dryRun = False

    self.autoAddUsers = False
    self.autoModifyUsers = False
    self.autoDeleteUsers = False
    self.detailedReport = True
    self.makeFCEntry = False

  def initialize( self ):
    """ Initialize the default parameters
    """

    voNames = self.am_getOption( 'VO', [] )
    if not voNames[0].lower() == "none":
      if voNames[0].lower() == "any":
        voNames = []
      result = getVOMSVOs( voNames )
      if not result['OK']:
        return result
      self.__voDict = result['Value']
      self.log.notice( "VOs: %s" % self.__voDict.keys() )

    self.csapi = CSAPI()

    self.dryRun = self.am_getOption( 'DryRun', self.dryRun )
    self.autoAddUsers = self.am_getOption( 'AutoAddUsers', self.autoAddUsers )
    self.autoModifyUsers = self.am_getOption( 'AutoModifyUsers', self.autoModifyUsers )
    self.autoDeleteUsers = self.am_getOption( 'AutoDeleteUsers', self.autoDeleteUsers )
    self.detailedReport = self.am_getOption( 'DetailedReport', self.detailedReport )
    self.makeFCEntry = self.am_getOption( 'MakeHomeDirectory', self.makeFCEntry )

    return S_OK()

  def execute( self ):

    self.__adminMsgs = {}
    self.csapi.downloadCSData()
    for vo in self.__voDict:
      self.voChanged = False
      voAdminUser = getVOOption( vo, "VOAdmin")
      voAdminMail = None
      if voAdminUser:
        voAdminMail = getUserOption( voAdminUser, "Email")
      voAdminGroup = getVOOption( vo, "VOAdminGroup", getVOOption( vo, "DefaultGroup" ) )

      self.log.info( 'Performing VOMS sync for VO %s with credentials %s@%s' % ( vo, voAdminUser, voAdminGroup ) )

      result = self.__syncCSWithVOMS( vo, proxyUserName = voAdminUser, proxyUserGroup = voAdminGroup ) #pylint: disable=unexpected-keyword-arg
      if not result['OK']:
        self.log.error( 'Failed to perform VOMS to CS synchronization:', 'VO %s: %s' % ( vo, result["Message"] ) )
        continue
      resultDict = result['Value']
      newUsers = resultDict.get( "NewUsers", [] )
      modUsers = resultDict.get( "ModifiedUsers", [] )
      delUsers = resultDict.get( "DeletedUsers", [] )
      self.log.info( "Run results: new users %d, modified users %d, deleted users %d" % \
                     ( len( newUsers ), len( modUsers ), len( delUsers ) ) )

      if self.csapi.csModified:
        # We have accumulated all the changes, commit them now
        self.log.info( "There are changes to the CS for vo %s ready to be committed" % vo )
        if self.dryRun:
          self.log.info( "Dry Run: CS won't be updated" )
          self.csapi.showDiff()
        else:
          result = self.csapi.commitChanges()
          if not result[ 'OK' ]:
            self.log.error( "Could not commit configuration changes", result[ 'Message' ] )
            return result
          self.log.notice( "Configuration committed for VO %s" % vo )
      else:
        self.log.info( "No changes to the CS for VO %s recorded at this cycle" % vo )

      # Add user home directory in the file catalog
      if self.makeFCEntry and newUsers:
        self.log.info( "Creating home directories for users %s" % str( newUsers ) )
        result = self.__addHomeDirectory( vo, newUsers, proxyUserName = voAdminUser, proxyUserGroup = voAdminGroup ) #pylint: disable=unexpected-keyword-arg
        if not result['OK']:
          self.log.error( 'Failed to create user home directories:', 'VO %s: %s' % ( vo, result["Message"] ) )
        else:
          for user in result['Value']['Failed']:
            self.log.error( "Failed to create home directory", "user: %s, operation: %s" % \
                            ( user, result['Value']['Failed'][user] ) )
            self.__adminMsgs[ 'Errors' ].append( "Failed to create home directory for user %s: operation %s" % \
                                                 ( user, result['Value']['Failed'][user] ) )
          for user in result['Value']['Successful']:
            self.__adminMsgs[ 'Info' ].append( "Created home directory for user %s" % user )

      if self.voChanged or self.detailedReport:
        mailMsg = ""
        if self.__adminMsgs[ 'Errors' ]:
          mailMsg += "\nErrors list:\n  %s" % "\n  ".join( self.__adminMsgs[ 'Errors' ] )
        if self.__adminMsgs[ 'Info' ]:
          mailMsg += "\nRun result:\n  %s" % "\n  ".join( self.__adminMsgs[ 'Info' ] )
        if self.detailedReport:
          result = self.getVOUserReport( vo )
          if result['OK']:
            mailMsg += '\n\n'
            mailMsg += result['Value']
          else:
            mailMsg += 'Failed to produce a detailed user report'
            mailMsg += result['Message']
        NotificationClient().sendMail( self.am_getOption( 'MailTo', voAdminMail ),
                                       "VOMS2CSAgent run log", mailMsg,
                                       self.am_getOption( 'MailFrom', self.am_getOption( 'mailFrom', "DIRAC system" ) ) )

    return S_OK()

  def getVOUserData( self, vo, refreshFlag = False ):
    """ Get a report for users of a given VO

    :param str vo: VO name
    :return: S_OK/S_ERROR, Value = user description dictionary
    """
    if refreshFlag:
      gConfig.forceRefresh()

    # Get DIRAC users
    diracUsers = getUsersInVO( vo )
    if not diracUsers:
      return S_ERROR( "No VO users found for %s" % vo )

    if refreshFlag:
      result = self.csapi.downloadCSData()
      if not result['OK']:
        return result
    result = self.csapi.describeUsers( diracUsers )
    if not result['OK']:
      self.log.error( 'Could not retrieve CS User description' )
    return result

  def getVOUserReport( self, vo ):
    """

    :param str vo: VO name
    :return: report string
    """

    result = self.getVOUserData( vo, refreshFlag = True )
    if not result['OK']:
      return result

    userDict = result['Value']

    # Get DIRAC group vs VOMS Role Mappings
    result = getVOMSRoleGroupMapping( vo )
    if not result['OK']:
      return result

    diracVOMSMapping = result['Value']['DIRACVOMS']

    records = []
    groupDict = defaultdict( int )
    multiDNUsers = {}
    suspendedUsers = []
    for user in userDict:
      for group in userDict[user]['Groups']:
        groupDict[group] += 1
      dnList = fromChar( userDict[user]['DN'] )
      if len( dnList ) > 1:
        multiDNUsers[user] = dnList
      if userDict[user].get( 'Status', 'Active' ) == 'Suspended':
        suspendedUsers.append( user )

    for group in diracVOMSMapping:
      records.append( ( group, str( groupDict[group] ), diracVOMSMapping.get( group, '' ) ) )

    fields = [ 'Group', 'Number of users', 'VOMS Role' ]
    output = printTable( fields, records, sortField = 'Group', printOut = False, numbering = False )

    if multiDNUsers:
      output += '\nUsers with multiple DNs:\n'
      for user in multiDNUsers:
        output += '  %s:\n' % user
        for dn in multiDNUsers[user]:
          output += '    %s\n' % dn

    if suspendedUsers:
      output += '\n%d suspended users:\n' % len( suspendedUsers )
      output += '  %s' % ','.join( suspendedUsers )

    return S_OK( output )

  @executeWithUserProxy
  def __syncCSWithVOMS( self, vo ):
    self.__adminMsgs = { 'Errors' : [], 'Info' : [] }
    resultDict = defaultdict( list )

    # Get DIRAC group vs VOMS Role Mappings
    result = getVOMSRoleGroupMapping( vo )
    if not result['OK']:
      return result

    vomsDIRACMapping = result['Value']['VOMSDIRAC']
    diracVOMSMapping = result['Value']['DIRACVOMS']
    noVOMSGroups = result['Value']['NoVOMS']
    noSyncVOMSGroups = result['Value']['NoSyncVOMS']

    vomsSrv = VOMSService( vo )

    # Get VOMS VO name
    result = vomsSrv.admGetVOName()
    if not result['OK']:
      self.log.error( 'Could not retrieve VOMS VO name', "for %s" % vo )
      return result
    vomsVOName = result[ 'Value' ].lstrip( '/' )
    self.log.verbose( "VOMS VO Name for %s is %s" % ( vo, vomsVOName ) )

    # Get VOMS users
    result = vomsSrv.getUsers()
    if not result['OK']:
      self.log.error( 'Could not retrieve user information from VOMS', result['Message'] )
      return result
    vomsUserDict = result[ 'Value' ]
    message = "There are %s user entries in VOMS for VO %s" % ( len( vomsUserDict ), vomsVOName )
    self.__adminMsgs[ 'Info' ].append( message )
    self.log.info( message )

    # Get DIRAC users
    result = self.getVOUserData( vo )
    if not result['OK']:
      return result
    diracUserDict = result['Value']
    self.__adminMsgs[ 'Info' ].append( "There are %s registered users in DIRAC for VO %s" % ( len( diracUserDict ), vo ) )
    self.log.info( "There are %s registered users in DIRAC VO %s" % ( len( diracUserDict ), vo ) )

    # Find new and obsoleted user DNs
    existingDNs = []
    obsoletedDNs = []
    newDNs = []
    for user in diracUserDict:
      dn = diracUserDict[user]['DN']
      # We can have users with more than one DN registered
      dnList = fromChar( dn )
      existingDNs.extend( dnList )
      for dn in dnList:
        if dn not in vomsUserDict:
          obsoletedDNs.append( dn )

    for dn in vomsUserDict:
      if dn not in existingDNs:
        newDNs.append( dn )

    allDiracUsers = getAllUsers()
    nonVOUserDict = {}
    nonVOUsers = list( set( allDiracUsers ) - set( diracUserDict.keys() ) )
    if nonVOUsers:
      result = self.csapi.describeUsers( nonVOUsers )
      if not result['OK']:
        self.log.error( 'Could not retrieve CS User description' )
        return result
      nonVOUserDict = result['Value']

    # Process users
    defaultVOGroup = getVOOption( vo, "DefaultGroup", "%s_user" % vo )
    newAddedUserDict = {}
    for dn in vomsUserDict:
      nickName = ''
      newDNForExistingUser = ''
      diracName = ''
      if dn in existingDNs:
        for user in diracUserDict:
          if dn == diracUserDict[user]['DN']:
            diracName = user

      if dn in newDNs:
        # Find if the DN is already registered in the DIRAC CS
        for user in nonVOUserDict:
          if dn == nonVOUserDict[user]['DN']:
            diracName = user

        # Check the nickName in the same VO to see if the user is already registered
        # with another DN
        result = vomsSrv.attGetUserNickname( dn, vomsUserDict[dn]['CA'] )
        if result['OK']:
          nickName = result['Value']
        if nickName in diracUserDict or nickName in newAddedUserDict:
          diracName = nickName
          # This is a flag for adding the new DN to an already existing user
          newDNForExistingUser = dn

        # We have a real new user
        if not diracName:
          if nickName:
            newDiracName = nickName
          else:
            newDiracName = getUserName( dn, vomsUserDict[dn]['mail'], vo )

          # If the chosen user name exists already, append a distinguishing suffix
          ind = 1
          trialName = newDiracName
          while newDiracName in allDiracUsers:
            # We have a user with the same name but with a different DN
            newDiracName = "%s_%d" % ( trialName, ind )
            ind += 1

          # We now have everything to add the new user
          userDict = { "DN": dn, "CA": vomsUserDict[dn]['CA'], "Email": vomsUserDict[dn]['mail'] }
          groupsWithRole = []
          for role in vomsUserDict[dn]['Roles']:
            fullRole = "/%s/%s" % ( vomsVOName, role )
            groupList = vomsDIRACMapping.get( fullRole, [] )
            for group in groupList:
              if group not in noSyncVOMSGroups:
                groupsWithRole.append( group )
          userDict['Groups'] = list( set( groupsWithRole + [defaultVOGroup] ) )
          message = "\n  Added new user %s:\n" % newDiracName
          for key in userDict:
            message += "    %s: %s\n" % ( key, str( userDict[key] ) )
          self.__adminMsgs[ 'Info' ].append( message )
          self.voChanged = True
          if self.autoAddUsers:
            self.log.info( "Adding new user %s: %s" % ( newDiracName, str( userDict ) ) )
            result = self.csapi.modifyUser( newDiracName, userDict, createIfNonExistant = True )
            if not result['OK']:
              self.log.warn( 'Failed adding new user %s' % newDiracName )
            resultDict['NewUsers'].append( newDiracName )
            newAddedUserDict[newDiracName] = userDict
          continue

      # We have an already existing user
      modified = False
      userDict = { "DN": dn, "CA": vomsUserDict[dn]['CA'], "Email": vomsUserDict[dn]['mail'] }
      if newDNForExistingUser:
        userDict['DN'] = ','.join( [ dn, diracUserDict[diracName]['DN'] ] )
        modified = True
      existingGroups = diracUserDict.get( diracName, {} ).get( 'Groups', [] )
      nonVOGroups = list( set( existingGroups ) - set( diracVOMSMapping.keys() ) )
      groupsWithRole = []
      for role in vomsUserDict[dn]['Roles']:
        fullRole = "/%s/%s" % ( vomsVOName, role )
        groupList = vomsDIRACMapping.get( fullRole, [] )
        for group in groupList:
          if group not in noSyncVOMSGroups:
            groupsWithRole.append( group )
      keepGroups = nonVOGroups + groupsWithRole + [defaultVOGroup]
      for group in existingGroups:
        if group in nonVOGroups:
          continue
        role = diracVOMSMapping.get( group, '' )
        # Among already existing groups for the user keep those without a special VOMS Role
        # because this membership is done by hand in the CS
        if not "Role" in role:
          keepGroups.append( group )
        # Keep existing groups with no VOMS attribute if any
        if group in noVOMSGroups:
          keepGroups.append( group )
        # Keep groups for which syncronization with VOMS is forbidden
        if group in noSyncVOMSGroups:
          keepGroups.append( group )
      userDict['Groups'] = list( set( keepGroups ) )
      # Merge together groups for the same user but different DNs
      if diracName in newAddedUserDict:
        otherGroups = newAddedUserDict[diracName].get( 'Groups', [] )
        userDict['Groups'] = list( set( keepGroups + otherGroups ) )
        modified = True

      # Check if something changed before asking CSAPI to modify
      if diracName in diracUserDict:
        message = "\n  Modified user %s:\n" % diracName
        modMsg = ''
        for key in userDict:
          if key == "Groups":
            addedGroups = set( userDict[key] ) - set( diracUserDict.get( diracName, {} ).get( key, [] ) )
            removedGroups = set( diracUserDict.get( diracName, {} ).get( key, [] ) ) - set( userDict[key] )
            if addedGroups:
              modMsg += "    Added to group(s) %s\n" % ','.join( addedGroups )
            if removedGroups:
              modMsg += "    Removed from group(s) %s\n" % ','.join( removedGroups )
          else:
            oldValue = str( diracUserDict.get( diracName, {} ).get( key, '' ) )
            if str( userDict[key] ) != oldValue:
              modMsg += "    %s: %s -> %s\n" % ( key, oldValue, str( userDict[key] ) )
        if modMsg:
          self.__adminMsgs[ 'Info' ].append( message + modMsg )
          modified = True

      if self.autoModifyUsers and modified:
        result = self.csapi.modifyUser( diracName, userDict )
        if result['OK'] and result['Value']:
          self.log.info( "Modified user %s: %s" % ( diracName, str( userDict ) ) )
          self.voChanged = True
          resultDict['ModifiedUsers'].append( diracName )

    # Check if there are potentially obsoleted users
    oldUsers = set()
    for user in diracUserDict:
      dnSet = set( fromChar( diracUserDict[user]['DN'] ) )
      if not dnSet.intersection( set( vomsUserDict.keys() ) ) and user not in nonVOUserDict:
        for group in diracUserDict[user]['Groups']:
          if group not in noVOMSGroups:
            oldUsers.add( user )

    # Check for obsoleted DNs
    for user in diracUserDict:
      dnSet = set( fromChar( diracUserDict[user]['DN'] ) )
      for dn in dnSet:
        if dn in obsoletedDNs and user not in oldUsers:
          self.log.verbose( "Modified user %s: dropped DN %s" % ( user, dn ) )
          if self.autoModifyUsers:
            userDict = diracUserDict[user]
            modDNSet = dnSet - set( [dn] )
            if modDNSet:
              userDict['DN'] = ','.join( modDNSet )
              result = self.csapi.modifyUser( user, userDict )
              if result['OK'] and result['Value']:
                self.log.info( "Modified user %s: dropped DN %s" % ( user, dn ) )
                self.__adminMsgs[ 'Info' ].append( "Modified user %s: dropped DN %s" % ( user, dn ) )
                self.voChanged = True
                resultDict['ModifiedUsers'].append( diracName )
            else:
              oldUsers.add( user )


    if oldUsers:
      self.voChanged = True
      if self.autoDeleteUsers:
        self.log.info( 'The following users will be deleted: %s' % str( oldUsers ) )
        result = self.csapi.deleteUsers( oldUsers )
        if result['OK']:
          self.__adminMsgs[ 'Info' ].append( 'The following users are deleted from CS:\n  %s\n' % str( oldUsers ) )
          resultDict['DeletedUsers'] = oldUsers
        else:
          self.__adminMsgs[ 'Errors' ].append( 'Error in deleting users from CS:\n  %s' % str( oldUsers ) )
          self.log.error( 'Error while user deletion from CS', result )
      else:
        self.__adminMsgs[ 'Info' ].append( 'The following users to be checked for deletion:\n  %s' % str( oldUsers ) )
        self.log.info( 'The following users to be checked for deletion: %s' % str( oldUsers ) )

    return S_OK( resultDict )

  @executeWithUserProxy
  def __addHomeDirectory( self, vo, newUsers ):

    fc = FileCatalog( vo = vo )
    defaultVOGroup = getVOOption( vo, "DefaultGroup", "%s_user" % vo )

    failed = {}
    successful = {}
    for user in newUsers:
      result = fc.addUser( user )
      if not result['OK']:
        failed[user] = "addUser"
        continue
      dirName = '/%s/user/%s/%s' % ( vo, user[0], user )
      result = fc.createDirectory( dirName )
      if not result['OK']:
        failed[user] = "createDirectory"
        continue
      result = fc.changePathOwner( { dirName: user }, recursive = False )
      if not result['OK']:
        failed[user] = "changePathOwner"
        continue
      result = fc.changePathGroup( { dirName: defaultVOGroup }, recursive = False )
      if not result['OK']:
        failed[user] = "changePathGroup"
        continue
      successful[user] = True

    return S_OK( { "Successful": successful, "Failed": failed } )

###############################################################
# Local utilities
###############################################################

def getVOMSVOs( voList = None ):
  """ Get all VOs that have VOMS correspondence

  :return: dictionary of the VO -> VOMSName correspondence
  """
  if voList is None:
    voList = []
  voDict = {}
  if not voList:
    result = gConfig.getSections( '/Registry/VO' )
    if not result['OK']:
      return result
    voList = result['Value']

  for vo in voList:
    vomsName = getVOOption( vo, 'VOMSName' )
    if vomsName:
      voDict[vo] = vomsName

  return S_OK( voDict )

def getUserName( dn, mail, vo ):
  """ Utility to construct user name
  :param str dn: user DN
  :param str mail: user e-mail address
  :return str: user name
  """
  dnName = getUserNameFromDN( dn, vo )
  mailName = getUserNameFromMail( mail )

  # If robot, take the dn based name
  if dnName.startswith( 'robot' ):
    return dnName

  # Is mailName reasonable ?
  if len( mailName ) > 5 and mailName.isalpha():
    return mailName

  # dnName too long
  if len( dnName ) >= 12:
    return dnName[:11]

  # May be the mail name is still more reasonable
  if len( dnName ) < len( mailName ) and mailName.isalpha():
    return mailName

  return dnName

def getUserNameFromMail( mail ):
  """ Utility to construct a reasonable user name from the user mail address

  :param str mail: e-mail address
  :return str: user name
  """

  mailName = mail.split( '@' )[0].lower()
  if '.' in mailName:
    # Most likely the mail contains the full user name
    names = mailName.split( '.' )
    name = names[0][0] + names[-1].lower()
    return name

  return mailName

def getUserNameFromDN( dn, vo ):
  """ Utility to construct a reasonable user name from the user DN
  :param str dn: user DN
  :return str: user name
  """

  shortVO = vo
  if '.' in vo:
    vos = vo.split( '.' )
    if vos[0] == 'vo':
      vos = vos[1:]
    if len( vos[-1] ) == 2 or vos[-1] == 'org':
      vos = vos[:1]
    shortVO = '.'.join( vos )

  # Weird case of just a name as DN !
  if '/' not in dn and 'CN=' not in dn:
    dn = 'CN=' + dn
  entries = dn.split( '/' )
  entries.reverse()
  for entry in entries:
    if entry:
      # Weird case of no field name !
      if '=' not in entry:
        key, value = "CN", entry
      else:
        key, value = entry.split( '=' )
      if key.upper() == 'CN':
        ind = value.find( "(" )
        # Strip of possible words in parenthesis in the name
        if ind != -1:
          value = value[:ind]
        names = value.split()
        if len( names ) == 1:
          nname = names[0].lower()
          if '.' in nname:
            names = nname.split( '.' )
            nname = ( names[0][0] + names[-1] ).lower()
          if '-' in nname:
            names = nname.split( '-' )
            nname = ( names[0][0] + names[-1] ).lower()
          return nname
        else:
          robot = False
          if names[0].lower().startswith( "robot" ):
            names.pop( 0 )
            robot = True
          for name in list( names ):
            if name[0].isdigit() or "@" in name:
              names.pop( names.index( name ) )
          if robot:
            nname = "robot-%s-%s" % ( names[-1].lower(), shortVO )
          else:
            nname = ( names[0][0] + names[-1] ).lower()
            if '.' in nname:
              names = nname.split( '.' )
              nname = ( names[0][0] + names[-1] ).lower()
          return nname
