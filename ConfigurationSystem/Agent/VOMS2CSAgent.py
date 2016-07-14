"""
  VOMS2CSAgent performs the following operations:

    - Adds new users for the given VO taking into account the VO VOMS information
    - Updates the data in the CS for existing users including DIRAC group membership
    -
"""

from DIRAC.Core.Base.AgentModule                       import AgentModule
from DIRAC.ConfigurationSystem.Client.CSAPI            import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient   import NotificationClient
from DIRAC.Core.Security.VOMSService                   import VOMSService
from DIRAC                                             import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption, getUserOption, \
                                                              getVOMSRoleGroupMapping, getUsersInVO, \
                                                              getAllUsers
from DIRAC.Core.Utilities.Proxy                        import executeWithUserProxy

__RCSID__ = "$Id$"

class VOMS2CSAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):
    """ Defines default parameters
    """
    super(VOMS2CSAgent, self).__init__( self, *args, **kwargs )

    self.__voDict = {}
    self.__adminMsgs = {}
    self.csapi = None
    self.voChanged = False
    self.dryRun = False

    self.autoAddUsers = False
    self.autoModifyUsers = False


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

      if self.voChanged:
        mailMsg = ""
        if self.__adminMsgs[ 'Errors' ]:
          mailMsg += "\nErrors list:\n  %s" % "\n  ".join( self.__adminMsgs[ 'Errors' ] )
        if self.__adminMsgs[ 'Info' ]:
          mailMsg += "\nRun result:\n  %s" % "\n  ".join( self.__adminMsgs[ 'Info' ] )
        NotificationClient().sendMail( self.am_getOption( 'MailTo', voAdminMail ),
                                       "VOMS2CSAgent run log", mailMsg,
                                       self.am_getOption( 'mailFrom', "DIRAC system" ) )

    # We have accumulated all the changes, commit them now
    if self.dryRun:
      self.log.info( "Dry Run: CS won't be updated" )
      self.csapi.showDiff()
    else:
      result = self.csapi.commitChanges()
      if not result[ 'OK' ]:
        self.log.error( "Could not commit configuration changes", result[ 'Message' ] )
        return result
      self.log.info( "Configuration committed" )
    return S_OK()

  @executeWithUserProxy
  def __syncCSWithVOMS( self, vo ):
    self.__adminMsgs = { 'Errors' : [], 'Info' : [] }

    # Get DIRAC group vs VOMS Role Mappings
    result = getVOMSRoleGroupMapping( vo )
    if not result['OK']:
      return result

    vomsDIRACMapping = result['Value']['VOMSDIRAC']
    diracVOMSMapping = result['Value']['DIRACVOMS']
    noVOMSGroups = result['Value']['NoVOMS']

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
    message = "There are %s registered users in VOMS VO %s" % ( len( vomsUserDict ), vomsVOName )
    self.__adminMsgs[ 'Info' ].append( message )
    self.log.info( message )

    # Get DIRAC users
    diracUsers = getUsersInVO( vo )
    if not diracUsers:
      return S_ERROR( "No VO users found for %s" % vo )

    result = self.csapi.describeUsers( diracUsers )
    if not result['OK']:
      self.log.error( 'Could not retrieve CS User description' )
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
      existingDNs.append( dn )
      if dn not in vomsUserDict:
        obsoletedDNs.append( dn )

    for dn in vomsUserDict:
      if dn not in existingDNs:
        newDNs.append( dn )

    allDiracUsers = getAllUsers()
    nonVOusers = list( set( allDiracUsers ) - set(diracUsers) )
    result = self.csapi.describeUsers( nonVOusers )
    if not result['OK']:
      self.log.error( 'Could not retrieve CS User description' )
      return result
    nonVOUserDict = result['Value']

    # Process users
    defaultVOGroup = getVOOption( vo, "DefaultGroup", "%s_user" % vo )
    for dn in vomsUserDict:
      if dn in newDNs:
        # Find if the DN is already registered in the DIRAC CS
        diracName = ''
        for user in nonVOUserDict:
          if dn == nonVOUserDict[user]['DN']:
            diracName = user
        # We have a real new user
        if not diracName:
          nickName = ''
          result = vomsSrv.attGetUserNickname( dn, vomsUserDict[dn]['CA'] )
          if result['OK']:
            nickName = result['Value']

          if nickName:
            newDiracName = nickName
          else:
            newDiracName = getUserName( dn, vomsUserDict[dn]['mail'] )

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
            group = vomsDIRACMapping.get( fullRole )
            if group:
              groupsWithRole.append( group )
          userDict['Groups'] = list( set( groupsWithRole + [defaultVOGroup] ) )
          self.__adminMsgs[ 'Info' ].append( "Adding new user %s: %s" % ( newDiracName, str( userDict ) ) )
          self.voChanged = True
          if self.autoAddUsers:
            self.log.info( "Adding new user %s: %s" % ( newDiracName, str( userDict ) ) )
            result = self.csapi.modifyUser( newDiracName, userDict, createIfNonExistant = True )
            if not result['OK']:
              self.log.warn( 'Failed adding new user %s' % newDiracName )
          continue

        # We have an already existing user
        userDict = { "DN": dn, "CA": vomsUserDict[dn]['CA'], "Email": vomsUserDict[dn]['mail'] }
        nonVOGroups = nonVOUserDict.get( diracName, {} ).get( 'Groups', [] )
        existingGroups = diracUserDict.get( diracName, {} ).get( 'Groups', [] )
        groupsWithRole = []
        for role in vomsUserDict[dn]['Roles']:
          fullRole = "/%s/%s" % ( vomsVOName, role )
          group = vomsDIRACMapping.get( fullRole )
          if group:
            groupsWithRole.append( group )
        keepGroups = nonVOGroups + groupsWithRole + [defaultVOGroup]
        for group in existingGroups:
          role = diracVOMSMapping[group]
          # Among already existing groups for the user keep those without a special VOMS Role
          # because this membership is done by hand in the CS
          if not "Role" in role:
            keepGroups.append( group )
          # Keep existing groups with no VOMS attribute if any
          if group in noVOMSGroups:
            keepGroups.append( group )
        userDict['Groups'] = keepGroups
        if self.autoModifyUsers:
          result = self.csapi.modifyUser( diracName, userDict )
          if result['OK'] and result['Value']:
            self.voChanged = True

    # Check if there are potentially obsoleted users
    oldUsers = set()
    for user in diracUserDict:
      dn = diracUserDict[user]['DN']
      if not dn in vomsUserDict and not user in nonVOUserDict:
        for group in diracUserDict[user]['Groups']:
          if not group in noVOMSGroups:
            oldUsers.add( user )
    if oldUsers:
      self.voChanged = True
      self.__adminMsgs[ 'Info' ].append( 'The following users to be checked for deletion: %s' % str( oldUsers ) )
      self.log.info( 'The following users to be checked for deletion: %s' % str( oldUsers ) )

    return S_OK()

###############################################################
# Local utilities
###############################################################

def getVOMSVOs( voList = [] ):
  """ Get all VOs that have VOMS correspondence

  :return: dictonary of the VO -> VOMSName correspondence
  """
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

def getUserName( dn, mail ):
  """ Utility to construct user name
  :param str dn: user DN
  :param str mail: user e-mail address
  :return str: user name
  """
  dnName = getUserNameFromDN( dn )
  mailName = getUserNameFromMail( mail )

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

def getUserNameFromDN( dn ):
  """ Utility to construct a reasonable user name from the user DN
  :param str dn: user DN
  :return str: user name
  """

  for entry in dn.split( '/' ):
    if entry:
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
            nname = "robot-%s" % names[-1].lower()
          else:
            nname = ( names[0][0] + names[-1] ).lower()
            if '.' in nname:
              names = nname.aplit( '.' )
              nname = ( names[0][0] + names[-1] ).lower()
          return nname
