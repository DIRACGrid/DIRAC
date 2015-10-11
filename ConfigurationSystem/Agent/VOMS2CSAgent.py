"""
  VOMS2CSAgent performs the following operations:

    - Adds new users for the given VO taking into account the VO VOMS information
    - Updates the data in the CS for existing users including DIRAC group membership
    -
"""

__RCSID__ = "$Id$"

import os
from DIRAC.Core.Base.AgentModule                       import AgentModule
from DIRAC.ConfigurationSystem.Client.CSAPI            import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient   import NotificationClient
from DIRAC.Core.Security.VOMSService                   import VOMSService
from DIRAC.Core.Security                               import Locations, X509Chain
from DIRAC                                             import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption, getUserOption, \
                                                              getVOMSRoleGroupMapping, getUsersInVO, \
                                                              getAllUsers

class VOMS2CSAgent( AgentModule ):

  def initialize( self ):
    self.am_setOption( "PollingTime", 3600 * 6 ) # Every 6 hours
    self.__voDict = {}
    voNames = self.am_getOption( 'VO', [] )
    if voNames[0].lower() == "any":
      voNames = []
    result = self.__getVOMSVOs( voNames )
    if not result['OK']:
      return result
    self.__voDict = result['Value']

    self.proxyLocation = os.path.join( self.am_getWorkDirectory(), ".volatileId" )
    self.__adminMsgs = {}
    self.csapi = CSAPI()

    self.log.notice( "VOs: %s" % self.__voDict.keys() )

    return S_OK()

  def __getVOMSVOs( self ):
    """ Get all VOs that have VOMS correspondence

    :return: dictionary of the VO -> VOMSName correspondence
    """
    voDict = {}
    result = gConfig.getSections( '/Registry/VO' )
    if not result['OK']:
      return result
    voList = result['Value']
    for vo in voList:
      vomsName = getVOOption( vo, 'VOMSName' )
      if vomsName:
        voDict[vo] = vomsName

    return S_OK( voDict )

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

  def execute( self ):



    self.__adminMsgs = {}
    self.csapi.downloadCSData()
    for vo in self.__voDict:
      voAdminUser = getVOOption( vo, "VOAdmin")
      voAdminMail = None
      if voAdminUser:
        voAdminMail = getUserOption( voAdminUser, "Email")

      result = self.__syncCSWithVOMS( vo )
      if not result['OK']:
        self.log.error( 'Failed to perform VOMS to CS synchronization. ', "VO: %s" % vo )
        continue

      mailMsg = ""
      if self.__adminMsgs[ 'Errors' ]:
        mailMsg += "\nErrors list:\n  %s" % "\n  ".join( self.__adminMsgs[ 'Errors' ] )
      if self.__adminMsgs[ 'Info' ]:
        mailMsg += "\nRun result:\n  %s" % "\n  ".join( self.__adminMsgs[ 'Info' ] )
      NotificationClient().sendMail( self.am_getOption( 'MailTo', voAdminMail ),
                                     "VOMS2CSAgent run log", mailMsg,
                                     self.am_getOption( 'mailFrom', "DIRAC system" ) )

    # We have accumulated all the changes, commit them now
    result = self.csapi.commitChanges()
    if not result[ 'OK' ]:
      self.log.error( "Could not commit configuration changes", result[ 'Message' ] )
      return result
    self.log.info( "Configuration committed" )
    return S_OK()


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
    vomsVOName = result[ 'Value' ]
    self.log.info( "VOMS VO Name for %s is %s" % ( vo, vomsVOName ) )

    # Get VOMS user info
    result = vomsSrv.getUsers()
    if not result['OK']:
      self.log.error( 'Could not retrieve user information from VOMS', result['Message'] )
      return result
    vomsUserDict = result[ 'Value' ]
    self.__adminMsgs[ 'Info' ].append( "There are %s registered users in VOMS VO %s" % ( len( vomsUserDict ), vomsVOName ) )
    self.log.info( "There are %s registered users in VOMS VO %s" % ( len( vomsUserDict ), vomsVOName ) )

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
          result = vomsSrv.getUserNickname( dn, vomsUserDict[dn]['CA'], vomsUserDict[dn]['mail'] )
          if not result['OK']:
            self.log.error( 'Failed to evaluate nickname for DN', dn )
            continue
          newDiracName = result['Value']
          ind = 1
          trialName = newDiracName
          while newDiracName in allDiracUsers:
            # We have a user with the same name but with a different DN
            newDiracName = "%s_%d" % ( trialName, ind )

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
          self.log.info( "Adding new user %s: %s" % ( newDiracName, str( userDict ) ) )
          self.csapi.modifyUser( newDiracName, userDict, createIfNonExistant = True )
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
      self.csapi.modifyUser( diracName, userDict )

      # Check if there are potentially obsoleted users
      oldUsers = []
      for user in diracUserDict:
        dn = diracUserDict[user]['DN']
        if not dn in vomsUserDict and not user in nonVOUserDict:
          for group in diracUserDict[user]['Groups']:
            if not group in noVOMSGroups:
              oldUsers.append( user )
      if oldUsers:
        self.__adminMsgs[ 'Info' ].append( 'The following users to be checked for deletion: %s' % str( oldUsers ) )
        self.log.info( 'The following users to be checked for deletion: %s' % str( oldUsers ) )

    return S_OK()
