""" The Bdii2CSAgent performs checking BDII for availability of CE and SE
    resources for a given or any configured VO. It detects resources not yet
    present in the CS and notifies the administrators.
    For the CEs and SEs already present in the CS, the agent is updating
    if necessary settings which were changed in the BDII recently
"""

from DIRAC                                              import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule                        import AgentModule
from DIRAC.Core.Utilities.Grid                          import getBdiiCEInfo, getBdiiSEInfo
from DIRAC.FrameworkSystem.Client.NotificationClient    import NotificationClient
from DIRAC.ConfigurationSystem.Client.CSAPI             import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Registry  import getVOs, getVOOption
from DIRAC.ConfigurationSystem.Client.Utilities         import getGridCEs, getSiteUpdates, getSRMUpdates, \
                                                               getCEsFromCS, getSEsFromCS, getGridSRMs

__RCSID__ = "$Id$"

class Bdii2CSAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):
    """ Defines default parameters
    """

    super(Bdii2CSAgent, self).__init__( self, *args, **kwargs )

    self.addressTo = ''
    self.addressFrom = ''
    self.voName = []
    self.subject = "Bdii2CSAgent"
    self.alternativeBDIIs = []
    self.voBdiiCEDict = {}
    self.voBdiiSEDict = {}

    self.csAPI = None

    # What to get
    self.processCEs = True
    self.processSEs = False
    # Update the CS or not?
    self.dryRun = False

  def initialize( self ):
    """ Gets run paramaters from the configuration
    """

    self.addressTo = self.am_getOption( 'MailTo', self.addressTo )
    self.addressFrom = self.am_getOption( 'MailFrom', self.addressFrom )
    # Create a list of alternative bdii urls
    self.alternativeBDIIs = self.am_getOption( 'AlternativeBDIIs', self.alternativeBDIIs )
    # Check if the bdii url is appended by a port number, if not append the default 2170
    for index, url in enumerate( self.alternativeBDIIs ):
      if not url.split( ':' )[-1].isdigit():
        self.alternativeBDIIs[index] += ':2170'
    if self.addressTo and self.addressFrom:
      self.log.info( "MailTo", self.addressTo )
      self.log.info( "MailFrom", self.addressFrom )
    if self.alternativeBDIIs :
      self.log.info( "AlternativeBDII URLs:", self.alternativeBDIIs )

    self.processCEs = self.am_getOption( 'ProcessCEs', self.processCEs )
    self.processSEs = self.am_getOption( 'ProcessSEs', self.processSEs )
    self.dryRun = self.am_getOption( 'DryRun', self.dryRun )

    self.voName = self.am_getOption( 'VirtualOrganization', self.voName )
    if not self.voName:
      self.voName = self.am_getOption( 'VO', [] )
    if not self.voName or ( len( self.voName ) == 1 and self.voName[0].lower() == 'all' ):
      # Get all VOs defined in the configuration
      self.voName = []
      result = getVOs()
      if result['OK']:
        vos = result['Value']
        for vo in vos:
          vomsVO = getVOOption( vo, "VOMSName" )
          if vomsVO:
            self.voName.append( vomsVO )

    if self.voName:
      self.log.info( "Agent will manage VO(s) %s" % self.voName )
    else:
      self.log.fatal( "VirtualOrganization option not defined for agent" )
      return S_ERROR()

    self.csAPI = CSAPI()
    return self.csAPI.initialize()

  def execute( self ):
    """ General agent execution method
    """
    self.voBdiiCEDict = {}
    self.voBdiiSEDict = {}

    # Get a "fresh" copy of the CS data
    result = self.csAPI.downloadCSData()
    if not result['OK']:
      self.log.warn( "Could not download a fresh copy of the CS data", result[ 'Message' ] )

    # Refresh the configuration from the master server
    gConfig.forceRefresh( fromMaster = True )

    if self.processCEs:
      self.__lookForNewCEs()
      self.__updateCEs()
    if self.processSEs:
      self.__lookForNewSEs()
      self.__updateSEs()
    return S_OK()

  def __lookForNewCEs( self ):
    """ Look up BDII for CEs not yet present in the DIRAC CS
    """

    bannedCEs = self.am_getOption( 'BannedCEs', [] )
    result = getCEsFromCS()
    if not result['OK']:
      return result
    knownCEs = set( result['Value'] )
    knownCEs = knownCEs.union( set( bannedCEs ) )

    for vo in self.voName:
      result = self.__getBdiiCEInfo( vo )
      if not result['OK']:
        continue
      bdiiInfo = result['Value']
      result = getGridCEs( vo, bdiiInfo = bdiiInfo, ceBlackList = knownCEs )
      if not result['OK']:
        self.log.error( 'Failed to get unused CEs', result['Message'] )
      siteDict = result['Value']
      body = ''
      for site in siteDict:
        newCEs = set( siteDict[site].keys() )
        if not newCEs:
          continue

        ceString = ''
        for ce in newCEs:
          queueString = ''
          ceInfo = bdiiInfo[site]['CEs'][ce]
          newCEString = "CE: %s, GOCDB Site Name: %s" % ( ce, site )
          systemTuple = siteDict[site][ce]['System']
          osString = "%s_%s_%s" % ( systemTuple )
          newCEString = "\n%s\n%s\n" % ( newCEString, osString )
          for queue in ceInfo['Queues']:
            queueStatus = ceInfo['Queues'][queue].get( 'GlueCEStateStatus', 'UnknownStatus' )
            if 'production' in queueStatus.lower():
              ceType = ceInfo['Queues'][queue].get( 'GlueCEImplementationName', '' )
              queueString += "   %s %s %s\n" % ( queue, queueStatus, ceType )
          if queueString:
            ceString += newCEString
            ceString += "Queues:\n"
            ceString += queueString

        if ceString:
          body += ceString

      if body:
        body = "\nWe are glad to inform You about new CE(s) possibly suitable for %s:\n" % vo + body
        body += "\n\nTo suppress information about CE add its name to BannedCEs list.\n"
        body += "Add new Sites/CEs for vo %s with the command:\n" % vo
        body += "dirac-admin-add-resources --vo %s --ce\n" % vo
        self.log.info( body )
        if self.addressTo and self.addressFrom:
          notification = NotificationClient()
          result = notification.sendMail( self.addressTo, self.subject, body, self.addressFrom, localAttempt = False )
          if not result['OK']:
            self.log.error( 'Can not send new site notification mail', result['Message'] )

    return S_OK()

  def __getBdiiCEInfo( self, vo ):

    if vo in self.voBdiiCEDict:
      return S_OK( self.voBdiiCEDict[vo] )
    self.log.info( "Check for available CEs for VO", vo )
    result = getBdiiCEInfo( vo )
    message = ''
    if not result['OK']:
      message = result['Message']
      for bdii in self.alternativeBDIIs :
        result = getBdiiCEInfo( vo, host = bdii )
        if result['OK']:
          break
    if not result['OK']:
      if message:
        self.log.error( "Error during BDII request", message )
      else:
        self.log.error( "Error during BDII request", result['Message'] )
    else:
      self.voBdiiCEDict[vo] = result['Value']
    return result

  def __getBdiiSEInfo( self, vo ):

    if vo in self.voBdiiSEDict:
      return S_OK( self.voBdiiSEDict[vo] )
    self.log.info( "Check for available SEs for VO", vo )
    result = getBdiiSEInfo( vo )
    message = ''
    if not result['OK']:
      message = result['Message']
      for bdii in self.alternativeBDIIs :
        result = getBdiiSEInfo( vo, host = bdii )
        if result['OK']:
          break
    if not result['OK']:
      if message:
        self.log.error( "Error during BDII request", message )
      else:
        self.log.error( "Error during BDII request", result['Message'] )
    else:
      self.voBdiiSEDict[vo] = result['Value']
    return result

  def __updateCEs( self ):
    """ Update the Site/CE/queue settings in the CS if they were changed in the BDII
    """

    bdiiChangeSet = set()

    for vo in self.voName:
      result = self.__getBdiiCEInfo( vo )
      if not result['OK']:
        continue
      ceBdiiDict = result['Value']
      result = getSiteUpdates( vo, bdiiInfo = ceBdiiDict, log = self.log )
      if not result['OK']:
        continue
      bdiiChangeSet = bdiiChangeSet.union( result['Value'] )

    # We have collected all the changes, consolidate VO settings
    result = self.__updateCS( bdiiChangeSet )
    return result

  def __updateCS( self, bdiiChangeSet ):

    queueVODict = {}
    changeSet = set()
    for entry in bdiiChangeSet:
      section, option , _value, new_value = entry
      if option == "VO":
        queueVODict.setdefault( section, set() )
        queueVODict[section] = queueVODict[section].union( set( new_value.split( ',' ) ) )
      else:
        changeSet.add( entry )
    for section, VOs in queueVODict.items():
      changeSet.add( ( section, 'VO', '', ','.join( VOs ) ) )

    if changeSet:
      changeList = list( changeSet )
      changeList.sort()
      body = '\n'.join( [ "%s/%s %s -> %s" % entry for entry in changeList ] )
      if body and self.addressTo and self.addressFrom:
        notification = NotificationClient()
        result = notification.sendMail( self.addressTo, self.subject, body, self.addressFrom, localAttempt = False )

      if body:
        self.log.info( 'The following configuration changes were detected:' )
        self.log.info( body )

      for section, option, value, new_value in changeSet:
        if value == 'Unknown' or not value:
          self.csAPI.setOption( cfgPath( section, option ), new_value )
        else:
          self.csAPI.modifyValue( cfgPath( section, option ), new_value )

      if self.dryRun:
        self.log.info( "Dry Run: CS won't be updated" )
        self.csAPI.showDiff()
      else:
        result = self.csAPI.commit()
        if not result['OK']:
          self.log.error( "Error while committing to CS", result['Message'] )
        else:
          self.log.info( "Successfully committed %d changes to CS" % len( changeList ) )
        return result
    else:
      self.log.info( "No changes found" )
      return S_OK()

  def __lookForNewSEs( self ):
    """ Look up BDII for SEs not yet present in the DIRAC CS
    """

    bannedSEs = self.am_getOption( 'BannedSEs', [] )
    result = getSEsFromCS()
    if not result['OK']:
      return result
    knownSEs = set( result['Value'] )
    knownSEs = knownSEs.union( set( bannedSEs ) )

    for vo in self.voName:
      result = self.__getBdiiSEInfo( vo )
      if not result['OK']:
        continue
      bdiiInfo = result['Value']
      result = getGridSRMs( vo, bdiiInfo = bdiiInfo, srmBlackList = knownSEs )
      if not result['OK']:
        continue
      siteDict = result['Value']
      body = ''
      for site in siteDict:
        newSEs = set( siteDict[site].keys() )
        if not newSEs:
          continue
        for se in newSEs:
          body += '\n New SE %s available at site %s:\n' % ( se, site )
          backend = siteDict[site][se]['SE'].get( 'GlueSEImplementationName', 'Unknown' )
          size = siteDict[site][se]['SE'].get( 'GlueSESizeTotal', 'Unknown' )
          body += '  Backend %s, Size %s' % ( backend, size )

      if body:
        body = "\nWe are glad to inform You about new SE(s) possibly suitable for %s:\n" % vo + body
        body += "\n\nTo suppress information about an SE add its name to BannedSEs list.\n"
        body += "Add new SEs for vo %s with the command:\n" % vo
        body += "dirac-admin-add-resources --vo %s --se\n" % vo
        self.log.info( body )
        if self.addressTo and self.addressFrom:
          notification = NotificationClient()
          result = notification.sendMail( self.addressTo, self.subject, body, self.addressFrom, localAttempt = False )
          if not result['OK']:
            self.log.error( 'Can not send new site notification mail', result['Message'] )

    return S_OK()

  def __updateSEs( self ):
    """ Update the Storage Element settings in the CS if they were changed in the BDII
    """

    bdiiChangeSet = set()

    for vo in self.voName:
      result = self.__getBdiiSEInfo( vo )
      if not result['OK']:
        continue
      seBdiiDict = result['Value']
      result = getSRMUpdates( vo, bdiiInfo = seBdiiDict )
      if not result['OK']:
        continue
      bdiiChangeSet = bdiiChangeSet.union( result['Value'] )

    # We have collected all the changes, consolidate VO settings
    result = self.__updateCS( bdiiChangeSet )
    return result
