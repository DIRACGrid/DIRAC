# $HeadURL$
""" Queries BDII for unknown CE.
    Queries BDII for CE information and puts it to CS.
"""
__RCSID__ = "$Id$"

from DIRAC                                              import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule                        import AgentModule
from DIRAC.Core.Utilities                               import List
from DIRAC.Core.Utilities.Grid                          import getBdiiCEInfo
from DIRAC.FrameworkSystem.Client.NotificationClient    import NotificationClient
from DIRAC.ConfigurationSystem.Client.CSAPI             import CSAPI
from DIRAC.Core.Security.ProxyInfo                      import getProxyInfo, formatProxyInfoAsString
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Registry  import getVOs, getVOOption
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping        import getDIRACSiteName

class CE2CSAgent( AgentModule ):

  addressTo = ''
  addressFrom = ''
  voName = ''
  subject = "CE2CSAgent"
  alternativeBDIIs = []

  def initialize( self ):

    # TODO: Have no default and if no mail is found then use the diracAdmin group
    # and resolve all associated mail addresses.
    self.addressTo = self.am_getOption( 'MailTo', self.addressTo )
    self.addressFrom = self.am_getOption( 'MailFrom', self.addressFrom )
    # Create a list of alternative bdii urls
    self.alternativeBDIIs = self.am_getOption( 'AlternativeBDIIs', [] )
    # Check if the bdii url is appended by a port number, if not append the default 2170
    for index, url in enumerate( self.alternativeBDIIs ):
      if not url.split( ':' )[-1].isdigit():
        self.alternativeBDIIs[index] += ':2170'
    if self.addressTo and self.addressFrom:
      self.log.info( "MailTo", self.addressTo )
      self.log.info( "MailFrom", self.addressFrom )
    if self.alternativeBDIIs :
      self.log.info( "AlternativeBDII URLs:", self.alternativeBDIIs )
    self.subject = "CE2CSAgent"

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/TestManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'TestManager' )

    self.voName = self.am_getOption( 'VirtualOrganization', [] )
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
    self.voBdiiDict = {}

    self.csAPI = CSAPI()
    return self.csAPI.initialize()

  def execute( self ):

    self.log.info( "Start Execution" )
    result = getProxyInfo()
    if not result['OK']:
      return result
    infoDict = result[ 'Value' ]
    self.log.info( formatProxyInfoAsString( infoDict ) )

    # Get a "fresh" copy of the CS data
    result = self.csAPI.downloadCSData()
    if not result['OK']:
      self.log.warn( "Could not download a fresh copy of the CS data", result[ 'Message' ] )

    self.__lookForCE()
    self.__updateCEs()
    self.log.info( "End Execution" )
    return S_OK()

  def __lookForCE( self ):

    knownCEs = self.am_getOption( 'BannedCEs', [] )

    result = gConfig.getSections( '/Resources/Sites' )
    if not result['OK']:
      return
    grids = result['Value']

    for grid in grids:
      result = gConfig.getSections( '/Resources/Sites/%s' % grid )
      if not result['OK']:
        return
      sites = result['Value']

      for site in sites:
        opt = gConfig.getOptionsDict( '/Resources/Sites/%s/%s' % ( grid, site ) )['Value']
        ces = List.fromChar( opt.get( 'CE', '' ) )
        knownCEs += ces
    knownCEs = set( knownCEs )

    for vo in self.voName:
      result = self.__getBdiiCEInfo( vo )
      if not result['OK']:
        continue

      ceDict = result['Value']
      body = ''
      possibleNewSites = []
      for site in ceDict:
        siteCEs = set( ceDict[site]['CEs'].keys() )
        newCEs = siteCEs - knownCEs
        if not newCEs:
          continue

        ceString = ''
        ceListString = ''
        for ce in newCEs:
          queueString = ''
          ceInfo = ceDict[site]['CEs'][ce]
          ceString = "CE: %s, GOCDB Site Name: %s" % ( ce, site )
          systemName = ceInfo.get( 'GlueHostOperatingSystemName', 'Unknown' )
          systemVersion = ceInfo.get( 'GlueHostOperatingSystemVersion', 'Unknown' )
          systemRelease = ceInfo.get( 'GlueHostOperatingSystemRelease', 'Unknown' )
          osString = "SystemName: %s, SystemVersion: %s, SystemRelease: %s" % ( systemName, systemVersion, systemRelease )
          newCEString = "\n%s\n%s\n" % ( ceString, osString )
          for queue in ceInfo['Queues']:
            queueStatus = ceInfo['Queues'][queue].get( 'GlueCEStateStatus', 'UnknownStatus' )
            if 'production' in queueStatus.lower():
              ceType = ceInfo['Queues'][queue].get( 'GlueCEImplementationName', '' )
              queueString += "   %s %s %s\n" % ( queue, queueStatus, ceType )
          if queueString:
            ceString = newCEString
            ceString += "Queues:\n"
            ceString += queueString
            ceListString += "%s " % ce

        if ceString:
          body += ceString
          possibleNewSites.append( 'dirac-admin-add-site DIRACSiteName %s %s' % ( site, ceListString ) )

      if body:
        body = "\nWe are glad to inform You about new CE(s) possibly suitable for %s:\n" % vo + body
        body += "\n\nTo suppress information about CE add its name to BannedCEs list.\n"
        for  possibleNewSite in  possibleNewSites:
          body = "%s\n%s" % ( body, possibleNewSite )
        self.log.info( body )
        if self.addressTo and self.addressFrom:
          notification = NotificationClient()
          result = notification.sendMail( self.addressTo, self.subject, body, self.addressFrom, localAttempt = False )
          if not result['OK']:
            self.log.error( 'Can not send new site notification mail', result['Message'] )

    return S_OK()

  def __getBdiiCEInfo( self, vo ):

    if vo in self.voBdiiDict:
      return S_OK( self.voBdiiDict[vo] )
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
      self.voBdiiDict[vo] = result['Value']
    return result

  def __updateCSOption( self, section, option, value, new_value ):

    logString = ''
    if new_value and new_value != value:
      logString = "%s/%s %s -> %s" % ( section, option, value, new_value )
      if logString in self.changeList:
        return logString
      self.changeList.append( logString )
      self.log.info( logString )
      if value == 'Unknown' or not value:
        self.csAPI.setOption( cfgPath( section, option ), new_value )
      else:
        self.csAPI.modifyValue( cfgPath( section, option ), new_value )
    return logString

  def __updateCEs( self ):

    self.changeList = []
    queueVODict = {}

    for vo in self.voName:
      result = self.__getBdiiCEInfo( vo )
      if not result['OK']:
        continue

      ceBdiiDict = result['Value']
      for site in ceBdiiDict:
        result = getDIRACSiteName( site )
        if not result['OK']:
          continue
        siteName = result['Value'][0]
        siteSection = cfgPath( '/Resources', 'Sites', siteName.split('.')[0], siteName )
        result = gConfig.getOptionsDict( siteSection )
        if not result['OK']:
          continue
        siteDict = result['Value']
        # Current CS values
        coor = siteDict.get( 'Coordinates', 'Unknown' )
        mail = siteDict.get( 'Mail', 'Unknown' ).replace( ' ','' )
        description = siteDict.get( 'Description', 'Unknown' )

        longitude = ceBdiiDict[site].get( 'GlueSiteLongitude', '' ).strip()
        latitude = ceBdiiDict[site].get( 'GlueSiteLatitude', '' ).strip()

        # Current BDII value
        newcoor = ''
        if longitude and latitude:
          newcoor = "%s:%s" % ( longitude, latitude )
        newmail = ceBdiiDict[site].get( 'GlueSiteSysAdminContact', '' ).replace( 'mailto:', '' ).strip()
        newdescription = ceBdiiDict[site].get( 'GlueSiteDescription', '' ).strip()

        self.log.debug( "%s %s %s %s" % ( site, newcoor, newmail, newdescription ) )

        # Adding site data to the CS
        self.__updateCSOption( siteSection, 'Coordinates', coor, newcoor )
        self.__updateCSOption( siteSection, 'Mail', mail, newmail )
        self.__updateCSOption( siteSection, 'Description', description, newdescription )

        ces = gConfig.getValue( cfgPath( siteSection, 'CE' ), [] )
        for ce in ces:
          ceSection = cfgPath( siteSection, 'CEs', ce )
          ceDict = {}
          result = gConfig.getOptionsDict( ceSection )
          if result['OK']:
            ceDict = result['Value']
          else:
            if ceBdiiDict[site]['CEs'].get( ce, None ):
              self.log.info( "Adding new CE %s to site %s/%s" % (ce, siteName, site) )
          ceInfo = ceBdiiDict[site]['CEs'].get( ce, None )
          if ceInfo is None:
            ceType = ceDict.get( 'CEType', '')
            continue

          # Current CS CE info
          arch = ceDict.get( 'architecture', 'Unknown' )
          OS = ceDict.get( 'OS', 'Unknown' )
          si00 = ceDict.get( 'SI00', 'Unknown' )
          ceType = ceDict.get( 'CEType', 'Unknown' )
          ram = ceDict.get( 'HostRAM', 'Unknown' )
          submissionMode = ceDict.get( 'SubmissionMode', 'Unknown' )

          # Current BDII CE info
          newarch = ceBdiiDict[site]['CEs'][ce].get( 'GlueHostArchitecturePlatformType', '' ).strip()
          systemName = ceInfo.get( 'GlueHostOperatingSystemName', '' ).strip()
          systemVersion = ceInfo.get( 'GlueHostOperatingSystemVersion', '' ).strip()
          systemRelease = ceInfo.get( 'GlueHostOperatingSystemRelease', '' ).strip()
          newOS = ''
          if systemName and systemVersion and systemRelease:
            newOS = '_'.join( ( systemName, systemVersion, systemRelease ) )
          newsi00 = ceInfo.get( 'GlueHostBenchmarkSI00', '' ).strip()
          newCEType = 'Unknown'
          for queue in ceInfo['Queues']:
            queueDict = ceInfo['Queues'][queue]
            newCEType = queueDict.get( 'GlueCEImplementationName', '' ).strip()
            if newCEType:
              break
          if newCEType=='ARC-CE':
            newCEType = 'ARC'
          if newCEType in ['ARC','CREAM']:
            newSubmissionMode = "Direct" 
          newRAM = ceInfo.get( 'GlueHostMainMemoryRAMSize', '' ).strip()

          # Adding CE data to the CS
          self.__updateCSOption( ceSection, 'architecture', arch, newarch )
          self.__updateCSOption( ceSection, 'OS', OS, newOS )
          self.__updateCSOption( ceSection, 'SI00', si00, newsi00 )
          self.__updateCSOption( ceSection, 'CEType', ceType, newCEType )
          self.__updateCSOption( ceSection, 'HostRAM', ram, newRAM )
          if submissionMode == "Unknown":
            self.__updateCSOption( ceSection, 'SubmissionMode', submissionMode, newSubmissionMode )

          queues = ceInfo['Queues'].keys()
          for queue in queues:
            queueSection = cfgPath( ceSection, 'Queues', queue )
            queueDict = {}
            result = gConfig.getOptionsDict( queueSection )
            if result['OK']:
              queueDict = result['Value']
            else:
              self.log.info( "Adding new queue %s to CE %s" % (queue, ce) )
            queueInfo = ceInfo['Queues'][queue]
            queueStatus = queueInfo['GlueCEStateStatus']
            if queueStatus.lower() != "production":
              continue

            # Current CS queue info
            maxCPUTime = queueDict.get( 'maxCPUTime', 'Unknown' )
            si00 = queueDict.get( 'SI00', 'Unknown' )
            maxTotalJobs = queueDict.get( 'MaxTotalJobs', 'Unknown' )
            maxWaitingJobs = queueDict.get( 'MaxWaitingJobs', 'Unknown' )

            # Current BDII queue info
            newMaxCPUTime = queueInfo.get( 'GlueCEPolicyMaxCPUTime', '' )
            newSI00 = ''
            caps = queueInfo['GlueCECapability']
            if type( caps ) == type( '' ):
              caps = [caps]
            for cap in caps:
              if 'CPUScalingReferenceSI00' in cap:
                newSI00 = cap.split( '=' )[-1]

            # Adding queue info to the CS
            self.__updateCSOption( queueSection, 'maxCPUTime', maxCPUTime, newMaxCPUTime )
            self.__updateCSOption( queueSection, 'SI00', si00, newSI00 )
            if maxTotalJobs == "Unknown":
              newTotalJobs =  min( 1000, int( int( queueInfo.get( 'GlueCEInfoTotalCPUs', 0 ) )/2 ) )
              newWaitingJobs =  max( 2, int( newTotalJobs * 0.1 ) )
              newTotalJobs = str( newTotalJobs )
              newWaitingJobs = str( newWaitingJobs )
              self.__updateCSOption( queueSection, 'MaxTotalJobs', '', newTotalJobs )
              self.__updateCSOption( queueSection, 'MaxWaitingJobs', '', newWaitingJobs )


            # Updating eligible VO list
            VOs = set()
            if queueDict.get( 'VO', '' ):
              VOs = set( [ q.strip() for q in queueDict.get( 'VO', '' ).split( ',' ) if q ] )
            if not queue in queueVODict:
              queueVODict[queue] = VOs
            queueVODict[queue].add( vo )
            if len( queueVODict[queue] - VOs ) > 0:
              newVOs = ','.join( queueVODict[queue] )
              self.__updateCSOption( queueSection, 'VO', '', newVOs )

    if self.changeList:
      body = '\n'.join( self.changeList )
      if body and self.addressTo and self.addressFrom:
        notification = NotificationClient()
        result = notification.sendMail( self.addressTo, self.subject, body, self.addressFrom, localAttempt = False )

      result = self.csAPI.commit()
      if not result['OK']:
        self.log.error( "Error while commit to CS", result['Message'] )
      else:
        self.log.info( "Successfully committed %d changes to CS" % len( self.changeList ) )
      return result
    else:
      self.log.info( "No changes found" )
      return S_OK()
