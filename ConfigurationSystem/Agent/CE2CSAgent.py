# $HeadURL$
""" Queries BDII for unknown CE.
    Queries BDII for CE information and puts it to CS.
"""
__RCSID__ = "$Id$"

from DIRAC                                              import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule                        import AgentModule
from DIRAC.Core.Utilities                               import List
from DIRAC.Core.Utilities.Grid                          import ldapSite, ldapCluster, ldapCE, ldapCEState
from DIRAC.FrameworkSystem.Client.NotificationClient    import NotificationClient
from DIRAC.ConfigurationSystem.Client.CSAPI             import CSAPI
from DIRAC.Core.Security.ProxyInfo                      import getProxyInfo, formatProxyInfoAsString
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources

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
    if not self.voName:
      vo = getVO()
      if vo:
        self.voName = [ vo ] 
    
    if self.voName:
      self.log.info( "Agent will manage VO(s) %s" % self.voName )
    else:
      self.log.fatal( "VirtualOrganization option not defined for agent" )
      return S_ERROR()

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
    self.__infoFromCE()
    self.log.info( "End Execution" )
    return S_OK()

  def __checkAlternativeBDIISite( self, fun, *args ):
    if self.alternativeBDIIs:
      self.log.warn( "Trying to use alternative BDII sites" )
      for site in self.alternativeBDIIs :
        self.log.info( "Trying to contact alternative BDII", site )
        if len( args ) == 1 :
          result = fun( args[0], host = site )
        elif len( args ) == 2 :
          result = fun( args[0], vo = args[1], host = site )
        if not result['OK'] :
          self.log.error ( "Problem contacting alternative BDII", result['Message'] )
        elif result['OK'] :
          return result
      self.log.warn( "Also checking alternative BDII sites failed" )
      return result

  def __lookForCE( self ):

    knownCEs = self.am_getOption( 'BannedCEs', [] )

    resources = Resources( self.voName )
    result    = resources.getEligibleResources( 'Computing', {'CEType':['LCG','CREAM'] } ) 
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

    response = ''
    for vo in self.voName:
      self.log.info( "Check for available CEs for VO", vo )
      response = ldapCEState( '', vo )
      if not response['OK']:
        self.log.error( "Error during BDII request", response['Message'] )
        response = self.__checkAlternativeBDIISite( ldapCEState, '', vo )
        return response

      newCEs = {}
      for queue in response['Value']:
        try:
          queueName = queue['GlueCEUniqueID']
        except:
          continue

        ceName = queueName.split( ":" )[0]
        if not ceName in knownCEs:
          newCEs[ceName] = None
          self.log.debug( "New CE", ceName )

      body = ""
      possibleNewSites = []
      for ce in newCEs.iterkeys():
        response = ldapCluster( ce )
        if not response['OK']:
          self.log.warn( "Error during BDII request", response['Message'] )
          response = self.__checkAlternativeBDIISite( ldapCluster, ce )
          continue
        clusters = response['Value']
        if len( clusters ) != 1:
          self.log.warn( "Error in cluster length", " CE %s Length %d" % ( ce, len( clusters ) ) )
        if len( clusters ) == 0:
          continue
        cluster = clusters[0]
        fkey = cluster.get( 'GlueForeignKey', [] )
        if type( fkey ) == type( '' ):
          fkey = [fkey]
        nameBDII = None
        for entry in fkey:
          if entry.count( 'GlueSiteUniqueID' ):
            nameBDII = entry.split( '=' )[1]
            break
        if not nameBDII:
          continue

        ceString = "CE: %s, GOCDB Name: %s" % ( ce, nameBDII )
        self.log.info( ceString )

        response = ldapCE( ce )
        if not response['OK']:
          self.log.warn( "Error during BDII request", response['Message'] )
          response = self.__checkAlternativeBDIISite( ldapCE, ce )
          continue

        ceInfos = response['Value']
        if len( ceInfos ):
          ceInfo = ceInfos[0]
          systemName = ceInfo.get( 'GlueHostOperatingSystemName', 'Unknown' )
          systemVersion = ceInfo.get( 'GlueHostOperatingSystemVersion', 'Unknown' )
          systemRelease = ceInfo.get( 'GlueHostOperatingSystemRelease', 'Unknown' )
        else:
          systemName = "Unknown"
          systemVersion = "Unknown"
          systemRelease = "Unknown"

        osString = "SystemName: %s, SystemVersion: %s, SystemRelease: %s" % ( systemName, systemVersion, systemRelease )
        self.log.info( osString )

        response = ldapCEState( ce, vo )
        if not response['OK']:
          self.log.warn( "Error during BDII request", response['Message'] )
          response = self.__checkAlternativeBDIISite( ldapCEState, ce, vo )
          continue

        newCEString = "\n\n%s\n%s" % ( ceString, osString )
        usefull = False
        ceStates = response['Value']
        for ceState in ceStates:
          queueName = ceState.get( 'GlueCEUniqueID', 'UnknownName' )
          queueStatus = ceState.get( 'GlueCEStateStatus', 'UnknownStatus' )

          queueString = "%s %s" % ( queueName, queueStatus )
          self.log.info( queueString )
          newCEString += "\n%s" % queueString
          if queueStatus.count( 'Production' ):
            usefull = True
        if usefull:
          body += newCEString
          possibleNewSites.append( 'dirac-admin-add-site DIRACSiteName %s %s' % ( nameBDII, ce ) )
      if body:
        body = "We are glad to inform You about new CE(s) possibly suitable for %s:\n" % vo + body
        body += "\n\nTo suppress information about CE add its name to BannedCEs list."
        for  possibleNewSite in  possibleNewSites:
          body = "%s\n%s" % ( body, possibleNewSite )
        self.log.info( body )
        if self.addressTo and self.addressFrom:
          notification = NotificationClient()
          result = notification.sendMail( self.addressTo, self.subject, body, self.addressFrom, localAttempt = False )

    return S_OK()

  def __infoFromCE( self ):

    sitesSection = cfgPath( 'Resources', 'Sites' )
    result = gConfig.getSections( sitesSection )
    if not result['OK']:
      return
    grids = result['Value']

    changed = False
    body = ""

    for grid in grids:
      gridSection = cfgPath( sitesSection, grid )
      result = gConfig.getSections( gridSection )
      if not result['OK']:
        return
      sites = result['Value']

      for site in sites:
        siteSection = cfgPath( gridSection, site )
        opt = gConfig.getOptionsDict( siteSection )['Value']
        name = opt.get( 'Name', '' )
        if name:
          coor = opt.get( 'Coordinates', 'Unknown' )
          mail = opt.get( 'Mail', 'Unknown' )

          result = ldapSite( name )
          if not result['OK']:
            self.log.warn( "BDII site %s: %s" % ( name, result['Message'] ) )
            result = self.__checkAlternativeBDIISite( ldapSite, name )

          if result['OK']:
            bdiiSites = result['Value']
            if len( bdiiSites ) == 0:
              self.log.warn( name, "Error in BDII: leng = 0" )
            else:
              if not len( bdiiSites ) == 1:
                self.log.warn( name, "Warning in BDII: leng = %d" % len( bdiiSites ) )

              bdiiSite = bdiiSites[0]

              try:
                longitude = bdiiSite['GlueSiteLongitude']
                latitude = bdiiSite['GlueSiteLatitude']
                newcoor = "%s:%s" % ( longitude, latitude )
              except:
                self.log.warn( "Error in BDII coordinates" )
                newcoor = "Unknown"

              try:
                newmail = bdiiSite['GlueSiteSysAdminContact'].split( ":" )[-1].strip()
              except:
                self.log.warn( "Error in BDII mail" )
                newmail = "Unknown"

              self.log.debug( "%s %s %s" % ( name, newcoor, newmail ) )

              if newcoor != coor:
                self.log.info( "%s" % ( name ), "%s -> %s" % ( coor, newcoor ) )
                if coor == 'Unknown':
                  self.csAPI.setOption( cfgPath( siteSection, 'Coordinates' ), newcoor )
                else:
                  self.csAPI.modifyValue( cfgPath( siteSection, 'Coordinates' ), newcoor )
                changed = True

              if newmail != mail:
                self.log.info( "%s" % ( name ), "%s -> %s" % ( mail, newmail ) )
                if mail == 'Unknown':
                  self.csAPI.setOption( cfgPath( siteSection, 'Mail' ), newmail )
                else:
                  self.csAPI.modifyValue( cfgPath( siteSection, 'Mail' ), newmail )
                changed = True

        ceList = List.fromChar( opt.get( 'CE', '' ) )

        if not ceList:
          self.log.warn( site, 'Empty site list' )
          continue

  #      result = gConfig.getSections( cfgPath( siteSection,'CEs' )
  #      if not result['OK']:
  #        self.log.debug( "Section CEs:", result['Message'] )

        for ce in ceList:
          ceSection = cfgPath( siteSection, 'CEs', ce )
          result = gConfig.getOptionsDict( ceSection )
          if not result['OK']:
            self.log.debug( "Section CE", result['Message'] )
            wnTmpDir = 'Unknown'
            arch = 'Unknown'
            os = 'Unknown'
            si00 = 'Unknown'
            pilot = 'Unknown'
            ceType = 'Unknown'
          else:
            ceopt = result['Value']
            wnTmpDir = ceopt.get( 'wnTmpDir', 'Unknown' )
            arch = ceopt.get( 'architecture', 'Unknown' )
            os = ceopt.get( 'OS', 'Unknown' )
            si00 = ceopt.get( 'SI00', 'Unknown' )
            pilot = ceopt.get( 'Pilot', 'Unknown' )
            ceType = ceopt.get( 'CEType', 'Unknown' )

          result = ldapCE( ce )
          if not result['OK']:
            self.log.warn( 'Error in BDII for %s' % ce, result['Message'] )
            result = self.__checkAlternativeBDIISite( ldapCE, ce )
            continue
          try:
            bdiiCE = result['Value'][0]
          except:
            self.log.warn( 'Error in BDII for %s' % ce, result )
            bdiiCE = None
          if bdiiCE:
            try:
              newWNTmpDir = bdiiCE['GlueSubClusterWNTmpDir']
            except:
              newWNTmpDir = 'Unknown'
            if wnTmpDir != newWNTmpDir and newWNTmpDir != 'Unknown':
              section = cfgPath( ceSection, 'wnTmpDir' )
              self.log.info( section, " -> ".join( ( wnTmpDir, newWNTmpDir ) ) )
              if wnTmpDir == 'Unknown':
                self.csAPI.setOption( section, newWNTmpDir )
              else:
                self.csAPI.modifyValue( section, newWNTmpDir )
              changed = True

            try:
              newArch = bdiiCE['GlueHostArchitecturePlatformType']
            except:
              newArch = 'Unknown'
            if arch != newArch and newArch != 'Unknown':
              section = cfgPath( ceSection, 'architecture' )
              self.log.info( section, " -> ".join( ( arch, newArch ) ) )
              if arch == 'Unknown':
                self.csAPI.setOption( section, newArch )
              else:
                self.csAPI.modifyValue( section, newArch )
              changed = True

            try:
              newOS = '_'.join( ( bdiiCE['GlueHostOperatingSystemName'],
                                  bdiiCE['GlueHostOperatingSystemVersion'],
                                  bdiiCE['GlueHostOperatingSystemRelease'] ) )
            except:
              newOS = 'Unknown'
            if os != newOS and newOS != 'Unknown':
              section = cfgPath( ceSection, 'OS' )
              self.log.info( section, " -> ".join( ( os, newOS ) ) )
              if os == 'Unknown':
                self.csAPI.setOption( section, newOS )
              else:
                self.csAPI.modifyValue( section, newOS )
              changed = True
              body = body + "OS was changed %s -> %s for %s at %s\n" % ( os, newOS, ce, site )

            try:
              newSI00 = bdiiCE['GlueHostBenchmarkSI00']
            except:
              newSI00 = 'Unknown'
            if si00 != newSI00 and newSI00 != 'Unknown':
              section = cfgPath( ceSection, 'SI00' )
              self.log.info( section, " -> ".join( ( si00, newSI00 ) ) )
              if si00 == 'Unknown':
                self.csAPI.setOption( section, newSI00 )
              else:
                self.csAPI.modifyValue( section, newSI00 )
              changed = True

            try:
              rte = bdiiCE['GlueHostApplicationSoftwareRunTimeEnvironment']
              for vo in self.voName:
                if vo.lower() == 'lhcb':
                  if 'VO-lhcb-pilot' in rte:
                    newPilot = 'True'
                  else:
                    newPilot = 'False'
                else:
                  newPilot = 'Unknown'
            except:
              newPilot = 'Unknown'
            if pilot != newPilot and newPilot != 'Unknown':
              section = cfgPath( ceSection, 'Pilot' )
              self.log.info( section, " -> ".join( ( pilot, newPilot ) ) )
              if pilot == 'Unknown':
                self.csAPI.setOption( section, newPilot )
              else:
                self.csAPI.modifyValue( section, newPilot )
              changed = True

          newVO = ''
          for vo in self.voName:
            result = ldapCEState( ce, vo )        #getBDIICEVOView
            if not result['OK']:
              self.log.warn( 'Error in BDII for queue %s' % ce, result['Message'] )
              result = self.__checkAlternativeBDIISite( ldapCEState, ce, vo )
              continue
            try:
              queues = result['Value']
            except:
              self.log.warn( 'Error in BDII for queue %s' % ce, result['Massage'] )
              continue

            newCEType = 'Unknown'
            for queue in queues:
              try:
                queueType = queue['GlueCEImplementationName']
              except:
                queueType = 'Unknown'
              if newCEType == 'Unknown':
                newCEType = queueType
              else:
                if queueType != newCEType:
                  self.log.warn( 'Error in BDII for CE %s ' % ce, 'different CE types %s %s' % ( newCEType, queueType ) )

            if newCEType=='ARC-CE':
              newCEType = 'ARC'

            if ceType != newCEType and newCEType != 'Unknown':
              section = cfgPath( ceSection, 'CEType' )
              self.log.info( section, " -> ".join( ( ceType, newCEType ) ) )
              if ceType == 'Unknown':
                self.csAPI.setOption( section, newCEType )
              else:
                self.csAPI.modifyValue( section, newCEType )
              changed = True

            for queue in queues:
              try:
                queueName = queue['GlueCEUniqueID'].split( '/' )[-1]
              except:
                self.log.warn( 'Error in queueName ', queue )
                continue

              try:
                newMaxCPUTime = queue['GlueCEPolicyMaxCPUTime']
              except:
                newMaxCPUTime = None

              newSI00 = None
              try:
                caps = queue['GlueCECapability']
                if type( caps ) == type( '' ):
                  caps = [caps]
                for cap in caps:
                  if cap.count( 'CPUScalingReferenceSI00' ):
                    newSI00 = cap.split( '=' )[-1]
              except:
                newSI00 = None

              queueSection = cfgPath( ceSection, 'Queues', queueName )
              result = gConfig.getOptionsDict( queueSection )
              if not result['OK']:
                self.log.warn( "Section Queues", result['Message'] )
                maxCPUTime = 'Unknown'
                si00 = 'Unknown'
                allowedVOs = ['']
              else:
                queueOpt = result['Value']
                maxCPUTime = queueOpt.get( 'maxCPUTime', 'Unknown' )
                si00 = queueOpt.get( 'SI00', 'Unknown' )
                if newVO == '':     # Remember previous iteration, if none - read from conf
                  allowedVOs = queueOpt.get( 'VO', '' ).split( "," )
                else:               # Else use newVO, as it can contain changes, which aren't in conf yet
                  allowedVOs = newVO.split( "," )
              if newMaxCPUTime and ( maxCPUTime != newMaxCPUTime ):
                section = cfgPath( queueSection, 'maxCPUTime' )
                self.log.info( section, " -> ".join( ( maxCPUTime, newMaxCPUTime ) ) )
                if maxCPUTime == 'Unknown':
                  self.csAPI.setOption( section, newMaxCPUTime )
                else:
                  self.csAPI.modifyValue( section, newMaxCPUTime )
                changed = True

              if newSI00 and ( si00 != newSI00 ):
                section = cfgPath( queueSection, 'SI00' )
                self.log.info( section, " -> ".join( ( si00, newSI00 ) ) )
                if si00 == 'Unknown':
                  self.csAPI.setOption( section, newSI00 )
                else:
                  self.csAPI.modifyValue( section, newSI00 )
                changed = True
                
              modifyVO = True                       # Flag saying if we need VO option to change
              newVO = ''
              if allowedVOs != ['']:
                for allowedVO in allowedVOs:
                  allowedVO = allowedVO.strip()     # Get rid of spaces
                  newVO += allowedVO
                  if allowedVO == vo:               # Current VO has been already in list
                    newVO = ''
                    modifyVO = False                # Don't change anything
                    break                           # Skip next 'if', proceed to next VO
                  newVO += ', '
                    
              if modifyVO:
                section = cfgPath( queueSection, 'VO' )
                newVO += vo
                self.log.info( section, " -> ".join( ( '%s' % allowedVOs, newVO ) ) )
                if allowedVOs == ['']:
                  self.csAPI.setOption( section, newVO )
                else:
                  self.csAPI.modifyValue( section, newVO )
                changed = True
                
    if changed:
      self.log.info( body )
      if body and self.addressTo and self.addressFrom:
        notification = NotificationClient()
        result = notification.sendMail( self.addressTo, self.subject, body, self.addressFrom, localAttempt = False )

      return self.csAPI.commit()
    else:
      self.log.info( "No changes found" )
      return S_OK()
