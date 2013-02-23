########################################################################
# $HeadURL$
########################################################################
""" Queries BDII for unknown CE.
    Queries BDII for CE information and puts it to CS.
"""
__RCSID__ = "$Id$"

from DIRAC                                              import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule                        import AgentModule
from DIRAC.Core.Utilities                               import List
from DIRAC.Core.Utilities.Grid                          import ldapSite, ldapCluster, ldapCE, ldapCEState, ldapService
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
    # create a list of alternative bdii urls
    self.alternativeBDIIs = self.am_getOption( 'AlternativeBDIIs', [] )
    # check if the bdii url is appended by a port number, if not append the default 2170
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
    # /Operations/Shifter/SAMManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'SAMManager' )

    self.voName = self.am_getOption( 'VirtualOrganization', self.voName )
    if not self.voName:
      self.voName = getVO()

    if not self.voName:
      self.log.fatal( "VO option not defined for agent" )
      return S_ERROR()

    self.csAPI = CSAPI()
    return self.csAPI.initialize()

  def execute( self ):

    self.log.info( "Start Execution" )
    result = getProxyInfo()
    if not result[ 'OK' ]:
      return result
    infoDict = result[ 'Value' ]
    self.log.info( formatProxyInfoAsString( infoDict ) )

    #Get a "fresh" copy of the CS data
    result = self.csAPI.downloadCSData()
    if not result[ 'OK' ]:
      self.log.warn( "Could not download a fresh copy of the CS data", result[ 'Message' ] )

    self.__lookForCE()
    self.__infoFromCE()
    self.log.info( "End Execution" )
    return S_OK()

  def __checkAlternativeBDIISite( self, fun, *args ):
    if self.alternativeBDIIs:
      self.log.warn( "Trying to use alternative bdii sites" )
      for site in self.alternativeBDIIs :
        self.log.info( "Trying to contact alternative bdii ", site )
        if len( args ) == 1 :
          result = fun( args[0], host = site )
        elif len( args ) == 2 :
          result = fun( args[0], vo = args[1], host = site )
        if not result['OK'] :
          self.log.error ( "Problem contacting alternative bddii", result['Message'] )
        elif result['OK'] :
          return result
      self.log.warn( "Also checking alternative BDII sites failed" )
      return result

  def __lookForCE( self ):

    knownces = self.am_getOption( 'BannedCEs', [] )

    resources = Resources( self.voName )
    result = resource.getEligibleResources( 'Computing', {'CEType':['LCG','CREAM'] } ) 
    if not result['OK']:
      return result
    
    siteDict = result['Value']
    for site in siteDict:
      knownces += siteDict[site]

#    result = gConfig.getSections( '/Resources/Sites' )
#    if not result['OK']:
#      return
#    grids = result['Value']
#
#    for grid in grids:
#
#      result = gConfig.getSections( '/Resources/Sites/%s' % grid )
#      if not result['OK']:
#        return
#      sites = result['Value']
#
#      for site in sites:
#        opt = gConfig.getOptionsDict( '/Resources/Sites/%s/%s' % ( grid, site ) )['Value']
#        ces = List.fromChar( opt.get( 'CE', '' ) )
#        knownces += ces

    response = ldapCEState( '', vo = self.voName )
    if not response['OK']:
      self.log.error( "Error during BDII request", response['Message'] )
      response = self.__checkAlternativeBDIISite( ldapCEState, '', self.voName )
      return response

    newces = {}
    for queue in response['Value']:
      try:
        queuename = queue['GlueCEUniqueID']
      except:
        continue

      cename = queuename.split( ":" )[0]
      if not cename in knownces:
        newces[cename] = None
        self.log.debug( "newce", cename )

    body = ""
    possibleNewSites = []
    for ce in newces.iterkeys():
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

      cestring = "CE: %s, GOCDB Name: %s" % ( ce, nameBDII )
      self.log.info( cestring )

      response = ldapCE( ce )
      if not response['OK']:
        self.log.warn( "Error during BDII request", response['Message'] )
        response = self.__checkAlternativeBDIISite( ldapCE, ce )
        continue

      ceinfos = response['Value']
      if len( ceinfos ):
        ceinfo = ceinfos[0]
        systemName = ceinfo.get( 'GlueHostOperatingSystemName', 'Unknown' )
        systemVersion = ceinfo.get( 'GlueHostOperatingSystemVersion', 'Unknown' )
        systemRelease = ceinfo.get( 'GlueHostOperatingSystemRelease', 'Unknown' )
      else:
        systemName = "Unknown"
        systemVersion = "Unknown"
        systemRelease = "Unknown"

      osstring = "SystemName: %s, SystemVersion: %s, SystemRelease: %s" % ( systemName, systemVersion, systemRelease )
      self.log.info( osstring )

      response = ldapCEState( ce, vo = self.voName )
      if not response['OK']:
        self.log.warn( "Error during BDII request", response['Message'] )
        response = self.__checkAlternativeBDIISite( ldapCEState, ce, self.voName )
        continue

      newcestring = "\n\n%s\n%s" % ( cestring, osstring )
      usefull = False
      cestates = response['Value']
      for cestate in cestates:
        queuename = cestate.get( 'GlueCEUniqueID', 'UnknownName' )
        queuestatus = cestate.get( 'GlueCEStateStatus', 'UnknownStatus' )

        queuestring = "%s %s" % ( queuename, queuestatus )
        self.log.info( queuestring )
        newcestring += "\n%s" % queuestring
        if queuestatus.count( 'Production' ):
          usefull = True
      if usefull:
        body += newcestring
        possibleNewSites.append( 'dirac-admin-add-site DIRACSiteName %s %s' % ( nameBDII, ce ) )
    if body:
      body = "We are glade to inform You about new CE(s) possibly suitable for %s:\n" % self.voName + body
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
  #      if site[-2:]!='ru':
  #        continue
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
            bdiisites = result['Value']
            if len( bdiisites ) == 0:
              self.log.warn( name, "Error in bdii: leng = 0" )
            else:
              if not len( bdiisites ) == 1:
                self.log.warn( name, "Warning in bdii: leng = %d" % len( bdiisites ) )

              bdiisite = bdiisites[0]

              try:
                longitude = bdiisite['GlueSiteLongitude']
                latitude = bdiisite['GlueSiteLatitude']
                newcoor = "%s:%s" % ( longitude, latitude )
              except:
                self.log.warn( "Error in bdii coor" )
                newcoor = "Unknown"

              try:
                newmail = bdiisite['GlueSiteSysAdminContact'].split( ":" )[-1].strip()
              except:
                self.log.warn( "Error in bdii mail" )
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

        celist = List.fromChar( opt.get( 'CE', '' ) )

        if not celist:
          self.log.warn( site, 'Empty site list' )
          continue

  #      result = gConfig.getSections( cfgPath( siteSection,'CEs' )
  #      if not result['OK']:
  #        self.log.debug( "Section CEs:", result['Message'] )

        for ce in celist:
          ceSection = cfgPath( siteSection, 'CEs', ce )
          result = gConfig.getOptionsDict( ceSection )
          if not result['OK']:
            self.log.debug( "Section CE", result['Message'] )
            wnTmpDir = 'Unknown'
            arch = 'Unknown'
            os = 'Unknown'
            si00 = 'Unknown'
            pilot = 'Unknown'
            cetype = 'Unknown'
          else:
            ceopt = result['Value']
            wnTmpDir = ceopt.get( 'wnTmpDir', 'Unknown' )
            arch = ceopt.get( 'architecture', 'Unknown' )
            os = ceopt.get( 'OS', 'Unknown' )
            si00 = ceopt.get( 'SI00', 'Unknown' )
            pilot = ceopt.get( 'Pilot', 'Unknown' )
            cetype = ceopt.get( 'CEType', 'Unknown' )

          result = ldapCE( ce )
          if not result['OK']:
            self.log.warn( 'Error in bdii for %s' % ce, result['Message'] )
            result = self.__checkAlternativeBDIISite( ldapCE, ce )
            continue
          try:
            bdiice = result['Value'][0]
          except:
            self.log.warn( 'Error in bdii for %s' % ce, result )
            bdiice = None
          if bdiice:
            try:
              newwnTmpDir = bdiice['GlueSubClusterWNTmpDir']
            except:
              newwnTmpDir = 'Unknown'
            if wnTmpDir != newwnTmpDir and newwnTmpDir != 'Unknown':
              section = cfgPath( ceSection, 'wnTmpDir' )
              self.log.info( section, " -> ".join( ( wnTmpDir, newwnTmpDir ) ) )
              if wnTmpDir == 'Unknown':
                self.csAPI.setOption( section, newwnTmpDir )
              else:
                self.csAPI.modifyValue( section, newwnTmpDir )
              changed = True

            try:
              newarch = bdiice['GlueHostArchitecturePlatformType']
            except:
              newarch = 'Unknown'
            if arch != newarch and newarch != 'Unknown':
              section = cfgPath( ceSection, 'architecture' )
              self.log.info( section, " -> ".join( ( arch, newarch ) ) )
              if arch == 'Unknown':
                self.csAPI.setOption( section, newarch )
              else:
                self.csAPI.modifyValue( section, newarch )
              changed = True

            try:
              newos = '_'.join( ( bdiice['GlueHostOperatingSystemName'],
                                  bdiice['GlueHostOperatingSystemVersion'],
                                  bdiice['GlueHostOperatingSystemRelease'] ) )
            except:
              newos = 'Unknown'
            if os != newos and newos != 'Unknown':
              section = cfgPath( ceSection, 'OS' )
              self.log.info( section, " -> ".join( ( os, newos ) ) )
              if os == 'Unknown':
                self.csAPI.setOption( section, newos )
              else:
                self.csAPI.modifyValue( section, newos )
              changed = True
              body = body + "OS was changed %s -> %s for %s at %s\n" % ( os, newos, ce, site )

            try:
              newsi00 = bdiice['GlueHostBenchmarkSI00']
            except:
              newsi00 = 'Unknown'
            if si00 != newsi00 and newsi00 != 'Unknown':
              section = cfgPath( ceSection, 'SI00' )
              self.log.info( section, " -> ".join( ( si00, newsi00 ) ) )
              if si00 == 'Unknown':
                self.csAPI.setOption( section, newsi00 )
              else:
                self.csAPI.modifyValue( section, newsi00 )
              changed = True

            try:
              rte = bdiice['GlueHostApplicationSoftwareRunTimeEnvironment']
              if self.voName.lower() == 'lhcb':
                if 'VO-lhcb-pilot' in rte:
                  newpilot = 'True'
                else:
                  newpilot = 'False'
              else:
                newpilot = 'Unknown'
            except:
              newpilot = 'Unknown'
            if pilot != newpilot and newpilot != 'Unknown':
              section = cfgPath( ceSection, 'Pilot' )
              self.log.info( section, " -> ".join( ( pilot, newpilot ) ) )
              if pilot == 'Unknown':
                self.csAPI.setOption( section, newpilot )
              else:
                self.csAPI.modifyValue( section, newpilot )
              changed = True

          result = ldapService( ce )
          if not result['OK'] :
            result = self.__checkAlternativeBDIISite( ldapService, ce )
          if result['OK'] and result['Value']:
            services = result['Value']
            newcetype = 'LCG'
            for service in services:
              if service['GlueServiceType'].count( 'CREAM' ):
                newcetype = "CREAM"
          else:
            newcetype = 'Unknown'

          if cetype != newcetype and newcetype != 'Unknown':
            section = cfgPath( ceSection, 'CEType' )
            self.log.info( section, " -> ".join( ( cetype, newcetype ) ) )
            if cetype == 'Unknown':
              self.csAPI.setOption( section, newcetype )
            else:
              self.csAPI.modifyValue( section, newcetype )
            changed = True

          result = ldapCEState( ce, vo = self.voName )        #getBDIICEVOView
          if not result['OK']:
            self.log.warn( 'Error in bdii for queue %s' % ce, result['Message'] )
            result = self.__checkAlternativeBDIISite( ldapCEState, ce, self.voName )
            continue
          try:
            queues = result['Value']
          except:
            self.log.warn( 'Error in bdii for queue %s' % ce, result['Massage'] )
            continue

          for queue in queues:
            try:
              queueName = queue['GlueCEUniqueID'].split( '/' )[-1]
            except:
              self.log.warn( 'error in queuename ', queue )
              continue

            try:
              newmaxCPUTime = queue['GlueCEPolicyMaxCPUTime']
            except:
              newmaxCPUTime = None

            newsi00 = None
            try:
              caps = queue['GlueCECapability']
              if type( caps ) == type( '' ):
                caps = [caps]
              for cap in caps:
                if cap.count( 'CPUScalingReferenceSI00' ):
                  newsi00 = cap.split( '=' )[-1]
            except:
              newsi00 = None

            queueSection = cfgPath( ceSection, 'Queues', queueName )
            result = gConfig.getOptionsDict( queueSection )
            if not result['OK']:
              self.log.warn( "Section Queues", result['Message'] )
              maxCPUTime = 'Unknown'
              si00 = 'Unknown'
            else:
              queueopt = result['Value']
              maxCPUTime = queueopt.get( 'maxCPUTime', 'Unknown' )
              si00 = queueopt.get( 'SI00', 'Unknown' )

            if newmaxCPUTime and ( maxCPUTime != newmaxCPUTime ):
              section = cfgPath( queueSection, 'maxCPUTime' )
              self.log.info( section, " -> ".join( ( maxCPUTime, newmaxCPUTime ) ) )
              if maxCPUTime == 'Unknown':
                self.csAPI.setOption( section, newmaxCPUTime )
              else:
                self.csAPI.modifyValue( section, newmaxCPUTime )
              changed = True

            if newsi00 and ( si00 != newsi00 ):
              section = cfgPath( queueSection, 'SI00' )
              self.log.info( section, " -> ".join( ( si00, newsi00 ) ) )
              if si00 == 'Unknown':
                self.csAPI.setOption( section, newsi00 )
              else:
                self.csAPI.modifyValue( section, newsi00 )
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
