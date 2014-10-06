# $HeadURL$
""" Queries BDII for unknown CE.
    Queries BDII for CE information and puts it to CS.
"""
__RCSID__ = "$Id$"

from DIRAC                                              import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                        import AgentModule
from DIRAC.Core.Utilities.Grid                          import getBdiiCEInfo
from DIRAC.FrameworkSystem.Client.NotificationClient    import NotificationClient
from DIRAC.ConfigurationSystem.Client.CSAPI             import CSAPI
from DIRAC.Core.Security.ProxyInfo                      import getProxyInfo, formatProxyInfoAsString
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Registry  import getVOs, getVOOption
from DIRAC.ConfigurationSystem.Client.Utilities         import getUnusedGridCEs, getSiteUpdates

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

    for vo in self.voName:
      result = self.__getBdiiCEInfo( vo )
      if not result['OK']:
        continue
      bdiiInfo = result['Value']
      result = getUnusedGridCEs( vo, bdiiInfo = bdiiInfo, ceBlackList = knownCEs )
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
          ceString = "CE: %s, GOCDB Site Name: %s" % ( ce, site )
          systemTuple = siteDict[site][ce]['System']
          osString = "%s_%s_%s" % ( systemTuple )
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

  def __updateCEs( self ):

    changeSet = set()

    for vo in self.voName:
      result = self.__getBdiiCEInfo( vo )
      if not result['OK']:
        continue
      ceBdiiDict = result['Value']
      result = getSiteUpdates( vo, bdiiInfo = ceBdiiDict, log = self.log )
      if not result['OK']:
        continue
      changeSet.union( result['Value'] )
      
    # We have collected all the changes, consolidate VO settings
    queueVODict = {}
    for entry in changeSet:
      section, option , _value, new_value = entry
      if option == "VO":
        changeSet.remove( entry )
        queueVODict.setdefault( section, set() )
        queueVODict[section].union( set( new_value.split( ',' ) ) )  
    for section, VOs in queueVODict.items():
      changeSet.add( ( section, 'VO', '', ','.join( VOs ) ) )    

    if changeSet:
      changeList = list( changeSet )
      changeList.sort()
      body = '\n'.join( [ "%s/%s %s -> %s" % entry for entry in changeList ] )
      if body and self.addressTo and self.addressFrom:
        notification = NotificationClient()
        result = notification.sendMail( self.addressTo, self.subject, body, self.addressFrom, localAttempt = False )
      self.log.info( body )

      for section, option, value, new_value in changeSet:
        if value == 'Unknown' or not value:
          self.csAPI.setOption( cfgPath( section, option ), new_value )
        else:
          self.csAPI.modifyValue( cfgPath( section, option ), new_value )

      result = self.csAPI.commit()
      if not result['OK']:
        self.log.error( "Error while commit to CS", result['Message'] )
      else:
        self.log.info( "Successfully committed %d changes to CS" % len( self.changeList ) )
      return result
    else:
      self.log.info( "No changes found" )
      return S_OK()
