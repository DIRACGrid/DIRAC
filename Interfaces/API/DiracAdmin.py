"""DIRAC Administrator API Class

All administrative functionality is exposed through the DIRAC Admin API.  Examples include
site banning and unbanning, WMS proxy uploading etc.

"""

import DIRAC
from DIRAC.Core.Utilities.PromptUser                          import promptUser
from DIRAC.Core.Base.API                                      import API
from DIRAC.ConfigurationSystem.Client.CSAPI                   import CSAPI
from DIRAC.Core.Security.ProxyInfo                            import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry        import getVOForGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Resources       import getSites, getSiteFullNames
from DIRAC.Core.DISET.RPCClient                               import RPCClient
from DIRAC.FrameworkSystem.Client.ProxyManagerClient          import gProxyManager
from DIRAC.FrameworkSystem.Client.NotificationClient          import NotificationClient
from DIRAC                                                    import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Grid                                import ldapSite, ldapCluster, ldapCE, ldapService
from DIRAC.Core.Utilities.Grid                                import ldapCEState, ldapCEVOView, ldapSA
from DIRAC.ResourceStatusSystem.Client.SiteStatus             import SiteStatus
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient   import ResourceStatusClient

import os, types
from datetime import datetime, timedelta

voName = ''
ret = getProxyInfo( disableVOMS = True )
if ret['OK'] and 'group' in ret['Value']:
  voName = getVOForGroup( ret['Value']['group'] )

COMPONENT_NAME = '/Interfaces/API/DiracAdmin'

class DiracAdmin( API ):
  """ Administrative functionalities
  """

  #############################################################################
  def __init__( self ):
    """Internal initialization of the DIRAC Admin API.
    """
    super( DiracAdmin, self ).__init__()

    self.csAPI = CSAPI()

    self.dbg = False
    if gConfig.getValue( self.section + '/LogLevel', 'DEBUG' ) == 'DEBUG':
      self.dbg = True

    self.scratchDir = gConfig.getValue( self.section + '/ScratchDir', '/tmp' )
    self.currentDir = os.getcwd()

  #############################################################################
  def uploadProxy( self, group ):
    """Upload a proxy to the DIRAC WMS.  This method

       Example usage:

       >>> print diracAdmin.uploadProxy('lhcb_pilot')
       {'OK': True, 'Value': 0L}

       :param group: DIRAC Group
       :type job: string
       :returns: S_OK,S_ERROR

       :param permanent: Indefinitely update proxy
       :type permanent: boolean

    """
    return gProxyManager.uploadProxy( diracGroup = group )

  #############################################################################
  def setProxyPersistency( self, userDN, userGroup, persistent = True ):
    """Set the persistence of a proxy in the Proxy Manager

       Example usage:

       >>> print diracAdmin.setProxyPersistency( 'some DN', 'dirac group', True )
       {'OK': True }

       :param userDN: User DN
       :type userDN: string
       :param userGroup: DIRAC Group
       :type userGroup: string
       :param persistent: Persistent flag
       :type persistent: boolean
       :returns: S_OK,S_ERROR
    """
    return gProxyManager.setPersistency( userDN, userGroup, persistent )

  #############################################################################
  def checkProxyUploaded( self, userDN, userGroup, requiredTime ):
    """Set the persistence of a proxy in the Proxy Manager

       Example usage:

       >>> print diracAdmin.setProxyPersistency( 'some DN', 'dirac group', True )
       {'OK': True, 'Value' : True/False }

       :param userDN: User DN
       :type userDN: string
       :param userGroup: DIRAC Group
       :type userGroup: string
       :param requiredTime: Required life time of the uploaded proxy
       :type requiredTime: boolean
       :returns: S_OK,S_ERROR
    """
    return gProxyManager.userHasProxy( userDN, userGroup, requiredTime )

  #############################################################################
  def getSiteMask( self, printOutput = False ):
    """Retrieve current site mask from WMS Administrator service.

       Example usage:

       >>> print diracAdmin.getSiteMask()
       {'OK': True, 'Value': 0L}

       :returns: S_OK,S_ERROR

    """
    
    siteStatus = SiteStatus()
    result = siteStatus.getUsableSites( 'ComputingAccess' )
    if result['OK']:
      sites = result['Value']
      if printOutput:
        sites.sort()
        for site in sites:
          print site

    return result

  #############################################################################
  def getBannedSites( self, printOutput = False ):
    """Retrieve current list of banned sites.

       Example usage:

       >>> print diracAdmin.getBannedSites()
       {'OK': True, 'Value': []}

       :returns: S_OK,S_ERROR

    """
    siteStatus = SiteStatus()

    result = siteStatus.getUnusableSites( 'ComputingAccess' )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    bannedSites = result['Value']

    bannedSites.sort()
    if printOutput:
      print '\n'.join( bannedSites )
    return S_OK( bannedSites )

  #############################################################################
  def getSiteSection( self, site, printOutput = False ):
    """Simple utility to get the list of CEs for DIRAC site name.

       Example usage:

       >>> print diracAdmin.getSiteSection('LCG.CERN.ch')
       {'OK': True, 'Value':}

       :returns: S_OK,S_ERROR
    """
    gridType = site.split( '.' )[0]
    if not gConfig.getSections( '/Resources/Sites/%s' % ( gridType ) )['OK']:
      return S_ERROR( '/Resources/Sites/%s is not a valid site section' % ( gridType ) )

    result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s' % ( gridType, site ) )
    if printOutput and result['OK']:
      print self.pPrint.pformat( result['Value'] )
    return result

  #############################################################################
  def getCSDict( self, sectionPath ):
    """Retrieve a dictionary from the CS for the specified path.

       Example usage:

       >>> print diracAdmin.getCSDict('Resources/Computing/OSCompatibility')
       {'OK': True, 'Value': {'slc4_amd64_gcc34': 'slc4_ia32_gcc34,slc4_amd64_gcc34', 'slc4_ia32_gcc34': 'slc4_ia32_gcc34'}}

       :returns: S_OK,S_ERROR

    """
    result = gConfig.getOptionsDict( sectionPath )
    return result

  #############################################################################
  def addSiteInMask( self, site, comment, printOutput = False ):
    """Adds the site to the site mask.

       Example usage:

       >>> print diracAdmin.addSiteInMask()
       {'OK': True, 'Value': }

       :returns: S_OK,S_ERROR

    """
    
    mask = self.getSiteMask()
    if not mask['OK']:
      return mask
    siteMask = mask['Value']
    if site in siteMask:
      return S_ERROR( 'Site %s already in mask of allowed sites' % site )
    
    result = self.__changeSiteStatus( site, comment, 'ComputingAccess', 
                                      'Active', printOutput=printOutput)
    if printOutput:
      if result['OK']:
        print 'Allowing %s in site mask' % site
      else:
        print "Failed to update status for site %s" % site
        
    return result    

  #############################################################################
  def __changeSiteStatus( self, site, comment, statusType, status, printOutput = False ):
    """
      Change the RSS status of the given site
    """
    result = self.__checkSiteIsValid( site )
    if not result['OK']:
      return result

    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.allowSite( site, comment )
    if not result['OK']:
      return result
    
    rsc = ResourceStatusClient()
    proxyInfo = getProxyInfo()
    if not proxyInfo[ 'OK' ]:
      return proxyInfo
    userName = proxyInfo[ 'Value' ][ 'username' ]   
    
    tomorrow = datetime.utcnow().replace( microsecond = 0 ) + timedelta( days = 1 )
  
    result = rsc.modifyStatusElement( 'Site', 'Status', 
                                      name = site, 
                                      statusType = statusType,
                                      status     = status,
                                      reason     = comment,  
                                      tokenOwner = userName, 
                                      tokenExpiration = tomorrow )

    return result

  #############################################################################
  def getSiteMaskLogging( self, site = None, printOutput = False ):
    """Retrieves site mask logging information.

       Example usage:

       >>> print diracAdmin.getSiteMaskLogging('LCG.AUVER.fr')
       {'OK': True, 'Value': }

       :returns: S_OK,S_ERROR
    """
    result = self.__checkSiteIsValid( site )
    if not result['OK']:
      return result
    
    rssClient = ResourceStatusClient()
    result = rssClient.selectStatusElement( 'Site', 'History', name = site, 
                                            statusType = 'ComputingAccess' )
    
    if not result['OK']:
      return result

    siteDict = {}
    for logTuple in result['Value']:
      status,reason,siteName,dateEffective,dateTokenExpiration,eType,sType,eID,lastCheckTime,author = logTuple
      result = getSiteFullNames( siteName )
      if not result['OK']:
        continue
      for sName in result['Value']:
        if site is None or (site and site == sName):
          siteDict.setdefault( sName, [] )
          siteDict[sName].append( (status,reason,dateEffective,author,dateTokenExpiration) )

    if printOutput:
      if site:
        print '\nSite Mask Logging Info for %s\n' % site
      else:
        print '\nAll Site Mask Logging Info\n'

      for site, tupleList in siteDict.items():
        if not site:
          print '\n===> %s\n' % site
        for tup in tupleList:
          print str( tup[0] ).ljust( 8 ) + str( tup[1] ).ljust( 20 ) + \
               '( ' + str( tup[2] ).ljust( len( str( tup[2] ) ) ) + ' )  "' + str( tup[3] ) + '"'
        print ' '
        
    return S_OK( siteDict )

  #############################################################################
  def banSiteFromMask( self, site, comment, printOutput = False ):
    """Removes the site from the site mask.

       Example usage:

       >>> print diracAdmin.banSiteFromMask("LCG.CERN.ch", "Job can't access their data")
       {'OK': True, 'Value': }

       :returns: S_OK,S_ERROR

    """
    result = self.__changeSiteStatus( site, comment, 'ComputingAccess', 
                                     'Banned', printOutput=printOutput)
    
    if printOutput:
      if result['OK']:
        print 'Banning %s in site mask' % site
      else:
        print "Failed to update status for site %s" % site
        
    return result    

  #############################################################################
  @classmethod
  def __checkSiteIsValid( self, site ):
    """Internal function to check that a site name is valid.
    """
    result = getSites()
    if not result['OK']:
      return S_ERROR( 'Could not get site CE mapping' )
    siteList = result['Value']
    if site in siteList:
      return S_OK( '%s is valid' % site )
    
    result = getSites( fullName = True )    
    if not result['OK']:
      return S_ERROR( 'Could not get site CE mapping' )
    siteList = result['Value']
    if site in siteList:
      return S_OK( '%s is valid' % site )
    
    return S_ERROR( 'Specified site %s is not in list of defined sites' % site )

  #############################################################################
  def getServicePorts( self, setup = '', printOutput = False ):
    """Checks the service ports for the specified setup.  If not given this is
       taken from the current installation (/DIRAC/Setup)

       Example usage:

       >>> print diracAdmin.getServicePorts()
       {'OK': True, 'Value':''}

       :returns: S_OK,S_ERROR

    """
    if not setup:
      setup = gConfig.getValue( '/DIRAC/Setup', '' )

    setupList = gConfig.getSections( '/DIRAC/Setups', [] )
    if not setupList['OK']:
      return S_ERROR( 'Could not get /DIRAC/Setups sections' )
    setupList = setupList['Value']
    if not setup in setupList:
      return S_ERROR( 'Setup %s is not in allowed list: %s' % ( setup, ', '.join( setupList ) ) )

    serviceSetups = gConfig.getOptionsDict( '/DIRAC/Setups/%s' % setup )
    if not serviceSetups['OK']:
      return S_ERROR( 'Could not get /DIRAC/Setups/%s options' % setup )
    serviceSetups = serviceSetups['Value'] #dict
    systemList = gConfig.getSections( '/Systems' )
    if not systemList['OK']:
      return S_ERROR( 'Could not get Systems sections' )
    systemList = systemList['Value']
    result = {}
    for system in systemList:
      if serviceSetups.has_key( system ):
        path = '/Systems/%s/%s/Services' % ( system, serviceSetups[system] )
        servicesList = gConfig.getSections( path )
        if not servicesList['OK']:
          self.log.warn( 'Could not get sections in %s' % path )
        else:
          servicesList = servicesList['Value']
          if not servicesList:
            servicesList = []
          self.log.verbose( 'System: %s ServicesList: %s' % ( system, ', '.join( servicesList ) ) )
          for service in servicesList:
            spath = '%s/%s/Port' % ( path, service )
            servicePort = gConfig.getValue( spath, 0 )
            if servicePort:
              self.log.verbose( 'Found port for %s/%s = %s' % ( system, service, servicePort ) )
              result['%s/%s' % ( system, service )] = servicePort
            else:
              self.log.warn( 'No port found for %s' % spath )
      else:
        self.log.warn( '%s is not defined in /DIRAC/Setups/%s' % ( system, setup ) )

    if printOutput:
      print self.pPrint.pformat( result )

    return S_OK( result )

  #############################################################################
  def getProxy( self, userDN, userGroup, validity = 43200, limited = False ):
    """Retrieves a proxy with default 12hr validity and stores
       this in a file in the local directory by default.

       Example usage:

       >>> print diracAdmin.getProxy()
       {'OK': True, 'Value': }

       :returns: S_OK,S_ERROR

    """
    return gProxyManager.downloadProxy( userDN, userGroup, limited = limited,
                                        requiredTimeLeft = validity )

  #############################################################################
  def getVOMSProxy( self, userDN, userGroup, vomsAttr = False, validity = 43200, limited = False ):
    """Retrieves a proxy with default 12hr validity and VOMS extensions and stores
       this in a file in the local directory by default.

       Example usage:

       >>> print diracAdmin.getVOMSProxy()
       {'OK': True, 'Value': }

       :returns: S_OK,S_ERROR

    """
    return gProxyManager.downloadVOMSProxy( userDN, userGroup, limited = limited,
                                            requiredVOMSAttribute = vomsAttr,
                                            requiredTimeLeft = validity )

  #############################################################################
  def getPilotProxy( self, userDN, userGroup, validity = 43200 ):
    """Retrieves a pilot proxy with default 12hr validity and stores
       this in a file in the local directory by default.

       Example usage:

       >>> print diracAdmin.getVOMSProxy()
       {'OK': True, 'Value': }

       :returns: S_OK,S_ERROR

    """

    return gProxyManager.getPilotProxyFromDIRACGroup( userDN, userGroup, requiredTimeLeft = validity )

  #############################################################################
  def resetJob( self, jobID ):
    """Reset a job or list of jobs in the WMS.  This operation resets the reschedule
       counter for a job or list of jobs and allows them to run as new.

       >>> print dirac.reset(12345)
       {'OK': True, 'Value': [12345]}

       :param job: JobID
       :type job: integer or list of integers
       :returns: S_OK,S_ERROR

    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or convertible integer for existing jobID' )
    elif type( jobID ) == type( [] ):
      try:
        jobID = [int( job ) for job in jobID]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or convertible integer for existing jobIDs' )

    jobManager = RPCClient( 'WorkloadManagement/JobManager', useCertificates = False )
    result = jobManager.resetJob( jobID )
    return result

  #############################################################################
  def getJobPilotOutput( self, jobID, directory = '' ):
    """Retrieve the pilot output for an existing job in the WMS.
       The output will be retrieved in a local directory unless
       otherwise specified.

       >>> print dirac.getJobPilotOutput(12345)
       {'OK': True, StdOut:'',StdError:''}

       :param job: JobID
       :type job: integer or string
       :returns: S_OK,S_ERROR
    """
    if not directory:
      directory = self.currentDir

    if not os.path.exists( directory ):
      return self._errorReport( 'Directory %s does not exist' % directory )

    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.getJobPilotOutput( jobID )
    if not result['OK']:
      return result

    outputPath = '%s/pilot_%s' % ( directory, jobID )
    if os.path.exists( outputPath ):
      self.log.info( 'Remove %s and retry to continue' % outputPath )
      return S_ERROR( 'Remove %s and retry to continue' % outputPath )

    if not os.path.exists( outputPath ):
      self.log.verbose( 'Creating directory %s' % outputPath )
      os.mkdir( outputPath )

    outputs = result['Value']
    if outputs.has_key( 'StdOut' ):
      stdout = '%s/std.out' % ( outputPath )
      fopen = open( stdout, 'w' )
      fopen.write( outputs['StdOut'] )
      fopen.close()
      self.log.verbose( 'Standard output written to %s' % ( stdout ) )
    else:
      self.log.warn( 'No standard output returned' )

    if outputs.has_key( 'StdError' ):
      stderr = '%s/std.err' % ( outputPath )
      fopen = open( stderr, 'w' )
      fopen.write( outputs['StdError'] )
      fopen.close()
      self.log.verbose( 'Standard error written to %s' % ( stderr ) )
    else:
      self.log.warn( 'No standard error returned' )

    self.log.info( 'Outputs retrieved in %s' % outputPath )
    return result

  #############################################################################
  def getPilotOutput( self, gridReference, directory = '' ):
    """Retrieve the pilot output  (std.out and std.err) for an existing job in the WMS.

       >>> print dirac.getJobPilotOutput(12345)
       {'OK': True, 'Value': {}}

       :param job: JobID
       :type job: integer or string
       :returns: S_OK,S_ERROR
    """
    if not type( gridReference ) == type( " " ):
      return self._errorReport( 'Expected string for pilot reference' )

    if not directory:
      directory = self.currentDir

    if not os.path.exists( directory ):
      return self._errorReport( 'Directory %s does not exist' % directory )

    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.getPilotOutput( gridReference )
    if not result['OK']:
      return result

    gridReferenceSmall = gridReference.split( '/' )[-1]
    if not gridReferenceSmall:
      gridReferenceSmall = 'reference'
    outputPath = '%s/pilot_%s' % ( directory, gridReferenceSmall )

    if os.path.exists( outputPath ):
      self.log.info( 'Remove %s and retry to continue' % outputPath )
      return S_ERROR( 'Remove %s and retry to continue' % outputPath )

    if not os.path.exists( outputPath ):
      self.log.verbose( 'Creating directory %s' % outputPath )
      os.mkdir( outputPath )

    outputs = result['Value']
    if outputs.has_key( 'StdOut' ):
      stdout = '%s/std.out' % ( outputPath )
      fopen = open( stdout, 'w' )
      fopen.write( outputs['StdOut'] )
      fopen.close()
      self.log.info( 'Standard output written to %s' % ( stdout ) )
    else:
      self.log.warn( 'No standard output returned' )

    if outputs.has_key( 'StdErr' ):
      stderr = '%s/std.err' % ( outputPath )
      fopen = open( stderr, 'w' )
      fopen.write( outputs['StdErr'] )
      fopen.close()
      self.log.info( 'Standard error written to %s' % ( stderr ) )
    else:
      self.log.warn( 'No standard error returned' )

    self.log.notice( 'Outputs retrieved in %s' % outputPath )
    return result

  #############################################################################
  def getPilotInfo( self, gridReference ):
    """Retrieve info relative to a pilot reference

       >>> print dirac.getPilotInfo(12345)
       {'OK': True, 'Value': {}}

       :param gridReference: Pilot Job Reference
       :type gridReference: string
       :returns: S_OK,S_ERROR
    """
    if not type( gridReference ) == type( " " ):
      return self._errorReport( 'Expected string for pilot reference' )

    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.getPilotInfo( gridReference )
    return result
  
  #############################################################################
  def killPilot( self, gridReference ):
    """Kill the pilot specified

       >>> print dirac.getPilotInfo(12345)
       {'OK': True, 'Value': {}}

       :param gridReference: Pilot Job Reference
       :returns: S_OK,S_ERROR
    """
    if not type( gridReference ) == type( " " ):
      return self._errorReport( 'Expected string for pilot reference' )

    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.killPilot( gridReference )
    return result

  #############################################################################
  def getPilotLoggingInfo( self, gridReference ):
    """Retrieve the pilot logging info for an existing job in the WMS.

       >>> print dirac.getPilotLoggingInfo(12345)
       {'OK': True, 'Value': {"The output of the command"}}

       :param gridReference: Gridp pilot job reference Id
       :type gridReference: string
       :returns: S_OK,S_ERROR
    """
    if type( gridReference ) not in types.StringTypes:
      return self._errorReport( 'Expected string for pilot reference' )

    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    return wmsAdmin.getPilotLoggingInfo( gridReference )

  #############################################################################
  def getJobPilots( self, jobID ):
    """Extract the list of submitted pilots and their status for a given
       jobID from the WMS.  Useful information is printed to the screen.

       >>> print dirac.getJobPilots()
       {'OK': True, 'Value': {PilotID:{StatusDict}}}

       :param job: JobID
       :type job: integer or string
       :returns: S_OK,S_ERROR

    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.getPilots( jobID )
    if result['OK']:
      print self.pPrint.pformat( result['Value'] )
    return result

  #############################################################################
  def getPilotSummary( self, startDate = '', endDate = '' ):
    """Retrieve the pilot output for an existing job in the WMS.  Summary is
       printed at INFO level, full dictionary of results also returned.

       >>> print dirac.getPilotSummary()
       {'OK': True, 'Value': {CE:{Status:Count}}}

       :param job: JobID
       :type job: integer or string
       :returns: S_OK,S_ERROR
    """
    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.getPilotSummary( startDate, endDate )
    if not result['OK']:
      return result

    ceDict = result['Value']
    headers = 'CE'.ljust( 28 )
    i = 0
    for ce, summary in ceDict.items():
      states = summary.keys()
      if len( states ) > i:
        i = len( states )

    for i in xrange( i ):
      headers += 'Status'.ljust( 12 ) + 'Count'.ljust( 12 )
    print headers

    for ce, summary in ceDict.items():
      line = ce.ljust( 28 )
      states = summary.keys()
      states.sort()
      for state in states:
        count = str( summary[state] )
        line += state.ljust( 12 ) + count.ljust( 12 )
      print line

    return result

  #############################################################################
  def selectRequests( self, jobID = None, requestID = None, requestName = None,
                      requestType = None, status = None, operation = None, ownerDN = None,
                      ownerGroup = None, requestStart = 0, limit = 100, printOutput = False ):
    """ Select requests from the request management system. A few notes on the selection criteria:
        - jobID is the WMS JobID for the request (if applicable)
        - requestID is assigned during submission of the request
        - requestName is the corresponding XML file name
        - requestType e.g. 'transfer'
        - status e.g. Done
        - operation e.g. replicateAndRegister
        - requestStart e.g. the first request to consider (start from 0 by default)
        - limit e.g. selection limit (default 100)

       >>> dirac.selectRequests(jobID='4894')
       {'OK': True, 'Value': [[<Requests>]]}
    """
    options = {'RequestID':requestID, 'RequestName':requestName, 'JobID':jobID, 'OwnerDN':ownerDN,
               'OwnerGroup':ownerGroup, 'RequestType':requestType, 'Status':status, 'Operation':operation}

    conditions = {}
    for key, value in options.items():
      if value:
        try:
          conditions[key] = str( value )
        except Exception, x:
          return self._errorReport( str( x ), 'Expected string for %s field' % key )

    try:
      requestStart = int( requestStart )
      limit = int( limit )
    except Exception, x:
      return self._errorReport( str( x ), 'Expected integer for %s field' % limit )

    self.log.verbose( 'Will select requests with the following conditions' )
    self.log.verbose( self.pPrint.pformat( conditions ) )
    requestClient = RPCClient( "RequestManagement/centralURL" )
    result = requestClient.getRequestSummaryWeb( conditions, [], requestStart, limit )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result

    requestIDs = result['Value']
    conds = []
    for key, value in conditions.items():
      if value:
        conds.append( '%s = %s' % ( key, value ) )
    self.log.verbose( '%s request(s) selected with conditions %s and limit %s' % ( len( requestIDs['Records'] ),
                                                                                   ', '.join( conds ), limit ) )
    if printOutput:
      requests = []
      if len( requestIDs['Records'] ) > limit:
        requestList = requestIDs['Records']
        requests = requestList[:limit]
      else:
        requests = requestIDs['Records']
      print '%s request(s) selected with conditions %s and limit %s' % ( len( requestIDs['Records'] ),
                                                                         ', '.join( conds ), limit )
      print requestIDs['ParameterNames']
      for request in requests:
        print request
    if not requestIDs:
      return S_ERROR( 'No requests selected for conditions: %s' % conditions )
    else:
      return result

  #############################################################################
  def getRequestSummary( self, printOutput = False ):
    """ Get a summary of the requests in the request DB.
    """
    requestClient = RPCClient( "RequestManagement/centralURL", timeout = 120 )
    result = requestClient.getDBSummary()
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result

    if printOutput:
      print self.pPrint.pformat( result['Value'] )

    return result

  #############################################################################
  def getExternalPackageVersions( self ):
    """ Simple function that attempts to obtain the external versions for
        the local DIRAC installation (frequently needed for debugging purposes).
    """
    gLogger.info( 'DIRAC version v%dr%d build %d' % ( DIRAC.majorVersion, DIRAC.minorVersion, DIRAC.patchLevel ) )
    try:
      import lcg_util
      infoStr = 'Using lcg_util from: \n%s' % lcg_util.__file__
      gLogger.info( infoStr )
      infoStr = "The version of lcg_utils is %s" % lcg_util.lcg_util_version()
      gLogger.info( infoStr )
    except Exception, x:
      errStr = "SRM2Storage.__init__: Failed to import lcg_util: %s" % ( x )
      gLogger.exception( errStr )

    try:
      import gfalthr as gfal
      infoStr = "Using gfalthr from: \n%s" % gfal.__file__
      gLogger.info( infoStr )
      infoStr = "The version of gfalthr is %s" % gfal.gfal_version()
      gLogger.info( infoStr )
    except Exception, x:
      errStr = "SRM2Storage.__init__: Failed to import gfalthr: %s." % ( x )
      gLogger.warn( errStr )
      try:
        import gfal
        infoStr = "Using gfal from: %s" % gfal.__file__
        gLogger.info( infoStr )
        infoStr = "The version of gfal is %s" % gfal.gfal_version()
        gLogger.info( infoStr )
      except Exception, x:
        errStr = "SRM2Storage.__init__: Failed to import gfal: %s" % ( x )
        gLogger.exception( errStr )


    defaultProtocols = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )
    gLogger.info( 'Default list of protocols are: %s' % ( ', '.join( defaultProtocols ) ) )
    return S_OK()

  #############################################################################
  def getSiteProtocols( self, site, printOutput = False ):
    """Allows to check the defined protocols for each site SE.
    """
    result = self.__checkSiteIsValid( site )
    if not result['OK']:
      return result

    siteSection = '/Resources/Sites/%s/%s/SE' % ( site.split( '.' )[0], site )
    siteSEs = gConfig.getValue( siteSection, [] )
    if not siteSEs:
      return S_ERROR( 'No SEs found for site %s in section %s' % ( site, siteSection ) )

    defaultProtocols = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )
    self.log.verbose( 'Default list of protocols are' ', '.join( defaultProtocols ) )
    seInfo = {}
    siteSEs.sort()
    for se in siteSEs:
      sections = gConfig.getSections( '/Resources/StorageElements/%s/' % ( se ) )
      if not sections['OK']:
        return sections
      for section in sections['Value']:
        if gConfig.getValue( '/Resources/StorageElements/%s/%s/ProtocolName' % ( se, section ), '' ) == 'SRM2':
          path = '/Resources/StorageElements/%s/%s/ProtocolsList' % ( se, section )
          seProtocols = gConfig.getValue( path, [] )
          if not seProtocols:
            seProtocols = defaultProtocols
          seInfo[se] = seProtocols

    if printOutput:
      print '\nSummary of protocols for StorageElements at site %s' % site
      print '\nStorageElement'.ljust( 30 ) + 'ProtocolsList'.ljust( 30 ) + '\n'
      for se, protocols in seInfo.items():
        print se.ljust( 30 ) + ', '.join( protocols ).ljust( 30 )

    return S_OK( seInfo )

  #############################################################################
  def setSiteProtocols( self, site, protocolsList, printOutput = False ):
    """Allows to set the defined protocols for each SE for a given site.
    """
    result = self.__checkSiteIsValid( site )
    if not result['OK']:
      return result

    siteSection = '/Resources/Sites/%s/%s/SE' % ( site.split( '.' )[0], site )
    siteSEs = gConfig.getValue( siteSection, [] )
    if not siteSEs:
      return S_ERROR( 'No SEs found for site %s in section %s' % ( site, siteSection ) )

    defaultProtocols = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )
    self.log.verbose( 'Default list of protocols are', ', '.join( defaultProtocols ) )

    for protocol in protocolsList:
      if not protocol in defaultProtocols:
        return S_ERROR( 'Requested to set protocol %s in list but %s is not '
                        'in default list of protocols:\n%s' % ( protocol, protocol, ', '.join( defaultProtocols ) ) )

    modifiedCS = False
    result = promptUser( 'Do you want to add the following default protocols:'
                         ' %s for SE(s):\n%s' % ( ', '.join( protocolsList ), ', '.join( siteSEs ) ) )
    if not result['OK']:
      return result
    if result['Value'].lower() != 'y':
      self.log.always( 'No protocols will be added' )
      return S_OK()

    for se in siteSEs:
      sections = gConfig.getSections( '/Resources/StorageElements/%s/' % ( se ) )
      if not sections['OK']:
        return sections
      for section in sections['Value']:
        if gConfig.getValue( '/Resources/StorageElements/%s/%s/ProtocolName' % ( se, section ), '' ) == 'SRM2':
          path = '/Resources/StorageElements/%s/%s/ProtocolsList' % ( se, section )
          self.log.verbose( 'Setting %s to %s' % ( path, ', '.join( protocolsList ) ) )
          result = self.csSetOption( path, ', '.join( protocolsList ) )
          if not result['OK']:
            return result
          modifiedCS = True

    if modifiedCS:
      result = self.csCommitChanges( False )
      if not result[ 'OK' ]:
        return S_ERROR( 'CS Commit failed with message = %s' % ( result[ 'Message' ] ) )
      else:
        if printOutput:
          print 'Successfully committed changes to CS'
    else:
      if printOutput:
        print 'No modifications to CS required'

    return S_OK()

  #############################################################################
  def csSetOption( self, optionPath, optionValue ):
    """Function to modify an existing value in the CS.
    """
    return self.csAPI.setOption( optionPath, optionValue )

  #############################################################################
  def csSetOptionComment( self, optionPath, comment ):
    """Function to modify an existing value in the CS.
    """
    return self.csAPI.setOptionComment( optionPath, comment )

  #############################################################################
  def csModifyValue( self, optionPath, newValue ):
    """Function to modify an existing value in the CS.
    """
    return self.csAPI.modifyValue( optionPath, newValue )

  #############################################################################
  def csRegisterUser( self, username, properties ):
    """Registers a user in the CS.
        - username: Username of the user (easy;)
        - properties: Dict containing:
            - DN
            - groups : list/tuple of groups the user belongs to
            - <others> : More properties of the user, like mail
    """
    return self.csAPI.addUser( username, properties )

  #############################################################################
  def csDeleteUser( self, user ):
    """Deletes a user from the CS. Can take a list of users
    """
    return self.csAPI.deleteUsers( user )

  #############################################################################
  def csModifyUser( self, username, properties, createIfNonExistant = False ):
    """Modify a user in the CS. Takes the same params as in addUser and applies
      the changes
    """
    return self.csAPI.modifyUser( username, properties, createIfNonExistant )

  #############################################################################
  def csListUsers( self, group = False ):
    """Lists the users in the CS. If no group is specified return all users.
    """
    return self.csAPI.listUsers( group )

  #############################################################################
  def csDescribeUsers( self, mask = False ):
    """List users and their properties in the CS.
        If a mask is given, only users in the mask will be returned
    """
    return self.csAPI.describeUsers( mask )

  #############################################################################
  def csModifyGroup( self, groupname, properties, createIfNonExistant = False ):
    """Modify a user in the CS. Takes the same params as in addGroup and applies
      the changes
    """
    return self.csAPI.modifyGroup( groupname, properties, createIfNonExistant )

  #############################################################################
  def csListHosts( self ):
    """Lists the hosts in the CS
    """
    return self.csAPI.listHosts()

  #############################################################################
  def csDescribeHosts( self, mask = False ):
    """Gets extended info for the hosts in the CS
    """
    return self.csAPI.describeHosts( mask )

  #############################################################################
  def csModifyHost( self, hostname, properties, createIfNonExistant = False ):
    """Modify a host in the CS. Takes the same params as in addHost and applies
      the changes
    """
    return self.csAPI.modifyHost( hostname, properties, createIfNonExistant )

  #############################################################################
  def csListGroups( self ):
    """Lists groups in the CS
    """
    return self.csAPI.listGroups()

  #############################################################################
  def csDescribeGroups( self, mask = False ):
    """List groups and their properties in the CS.
        If a mask is given, only groups in the mask will be returned
    """
    return self.csAPI.describeGroups( mask )

  #############################################################################
  def csSyncUsersWithCFG( self, usersCFG ):
    """Synchronize users in cfg with its contents
    """
    return self.csAPI.syncUsersWithCFG( usersCFG )

  #############################################################################
  def csCommitChanges( self, sortUsers = True ):
    """Commit the changes in the CS
    """
    return self.csAPI.commitChanges( sortUsers = False )

  #############################################################################
  def sendMail( self, address, subject, body, fromAddress = None, localAttempt = True ):
    """ Send mail to specified address with body.
    """
    notification = NotificationClient()
    return notification.sendMail( address, subject, body, fromAddress, localAttempt )

  #############################################################################
  def sendSMS( self, userName, body, fromAddress = None ):
    """ Send mail to specified address with body.
    """
    if len( body ) > 160:
      return S_ERROR( 'Exceeded maximum SMS length of 160 characters' )
    notification = NotificationClient()
    return notification.sendSMS( userName, body, fromAddress )

  #############################################################################
  def getBDIISite( self, site, host = None ):
    """Get information about site from BDII at host
    """
    return ldapSite( site, host = host )

  #############################################################################
  def getBDIICluster( self, ce, host = None ):
    """Get information about ce from BDII at host
    """
    return ldapCluster( ce, host = host )

  #############################################################################
  def getBDIICE( self, ce, host = None ):
    """Get information about ce from BDII at host
    """
    return ldapCE( ce, host = host )

  #############################################################################
  def getBDIIService( self, ce, host = None ):
    """Get information about ce from BDII at host
    """
    return ldapService( ce, host = host )

  #############################################################################
  def getBDIICEState( self, ce, useVO = voName, host = None ):
    """Get information about ce state from BDII at host
    """
    return ldapCEState( ce, useVO, host = host )

  #############################################################################
  def getBDIICEVOView( self, ce, useVO = voName, host = None ):
    """Get information about ce voview from BDII at host
    """
    return ldapCEVOView( ce, useVO, host = host )

  #############################################################################
  def getBDIISA( self, site, useVO = voName, host = None ):
    """Get information about SA  from BDII at host
    """
    return ldapSA( site, useVO, host = host )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
