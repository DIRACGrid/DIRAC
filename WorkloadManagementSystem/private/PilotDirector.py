########################################################################
# $HeadURL$
# File :   PilotDirector.py
# Author : Ricardo Graciani
########################################################################
"""
  Base PilotDirector class to be inherited by DIRAC and Grid specific PilotDirectors, inherited by MW
  specific PilotDirectors if appropriated.
  It includes:
   - basic configuration functionality

  The main difference between DIRAC and Grid Pilot Directors is that in the first case
  DIRAC talks directly to the local resources via a DIRAC CE class, while in the second
  many CE's are address at the same time via a Grid Resource Broker.
  This means that DIRAC direct submission to Grid CE's (CREAM, ...) will be handled by DIRAC Pilot
  Director making use of a DIRAC CREAM Computing Element class
"""
__RCSID__ = "$Id$"


import os, random
random.seed()


import DIRAC
# Some reasonable Defaults
DIRAC_PILOT = os.path.join( DIRAC.rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'dirac-pilot.py' )
DIRAC_INSTALL = os.path.join( DIRAC.rootPath, 'DIRAC', 'Core', 'scripts', 'dirac-install.py' )
DIRAC_VERSION = 'Integration'
DIRAC_PROJECT = ''
DIRAC_INSTALLATION = ''

MAX_JOBS_IN_FILLMODE = 2

ERROR_CLEAR_TIME = 60 * 60  # 1 hour
ERROR_TICKET_TIME = 60 * 60  # 1 hour (added to the above)
FROM_MAIL = "diracproject@gmail.com"

VIRTUAL_ORGANIZATION = 'dirac'

ENABLE_LISTMATCH = 1
LISTMATCH_DELAY = 5

PRIVATE_PILOT_FRACTION = 0.5

ERROR_PROXY = 'No proxy Available'
ERROR_TOKEN = 'Invalid proxy token request'
ERROR_GENERIC_CREDENTIALS = "Cannot find generic pilot credentials"

from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.WorkloadManagementSystem.private.ConfigHelper   import findGenericPilotCredentials
from DIRAC.ConfigurationSystem.Client.ConfigurationData    import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers              import getCSExtensions
from DIRAC.ConfigurationSystem.Client.Helpers.Path         import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Registry     import getVOForGroup, getPropertiesForGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Operations   import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources    import Resources
from DIRAC.ResourceStatusSystem.Client.SiteStatus          import SiteStatus

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities.DictCache import DictCache

#from DIRAC import S_OK, S_ERROR, gLogger, gConfig, List, Time, Source, systemCall, DictCache

class PilotDirector:
  """
    Base Pilot Director class.
    Derived classes must implement:
      * __init__( self, submitPool ):
          that must call the parent class __init__ method and then do its own initialization
      * configure( self, csSection, submitPool ):
          that must call the parent class configure method and the do its own configuration
      * _submitPilots( self, workDir, taskQueueDict, pilotOptions, pilotsToSubmit, ceMask,
                      submitPrivatePilot, privateTQ, proxy, pilotsPerJob )
          actual method doing the submission to the backend once the submitPilots method
          has prepared the common part

    Derived classes might implement:
      * configureFromSection( self, mySection ):
          to reload from a CS section the additional datamembers they might have defined.

    If additional datamembers are defined, they must:
      - be declared in the __init__
      - be reconfigured in the configureFromSection method by executing
        self.reloadConfiguration( csSection, submitPool ) in their configure method
  """
  gridMiddleware = ''

  def __init__( self, submitPool ):
    """
     Define the logger and some defaults
    """

    if submitPool == self.gridMiddleware:
      self.log = gLogger.getSubLogger( '%sPilotDirector' % self.gridMiddleware )
    else:
      self.log = gLogger.getSubLogger( '%sPilotDirector/%s' % ( self.gridMiddleware, submitPool ) )

    self.pilot = DIRAC_PILOT
    self.submitPoolOption = '-o /Resources/Computing/CEDefaults/SubmitPool=%s' % submitPool
    self.extraPilotOptions = []
    self.installVersion = DIRAC_VERSION
    self.installProject = DIRAC_PROJECT
    self.installation = DIRAC_INSTALLATION
    self.pilotExtensionsList = []

    self.virtualOrganization = VIRTUAL_ORGANIZATION
    self.install = DIRAC_INSTALL
    self.maxJobsInFillMode = MAX_JOBS_IN_FILLMODE
    self.targetGrids = [ self.gridMiddleware ]


    self.enableListMatch = ENABLE_LISTMATCH
    self.listMatchDelay = LISTMATCH_DELAY
    self.listMatchCache = DictCache()

    self.privatePilotFraction = PRIVATE_PILOT_FRACTION

    self.errorClearTime = ERROR_CLEAR_TIME
    self.errorTicketTime = ERROR_TICKET_TIME
    self.errorMailAddress = DIRAC.errorMail
    self.alarmMailAddress = DIRAC.alarmMail
    self.mailFromAddress = FROM_MAIL

    if not  'log' in self.__dict__:
      self.log = gLogger.getSubLogger( 'PilotDirector' )
    self.log.info( 'Initialized' )

  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for all PilotDirectors
    """
    self.configureFromSection( csSection )
    self.reloadConfiguration( csSection, submitPool )

    # Get the defaults for the Setup where the Director is running
    opsHelper = Operations()
    self.installVersion = opsHelper.getValue( cfgPath( 'Pilot', 'Version' ), [ self.installVersion ] )[0]
    self.installProject = opsHelper.getValue( cfgPath( 'Pilot', 'Project' ), self.installProject )
    self.installation = opsHelper.getValue( cfgPath( 'Pilot', 'Installation' ), self.installation )
    self.pilotExtensionsList = opsHelper.getValue( "Pilot/Extensions", self.pilotExtensionsList )

    self.log.info( '===============================================' )
    self.log.info( 'Configuration:' )
    self.log.info( '' )
    self.log.info( ' Target Grids:   ', ', '.join( self.targetGrids ) )
    self.log.info( ' Install script: ', self.install )
    self.log.info( ' Pilot script:   ', self.pilot )
    self.log.info( ' Install Ver:    ', self.installVersion )
    if self.installProject:
      self.log.info( ' Project:        ', self.installProject )
    if self.installation:
      self.log.info( ' Installation:   ', self.installation )
    if self.extraPilotOptions:
      self.log.info( ' Extra Options:   ', ' '.join( self.extraPilotOptions ) )
    self.log.info( ' ListMatch:      ', self.enableListMatch )
    self.log.info( ' Private %:      ', self.privatePilotFraction * 100 )
    if self.enableListMatch:
      self.log.info( ' ListMatch Delay:', self.listMatchDelay )
    self.listMatchCache.purgeExpired()

  def reloadConfiguration( self, csSection, submitPool ):
    """
     Common Configuration can be overwriten for each GridMiddleware
    """
    mySection = csSection + '/' + self.gridMiddleware
    self.configureFromSection( mySection )

    # And Again for each SubmitPool
    mySection = csSection + '/' + submitPool
    self.configureFromSection( mySection )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    self.pilot = gConfig.getValue( mySection + '/PilotScript'          , self.pilot )
    #TODO: Remove this DIRACVersion after 06/2012
    self.installVersion = gConfig.getValue( mySection + '/DIRACVersion'         , self.installVersion )
    self.installVersion = gConfig.getValue( mySection + '/Version'         , self.installVersion )
    self.extraPilotOptions = gConfig.getValue( mySection + '/ExtraPilotOptions'    , self.extraPilotOptions )
    self.install = gConfig.getValue( mySection + '/InstallScript'        , self.install )
    self.installProject = gConfig.getValue( mySection + '/Project'        , self.installProject )
    self.installation = gConfig.getValue( mySection + '/Installation'        , self.installation )
    self.maxJobsInFillMode = gConfig.getValue( mySection + '/MaxJobsInFillMode'    , self.maxJobsInFillMode )
    self.targetGrids = gConfig.getValue( mySection + '/TargetGrids'    , self.targetGrids )

    self.enableListMatch = gConfig.getValue( mySection + '/EnableListMatch'      , self.enableListMatch )
    self.listMatchDelay = gConfig.getValue( mySection + '/ListMatchDelay'       , self.listMatchDelay )
    self.errorClearTime = gConfig.getValue( mySection + '/ErrorClearTime'       , self.errorClearTime )
    self.errorTicketTime = gConfig.getValue( mySection + '/ErrorTicketTime'      , self.errorTicketTime )
    self.errorMailAddress = gConfig.getValue( mySection + '/ErrorMailAddress'     , self.errorMailAddress )
    self.alarmMailAddress = gConfig.getValue( mySection + '/AlarmMailAddress'     , self.alarmMailAddress )
    self.mailFromAddress = gConfig.getValue( mySection + '/MailFromAddress'      , self.mailFromAddress )
    self.privatePilotFraction = gConfig.getValue( mySection + '/PrivatePilotFraction' , self.privatePilotFraction )

    virtualOrganization = gConfig.getValue( mySection + '/VirtualOrganization' , '' )
    if not virtualOrganization:
      virtualOrganization = getVOForGroup( 'NonExistingGroup' )
      if not virtualOrganization:
        virtualOrganization = self.virtualOrganization
    self.virtualOrganization = virtualOrganization

  def _resolveCECandidates( self, taskQueueDict ):
    """
      Return a list of CEs for this TaskQueue
    """
    # assume user knows what they're doing and avoid site mask e.g. sam jobs
    if 'GridCEs' in taskQueueDict and taskQueueDict['GridCEs']:
      self.log.info( 'CEs requested by TaskQueue %s:' % taskQueueDict['TaskQueueID'],
                     ', '.join( taskQueueDict['GridCEs'] ) )
      return taskQueueDict['GridCEs']

    # Get the mask
    siteStatus = SiteStatus()
    ret = siteStatus.getUsableSites( 'ComputingAccess' )
    if not ret['OK']:
      self.log.error( 'Can not retrieve site Mask from DB:', ret['Message'] )
      return []

    usableSites = ret['Value']
    if not usableSites:
      self.log.error( 'Site mask is empty' )
      return []

    self.log.verbose( 'Site Mask: %s' % ', '.join( usableSites ) )

    # remove banned sites from siteMask
    if 'BannedSites' in taskQueueDict:
      for site in taskQueueDict['BannedSites']:
        if site in usableSites:
          usableSites.remove( site )
          self.log.verbose( 'Removing banned site %s from site Mask' % site )

    # remove from the mask if a Site is given
    siteMask = [ site for site in usableSites if 'Sites' not in taskQueueDict or site in taskQueueDict['Sites'] ]

    if not siteMask:
      # pilot can not be submitted
      self.log.info( 'No Valid Site Candidate in Mask for TaskQueue %s' % taskQueueDict['TaskQueueID'] )
      return []

    self.log.info( 'Site Candidates for TaskQueue %s:' % taskQueueDict['TaskQueueID'], ', '.join( siteMask ) )

    # Get CE's associates to the given site Names
    ceMask = []

    resources = Resources( vo = self.virtualOrganization )
    result = resources.getEligibleResources( 'Computing', {'Site':siteMask,
                                                           'SubmissionMode':'gLite',
                                                           'CEType':['LCG','CREAM']} )
    if not result['OK']:
      self.log.error( "Failed to get eligible ce's:", result['Message'] )
      return []
    ces = result['Value']

    for ce in ces:
      ceHost = resources.getComputingElementValue( ce, 'Host', 'unknown' )
      if ceHost != 'unknown':
        ceMask.append( ceHost )

    if not ceMask:
      self.log.info( 'No CE Candidate found for TaskQueue %s:' % taskQueueDict['TaskQueueID'], ', '.join( siteMask ) )

    self.log.verbose( 'CE Candidates for TaskQueue %s:' % taskQueueDict['TaskQueueID'], ', '.join( ceMask ) )

    return ceMask

  def _getPilotOptions( self, taskQueueDict, pilotsToSubmit ):

    # Need to limit the maximum number of pilots to submit at once
    # For generic pilots this is limited by the number of use of the tokens and the
    # maximum number of jobs in Filling mode, but for private Jobs we need an extra limitation:
    pilotsToSubmit = max( min( pilotsToSubmit, int( 50 / self.maxJobsInFillMode ) ), 1 )
    pilotOptions = []
    privateIfGenericTQ = self.privatePilotFraction > random.random()
    privateTQ = ( 'PilotTypes' in taskQueueDict and 'private' in [ t.lower() for t in taskQueueDict['PilotTypes'] ] )
    forceGeneric = 'ForceGeneric' in taskQueueDict
    submitPrivatePilot = ( privateIfGenericTQ or privateTQ ) and not forceGeneric
    if submitPrivatePilot:
      self.log.verbose( 'Submitting private pilots for TaskQueue %s' % taskQueueDict['TaskQueueID'] )
      ownerDN = taskQueueDict['OwnerDN']
      ownerGroup = taskQueueDict['OwnerGroup']
      # User Group requirement
      pilotOptions.append( '-G %s' % taskQueueDict['OwnerGroup'] )
      # check if group allows jobsharing
      ownerGroupProperties = getPropertiesForGroup( ownerGroup )
      if not 'JobSharing' in ownerGroupProperties:
        # Add Owner requirement to pilot
        pilotOptions.append( "-O '%s'" % ownerDN )
      if privateTQ:
        pilotOptions.append( '-o /Resources/Computing/CEDefaults/PilotType=private' )
      maxJobsInFillMode = self.maxJobsInFillMode
    else:
      #For generic jobs we'll submit mixture of generic and private pilots
      self.log.verbose( 'Submitting generic pilots for TaskQueue %s' % taskQueueDict['TaskQueueID'] )
      #ADRI: Find the generic group
      result = findGenericPilotCredentials( group = taskQueueDict[ 'OwnerGroup' ] )
      if not result[ 'OK' ]:
        self.log.error( ERROR_GENERIC_CREDENTIALS, result[ 'Message' ] )
        return S_ERROR( ERROR_GENERIC_CREDENTIALS )
      ownerDN, ownerGroup = result[ 'Value' ]

      result = gProxyManager.requestToken( ownerDN, ownerGroup, max( pilotsToSubmit, self.maxJobsInFillMode ) )
      if not result[ 'OK' ]:
        self.log.error( ERROR_TOKEN, result['Message'] )
        return S_ERROR( ERROR_TOKEN )
      ( token, numberOfUses ) = result[ 'Value' ]
      pilotsToSubmit = min( numberOfUses, pilotsToSubmit )

      pilotOptions.append( '-o /Security/ProxyToken=%s' % token )

      pilotsToSubmit = max( 1, ( pilotsToSubmit - 1 ) / self.maxJobsInFillMode + 1 )

      maxJobsInFillMode = int( numberOfUses / pilotsToSubmit )
    # Use Filling mode
    pilotOptions.append( '-M %s' % maxJobsInFillMode )

    # Debug
    pilotOptions.append( '-d' )
    # Setup.
    pilotOptions.append( '-S %s' % taskQueueDict['Setup'] )
    # CS Servers
    csServers = gConfig.getServersList()
    if len( csServers ) > 3:
      # Remove the master
      master = gConfigurationData.getMasterServer()
      if master in csServers:
        csServers.remove( master )
    pilotOptions.append( '-C %s' % ",".join( csServers ) )
    # DIRAC Extensions to be used in pilots
    # ubeda: I'm not entirely sure if we can use here the same opsHelper as in line
    # line +352
    pilotExtensionsList = Operations().getValue( "Pilot/Extensions", [] )
    extensionsList = []
    if pilotExtensionsList:
      if pilotExtensionsList[0] != 'None':
        extensionsList = pilotExtensionsList
    else:
      extensionsList = getCSExtensions()
    if extensionsList:
      pilotOptions.append( '-e %s' % ",".join( extensionsList ) )

    #Get DIRAC version and project, There might be global Setup defaults and per VO/Setup defaults (from configure)
    opsHelper = Operations( group = taskQueueDict['OwnerGroup'], setup = taskQueueDict['Setup'] )
    # Requested version of DIRAC (it can be a list, so we take the fist one)
    version = opsHelper.getValue( cfgPath( 'Pilot', 'Version' ) , [ self.installVersion ] )[0]
    pilotOptions.append( '-r %s' % version )
    # Requested Project to install
    installProject = opsHelper.getValue( cfgPath( 'Pilot', 'Project' ) , self.installProject )
    if installProject:
      pilotOptions.append( '-l %s' % installProject )
    installation = opsHelper.getValue( cfgPath( 'Pilot', 'Installation' ), self.installation )
    if installation:
      pilotOptions.append( "-V %s" % installation )
    # Requested CPU time
    pilotOptions.append( '-T %s' % taskQueueDict['CPUTime'] )

    if self.submitPoolOption not in self.extraPilotOptions:
      pilotOptions.append( self.submitPoolOption )

    if self.extraPilotOptions:
      pilotOptions.extend( self.extraPilotOptions )

    return S_OK( ( pilotOptions, pilotsToSubmit, ownerDN, ownerGroup, submitPrivatePilot, privateTQ ) )

  def _submitPilots( self, workDir, taskQueueDict, pilotOptions, pilotsToSubmit,
                     ceMask, submitPrivatePilot, privateTQ, proxy, pilotsPerJob ):
    """
      This method must be implemented on the Backend specific derived class.
      This is problem with the Director, not with the Job so we must return S_OK
      Return S_ERROR if not defined.
    """
    self.log.error( '_submitPilots method not implemented' )
    return S_OK()


  def submitPilots( self, taskQueueDict, pilotsToSubmit, workDir = None ):
    """
      Submit pilot for the given TaskQueue,
      this method just insert the request in the corresponding ThreadPool,
      the submission is done from the Thread Pool job
    """
    try:

      taskQueueID = taskQueueDict['TaskQueueID']

      self.log.verbose( 'Submitting Pilot' )
      ceMask = self._resolveCECandidates( taskQueueDict )
      if not ceMask:
        return S_ERROR( 'No CE available for TaskQueue %d' % int( taskQueueID ) )
      result = self._getPilotOptions( taskQueueDict, pilotsToSubmit )
      if not result['OK']:
        return result
      ( pilotOptions, pilotsPerJob, ownerDN, ownerGroup, submitPrivatePilot, privateTQ ) = result['Value']
      # get a valid proxy, submit with a long proxy to avoid renewal
      ret = self._getPilotProxyFromDIRACGroup( ownerDN, ownerGroup, requiredTimeLeft = 86400 * 5 )
      if not ret['OK']:
        self.log.error( ret['Message'] )
        self.log.error( 'No proxy Available', 'User "%s", Group "%s"' % ( ownerDN, ownerGroup ) )
        return S_ERROR( ERROR_PROXY )
      proxy = ret['Value']
      # Now call a Grid Specific method to handle the final submission of the pilots
      return self._submitPilots( workDir, taskQueueDict, pilotOptions,
                                 pilotsToSubmit, ceMask,
                                 submitPrivatePilot, privateTQ,
                                 proxy, pilotsPerJob )

    except Exception:
      self.log.exception( 'Error in Pilot Submission' )

    return S_OK( 0 )

  def _getPilotProxyFromDIRACGroup( self, ownerDN, ownerGroup, requiredTimeLeft ):
    """
     To be overwritten if a given Pilot does not require a full proxy
    """
    self.log.info( "Downloading %s@%s proxy" % ( ownerDN, ownerGroup ) )
    return gProxyManager.getPilotProxyFromDIRACGroup( ownerDN, ownerGroup, requiredTimeLeft )

  def exceptionCallBack( self, threadedJob, exceptionInfo ):
    self.log.exception( 'Error in Pilot Submission' )
