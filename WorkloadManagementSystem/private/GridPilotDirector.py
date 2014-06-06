########################################################################
# $HeadURL$
# File :   GridPilotDirector.py
# Author : Ricardo Graciani
########################################################################
"""
  Base Grid PilotDirector class to be inherited MW specific PilotDirectors gLite/LCG.
  It includes:
   - basic configuration for Grid PilotDirector

  A Grid PilotDirector make use of a Grid Resource Broker to place the pilots on the
  underlying resources.

"""
__RCSID__ = "$Id$"


GRIDENV = ''
CPU_POWER_REF = '250' # 250 SpecInt 2000 or 1 HepSpec 2006
REQUIREMENTS = ['Rank > -2']
RANK = '( other.GlueCEStateWaitingJobs == 0 ? other.GlueCEStateFreeCPUs * 10 / other.GlueCEInfoTotalCPUs : -other.GlueCEStateWaitingJobs * 4 / (other.GlueCEStateRunningJobs + 1 ) - 1 )'
FUZZY_RANK = 'true'

ERROR_VOMS = 'Proxy without VOMS Extensions'
ERROR_CE = 'No queue available for pilot'
ERROR_JDL = 'Could not create Grid JDL'
ERROR_RB = 'No Broker available'
ERROR_TOKEN = 'Invalid proxy token request'

import os, tempfile, time, re, shutil, sys

from DIRAC.WorkloadManagementSystem.Service.WMSUtilities   import outputSandboxFiles
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.WorkloadManagementSystem.private.PilotDirector  import PilotDirector
from DIRAC.FrameworkSystem.Client.NotificationClient       import NotificationClient
from DIRAC.Core.Security.ProxyInfo                         import getProxyInfoAsString
from DIRAC.Core.Utilities.Grid                             import executeGridCommand
from DIRAC.Core.Utilities                                  import Time, List
from DIRAC.WorkloadManagementSystem.Client.ServerUtils     import pilotAgentsDB
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.DictCache import DictCache

class GridPilotDirector( PilotDirector ):
  """
    Base Grid PilotDirector class
    Derived classes must declare:
      self.Middleware: It must correspond to the string before "PilotDirector".
        (For proper naming of the logger)
      self.ResourceBrokers: list of Brokers used by the Director.
        (For proper error reporting)
  """
  def __init__( self, submitPool ):
    """
     Define some defaults and call parent __init__
    """
    self.gridEnv = GRIDENV

    self.cpuPowerRef = CPU_POWER_REF
    self.requirements = REQUIREMENTS
    self.rank = RANK
    self.fuzzyRank = FUZZY_RANK

    self.__failingWMSCache = DictCache()
    self.__ticketsWMSCache = DictCache()
    self.__listMatchWMSCache = DictCache()

    PilotDirector.__init__( self, submitPool )

  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for all Grid PilotDirectors
    """
    PilotDirector.configure( self, csSection, submitPool )
    self.reloadConfiguration( csSection, submitPool )

    self.__failingWMSCache.purgeExpired()
    self.__ticketsWMSCache.purgeExpired()
    for rb in self.__failingWMSCache.getKeys():
      if rb in self.resourceBrokers:
        try:
          self.resourceBrokers.remove( rb )
        except:
          pass

    self.resourceBrokers = List.randomize( self.resourceBrokers )

    if self.gridEnv:
      self.log.info( ' GridEnv:        ', self.gridEnv )
    if self.resourceBrokers:
      self.log.info( ' ResourceBrokers:', ', '.join( self.resourceBrokers ) )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    PilotDirector.configureFromSection( self, mySection )

    self.gridEnv = gConfig.getValue( mySection + '/GridEnv', self.gridEnv )
    if not self.gridEnv:
      # No specific option found, try a general one
      setup = gConfig.getValue( '/DIRAC/Setup', '' )
      if setup:
        instance = gConfig.getValue( '/DIRAC/Setups/%s/WorkloadManagement' % setup, '' )
        if instance:
          self.gridEnv = gConfig.getValue( '/Systems/WorkloadManagement/%s/GridEnv' % instance, '' )

    self.resourceBrokers = gConfig.getValue( mySection + '/ResourceBrokers'      , self.resourceBrokers )

    self.cpuPowerRef = gConfig.getValue( mySection + '/CPUPowerRef'           , self.cpuPowerRef )
    self.requirements = gConfig.getValue( mySection + '/Requirements'         , self.requirements )
    self.rank = gConfig.getValue( mySection + '/Rank'                 , self.rank )
    self.fuzzyRank = gConfig.getValue( mySection + '/FuzzyRank'            , self.fuzzyRank )

  def _submitPilots( self, workDir, taskQueueDict, pilotOptions, pilotsToSubmit,
                     ceMask, submitPrivatePilot, privateTQ, proxy, pilotsPerJob ):
    """
      This method does the actual pilot submission to the Grid RB
      The logic is as follows:
      - If there are no available RB it return error
      - If there is no VOMS extension in the proxy, return error
      - It creates a temp directory
      - Prepare a JDL
        it has some part common to gLite and LCG (the payload description)
        it has some part specific to each middleware
    """
    taskQueueID = taskQueueDict['TaskQueueID']
    # ownerDN = taskQueueDict['OwnerDN']
    credDict = proxy.getCredentials()['Value']
    ownerDN = credDict['identity']
    ownerGroup = credDict[ 'group' ]

    if not self.resourceBrokers:
      # Since we can exclude RBs from the list, it may become empty
      return S_ERROR( ERROR_RB )

    # Need to get VOMS extension for the later interactions with WMS
    ret = gProxyManager.getVOMSAttributes( proxy )
    if not ret['OK']:
      self.log.error( ERROR_VOMS, ret['Message'] )
      return S_ERROR( ERROR_VOMS )
    if not ret['Value']:
      return S_ERROR( ERROR_VOMS )
    vomsGroup = ret['Value'][0]

    workingDirectory = tempfile.mkdtemp( prefix = 'TQ_%s_' % taskQueueID, dir = workDir )
    self.log.verbose( 'Using working Directory:', workingDirectory )

    # Write JDL
    retDict = self._prepareJDL( taskQueueDict, workingDirectory, pilotOptions, pilotsPerJob,
                                ceMask, submitPrivatePilot, privateTQ )
    jdl = retDict['JDL']
    pilotRequirements = retDict['Requirements']
    rb = retDict['RB']
    if not jdl:
      try:
        shutil.rmtree( workingDirectory )
      except:
        pass
      return S_ERROR( ERROR_JDL )

    # Check that there are available queues for the Job:
    if self.enableListMatch:
      availableCEs = []
      now = Time.dateTime()
      availableCEs = self.listMatchCache.get( pilotRequirements )
      if availableCEs == False:
        availableCEs = self._listMatch( proxy, jdl, taskQueueID, rb )
        if availableCEs != False:
          self.log.verbose( 'LastListMatch', now )
          self.log.verbose( 'AvailableCEs ', availableCEs )
          self.listMatchCache.add( pilotRequirements, self.listMatchDelay * 60,
                                   value = availableCEs )                      # it is given in minutes
      if not availableCEs:
        try:
          shutil.rmtree( workingDirectory )
        except:
          pass
        return S_ERROR( ERROR_CE + ' TQ: %d' % taskQueueID )

    # Now we are ready for the actual submission, so

    self.log.verbose( 'Submitting Pilots for TaskQueue', taskQueueID )
    submitRet = self._submitPilot( proxy, pilotsPerJob, jdl, taskQueueID, rb )
    try:
      shutil.rmtree( workingDirectory )
    except:
      pass
    if not submitRet:
      return S_ERROR( 'Pilot Submission Failed for TQ %d ' % taskQueueID )
    # pilotReference, resourceBroker = submitRet

    submittedPilots = 0

    if pilotsPerJob != 1 and len( submitRet ) != pilotsPerJob:
      # Parametric jobs are used
      for pilotReference, resourceBroker in submitRet:
        pilotReference = self._getChildrenReferences( proxy, pilotReference, taskQueueID )
        submittedPilots += len( pilotReference )
        pilotAgentsDB.addPilotTQReference( pilotReference, taskQueueID, ownerDN,
                      ownerGroup, resourceBroker, self.gridMiddleware,
                      pilotRequirements )
    else:
      for pilotReference, resourceBroker in submitRet:
        pilotReference = [pilotReference]
        submittedPilots += len( pilotReference )
        pilotAgentsDB.addPilotTQReference( pilotReference, taskQueueID, ownerDN,
                      ownerGroup, resourceBroker, self.gridMiddleware, pilotRequirements )

    # add some sleep here
    time.sleep( 0.1 * submittedPilots )

    if pilotsToSubmit > pilotsPerJob:
      # Additional submissions are necessary, need to get a new token and iterate.
      pilotsToSubmit -= pilotsPerJob
      result = gProxyManager.requestToken( ownerDN, ownerGroup, max( pilotsToSubmit, self.maxJobsInFillMode ) )
      if not result[ 'OK' ]:
        self.log.error( ERROR_TOKEN, result['Message'] )
        result = S_ERROR( ERROR_TOKEN )
        result['Value'] = submittedPilots
        return result
      ( token, numberOfUses ) = result[ 'Value' ]
      for option in pilotOptions:
        if option.find( '-o /Security/ProxyToken=' ) == 0:
          pilotOptions.remove( option )
      pilotOptions.append( '-o /Security/ProxyToken=%s' % token )
      pilotsPerJob = max( 1, min( pilotsPerJob, int( numberOfUses / self.maxJobsInFillMode ) ) )
      result = self._submitPilots( workDir, taskQueueDict, pilotOptions,
                                   pilotsToSubmit, ceMask,
                                   submitPrivatePilot, privateTQ,
                                   proxy, pilotsPerJob )
      if not result['OK']:
        if 'Value' not in result:
          result['Value'] = 0
        result['Value'] += submittedPilots
        return result
      submittedPilots += result['Value']

    return S_OK( submittedPilots )

  def _prepareJDL( self, taskQueueDict, workingDirectory, pilotOptions, pilotsToSubmit, ceMask, submitPrivatePilot, privateTQ ):
    """
      This method should be overridden in a subclass
    """
    self.log.error( '_prepareJDL() method should be implemented in a subclass' )
    sys.exit()

  def _JobJDL( self, taskQueueDict, pilotOptions, ceMask ):
    """
     The Job JDL is the same for LCG and GLite
    """
    pilotJDL = 'Executable     = "%s";\n' % os.path.basename( self.pilot )
    executable = self.pilot

    pilotJDL += 'Arguments     = "%s";\n' % ' '.join( pilotOptions )

    pilotJDL += 'CPUTimeRef    = %s;\n' % taskQueueDict['CPUTime']

    pilotJDL += 'CPUPowerRef   = %s;\n' % self.cpuPowerRef

    pilotJDL += """CPUWorkRef    = real( CPUTimeRef * CPUPowerRef );

Lookup        = "CPUScalingReferenceSI00=*";
cap = isList( other.GlueCECapability ) ? other.GlueCECapability : { "dummy" };
i0 = regexp( Lookup, cap[0] ) ? 0 : undefined;
i1 = isString( cap[1] ) && regexp( Lookup, cap[1] ) ? 1 : i0;
i2 = isString( cap[2] ) && regexp( Lookup, cap[2] ) ? 2 : i1;
i3 = isString( cap[3] ) && regexp( Lookup, cap[3] ) ? 3 : i2;
i4 = isString( cap[4] ) && regexp( Lookup, cap[4] ) ? 4 : i3;
i5 = isString( cap[5] ) && regexp( Lookup, cap[5] ) ? 5 : i4;
index = isString( cap[6] ) && regexp( Lookup, cap[6] ) ? 6 : i5;
i = isUndefined( index ) ? 0 : index;

QueuePowerRef = real( ! isUndefined( index ) ? int( substr( cap[i], size( Lookup ) - 1 ) ) : other.GlueHostBenchmarkSI00 );
QueueTimeRef  = real( other.GlueCEPolicyMaxCPUTime * 60 );
QueueWorkRef  = QueuePowerRef * QueueTimeRef;
"""

    requirements = list( self.requirements )
    if 'GridCEs' in taskQueueDict and taskQueueDict['GridCEs']:
      # if there an explicit Grig CE requested by the TQ, remove the Ranking requirement
      for req in self.requirements:
        if req.strip().lower()[:6] == 'rank >':
          requirements.remove( req )

    requirements.append( 'QueueWorkRef > CPUWorkRef' )

    siteRequirements = '\n || '.join( [ 'other.GlueCEInfoHostName == "%s"' % s for s in ceMask ] )
    requirements.append( "( %s\n )" % siteRequirements )

    pilotRequirements = '\n && '.join( requirements )

    pilotJDL += 'pilotRequirements  = %s;\n' % pilotRequirements

    pilotJDL += 'Rank          = %s;\n' % self.rank
    pilotJDL += 'FuzzyRank     = %s;\n' % self.fuzzyRank
    pilotJDL += 'StdOutput     = "%s";\n' % outputSandboxFiles[0]
    pilotJDL += 'StdError      = "%s";\n' % outputSandboxFiles[1]

    pilotJDL += 'InputSandbox  = { "%s" };\n' % '", "'.join( [ self.install, executable ] )

    pilotJDL += 'OutputSandbox = { %s };\n' % ', '.join( [ '"%s"' % f for f in outputSandboxFiles ] )

    self.log.verbose( pilotJDL )

    return ( pilotJDL, pilotRequirements )


  def parseListMatchStdout( self, proxy, cmd, taskQueueID, rb ):
    """
      Parse List Match stdout to return list of matched CE's
    """
    self.log.verbose( 'Executing List Match for TaskQueue', taskQueueID )

    start = time.time()
    ret = executeGridCommand( proxy, cmd, self.gridEnv )

    if not ret['OK']:
      self.log.error( 'Failed to execute List Match:', ret['Message'] )
      self.__sendErrorMail( rb, 'List Match', cmd, ret, proxy )
      return False
    if ret['Value'][0] != 0:
      self.log.error( 'Error executing List Match:', str( ret['Value'][0] ) + '\n'.join( ret['Value'][1:3] ) )
      self.__sendErrorMail( rb, 'List Match', cmd, ret, proxy )
      return False
    self.log.info( 'List Match Execution Time: %.2f for TaskQueue %d' % ( ( time.time() - start ), taskQueueID ) )

    stdout = ret['Value'][1]
    stderr = ret['Value'][2]
    availableCEs = []
    # Parse std.out
    for line in List.fromChar( stdout, '\n' ):
      if re.search( '/jobmanager-', line ) or re.search( '/cream-', line ):
        # TODO: the line has to be stripped from extra info
        availableCEs.append( line )

    if not availableCEs:
      self.log.info( 'List-Match failed to find CEs for TaskQueue', taskQueueID )
      self.log.info( stdout )
      self.log.info( stderr )
    else:
      self.log.debug( 'List-Match returns:', str( ret['Value'][0] ) + '\n'.join( ret['Value'][1:3] ) )
      self.log.info( 'List-Match found %s CEs for TaskQueue' % len( availableCEs ), taskQueueID )
      self.log.verbose( ', '.join( availableCEs ) )


    return availableCEs

  def parseJobSubmitStdout( self, proxy, cmd, taskQueueID, rb ):
    """
      Parse Job Submit stdout to return pilot reference
    """
    start = time.time()
    self.log.verbose( 'Executing Job Submit for TaskQueue', taskQueueID )

    ret = executeGridCommand( proxy, cmd, self.gridEnv )

    if not ret['OK']:
      self.log.error( 'Failed to execute Job Submit:', ret['Message'] )
      self.__sendErrorMail( rb, 'Job Submit', cmd, ret, proxy )
      return False
    if ret['Value'][0] != 0:
      self.log.error( 'Error executing Job Submit:', str( ret['Value'][0] ) + '\n'.join( ret['Value'][1:3] ) )
      self.__sendErrorMail( rb, 'Job Submit', cmd, ret, proxy )
      return False
    self.log.info( 'Job Submit Execution Time: %.2f for TaskQueue %d' % ( ( time.time() - start ), taskQueueID ) )

    stdout = ret['Value'][1]
    stderr = ret['Value'][2]

    submittedPilot = None

    failed = 1
    rb = ''
    for line in List.fromChar( stdout, '\n' ):
      m = re.search( "(https:\S+)", line )
      if ( m ):
        glite_id = m.group( 1 )
        submittedPilot = glite_id
        if not rb:
          m = re.search( "https://(.+):.+", glite_id )
          rb = m.group( 1 )
        failed = 0
    if failed:
      self.log.error( 'Job Submit returns no Reference:', str( ret['Value'][0] ) + '\n'.join( ret['Value'][1:3] ) )
      return False

    self.log.info( 'Reference %s for TaskQueue %s' % ( glite_id, taskQueueID ) )

    return glite_id, rb

  def _writeJDL( self, filename, jdlList ):
    try:
      f = open( filename, 'w' )
      f.write( '\n'.join( jdlList ) )
      f.close()
    except Exception, x:
      self.log.exception()
      return ''

    return filename

  def __sendErrorMail( self, rb, name, command, result, proxy ):
    """
     In case or error with RB/WM:
     - check if RB/WMS still in use
      - remove RB/WMS from current list
      - check if RB/WMS not in cache
        - add RB/WMS to cache
        - send Error mail

    """
    if rb in self.resourceBrokers:
      try:
        self.resourceBrokers.remove( rb )
        self.log.info( 'Removed RB from list', rb )
      except:
        pass
      if not self.__failingWMSCache.exists( rb ):
        self.__failingWMSCache.add( rb, self.errorClearTime ) # disable for 30 minutes
        mailAddress = self.errorMailAddress
        msg = ''
        if not result['OK']:
          subject = "%s: timeout executing %s" % ( rb, name )
          msg += '\n%s' % result['Message']
        elif result['Value'][0] != 0:
          if re.search( 'the server is temporarily drained', ' '.join( result['Value'][1:3] ) ):
            return
          if re.search( 'System load is too high:', ' '.join( result['Value'][1:3] ) ):
            return
          subject = "%s: error executing %s" % ( rb, name )
        else:
          return
        msg += ' '.join( command )
        msg += '\nreturns: %s\n' % str( result['Value'][0] ) + '\n'.join( result['Value'][1:3] )
        msg += '\nUsing Proxy:\n' + getProxyInfoAsString( proxy )['Value']

        #msg += '\nUsing Proxy:\n' + gProxyManager.

        ticketTime = self.errorClearTime + self.errorTicketTime

        if self.__ticketsWMSCache.exists( rb ):
          mailAddress = self.alarmMailAddress
          # the RB was already detected failing a short time ago
          msg = 'Submit GGUS Ticket for this error if not already opened\n' + \
                       'It has been failing at least for %s hours\n' % ( ticketTime / 60 / 60 ) + msg
        else:
          self.__ticketsWMSCache.add( rb, ticketTime )

        if mailAddress:
          result = NotificationClient().sendMail( mailAddress, subject, msg, fromAddress = self.mailFromAddress )
          if not result[ 'OK' ]:
            self.log.error( "Mail could not be sent" )

    return


  def _listMatch( self, proxy, jdl, taskQueueID, rb ):
    return S_ERROR( "overload me!!" )

  def _submitPilot( self, proxy, pilotsToSubmit, jdl, taskQueueID, rb ):
    return S_ERROR( "overload me!!" )

  def _getChildrenReferences( self, proxy, parentReference, taskQueueID ):
    return S_ERROR( "overload me!!" )
