########################################################################
# $HeadURL$
# File :    SiteDirector.py
# Author :  A.T.
########################################################################

"""  The Site Director is a simple agent performing pilot job submission to particular sites.
"""

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers              import getCSExtensions, getVO
from DIRAC.Resources.Computing.ComputingElementFactory     import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client.ServerUtils     import pilotAgentsDB, taskQueueDB, jobDB
from DIRAC                                                 import S_OK, S_ERROR, gConfig
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.AccountingSystem.Client.Types.Pilot             import Pilot as PilotAccounting
from DIRAC.AccountingSystem.Client.DataStoreClient         import gDataStoreClient
from DIRAC.Core.Security                                   import CS
from DIRAC.Core.Utilities.SiteCEMapping                    import getSiteForCE
import os, base64, bz2, tempfile, random, socket
import DIRAC

__RCSID__ = "$Id$"

DIRAC_PILOT = os.path.join( DIRAC.rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'dirac-pilot.py' )
DIRAC_INSTALL = os.path.join( DIRAC.rootPath, 'DIRAC', 'Core', 'scripts', 'dirac-install.py' )
TRANSIENT_PILOT_STATUS = ['Submitted', 'Waiting', 'Running', 'Scheduled', 'Ready']
FINAL_PILOT_STATUS = ['Aborted', 'Failed', 'Done']
ERROR_TOKEN = 'Invalid proxy token request'

class SiteDirector( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

  def initialize( self ):
    """ Standard constructor
    """
    self.am_setOption( "PollingTime", 60.0 )
    self.am_setOption( "maxPilotWaitingHours", 6 )

    # Get the site description dictionary
    siteNames = self.am_getOption( 'Site', [] )
    if not siteNames:
      siteName = gConfig.getValue( '/DIRAC/Site', 'Unknown' )
      if siteName == 'Unknown':
        return S_ERROR( 'Unknown site' )
      else:
        siteNames = [siteName]

    self.siteNames = siteNames
    self.gridEnv = self.am_getOption( "GridEnv", '' )

    self.genericPilotDN = self.am_getOption( 'GenericPilotDN', 'Unknown' )
    self.genericPilotGroup = self.am_getOption( 'GenericPilotGroup', 'Unknown' )
    self.pilot = DIRAC_PILOT
    self.install = DIRAC_INSTALL
    self.workingDirectory = self.am_getOption( 'WorkDirectory' )
    self.maxQueueLength = self.am_getOption( 'MaxQueueLength', 86400*3 )

    # Flags
    self.updateStatus = self.am_getOption( 'UpdatePilotStatus', True )
    self.getOutput = self.am_getOption( 'GetPilotOutput', True )
    self.sendAccounting = self.am_getOption( 'SendPilotAccounting', True )
    if self.updateStatus:
      self.log.always( 'Pilot status update requested' )
    if self.getOutput:
      self.log.always( 'Pilot output retrieval requested' )
    if self.sendAccounting:
      self.log.always( 'Pilot accounting sending requested' )

    self.log.always( 'Site:', self.siteNames )
    ceTypes = self.am_getOption( 'CETypes', [] )
    if ceTypes:
      self.log.always( 'CETypes:', ceTypes )
    ces = self.am_getOption( 'CEs', [] )
    if ceTypes:
      self.log.always( 'CEs:', ces )
    self.log.always( 'GenericPilotDN:', self.genericPilotDN )
    self.log.always( 'GenericPilotGroup:', self.genericPilotGroup )

    self.localhost = socket.getfqdn()
    self.proxy = ''
    self.queueDict = {}
    result = self.getQueues()
    if not result['OK']:
      return result

    if self.queueDict:
      self.log.always( "Agent will serve queues:" )
      for queue in self.queueDict:
        self.log.always( "Site: %s, CE: %s, Queue: %s" % ( self.queueDict[queue]['Site'],
                                                         self.queueDict[queue]['CEName'],
                                                         queue ) )

    return S_OK()

  def getQueues( self ):
    """ Get the list of relevant CEs and their descriptions
    """

    ceFactory = ComputingElementFactory()
    ceTypes = self.am_getOption( 'CETypes', [] )
    ceConfList = self.am_getOption( 'CEs', [] )

    for siteName in self.siteNames:
      # Look up CE definitions in the site CS description
      ceList = []
      gridType = siteName.split( '.' )[0]
      result = gConfig.getSections( '/Resources/Sites/%s/%s/CEs' % ( gridType, siteName ) )
      if not result['OK']:
        return S_ERROR( 'Failed to look up the CS for the site %s CEs' % siteName )
      if not result['Value']:
        return S_ERROR( 'No CEs found for site %s' % siteName )
      ceTotalList = result['Value']
      for ce in ceTotalList:
        if ( ceConfList and ce in ceConfList ) or not ceConfList:
          ceType = gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/CEType' % ( gridType, siteName, ce ), 'Unknown' )
          result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/CEs/%s' % ( gridType, siteName, ce ) )
          if not result['OK']:
            return S_ERROR( 'Failed to look up the CS for ce %s' % ce )
          ceDict = result['Value']
          if "SubmissionMode" in ceDict and ceDict['SubmissionMode'].lower() == "direct":
            if ceType in ceTypes:
              ceList.append( ( ce, ceType, ceDict ) )

      for ce, ceType, ceDict in ceList:
        section = '/Resources/Sites/%s/%s/CEs/%s/Queues' % ( gridType, siteName, ce )
        result = gConfig.getSections( section )
        if not result['OK']:
          return S_ERROR( 'Failed to look up the CS for queues' )
        if not result['Value']:
          return S_ERROR( 'No Queues found for site %s, ce %s' % ( siteName, ce ) )

        queues = result['Value']
        for queue in queues:
          result = gConfig.getOptionsDict( '%s/%s' % ( section, queue ) )
          if not result['OK']:
            return S_ERROR( 'Failed to look up the CS for ce,queue %s,%s' % ( ce, queue ) )

          queueName = '%s_%s' % ( ce, queue )
          self.queueDict[queueName] = {}
          self.queueDict[queueName]['ParametersDict'] = result['Value']
          self.queueDict[queueName]['ParametersDict']['Queue'] = queue
          self.queueDict[queueName]['ParametersDict']['Site'] = siteName
          self.queueDict[queueName]['ParametersDict']['GridEnv'] = self.gridEnv
          self.queueDict[queueName]['ParametersDict']['Setup'] = gConfig.getValue( '/DIRAC/Setup', 'unknown' )
          # Evaluate the CPU limit of the queue according to the Glue convention
          # To Do: should be a utility
          if "maxCPUTime" in self.queueDict[queueName]['ParametersDict'] and \
             "SI00" in self.queueDict[queueName]['ParametersDict']:
            maxCPUTime = float( self.queueDict[queueName]['ParametersDict']['maxCPUTime'] )
            # For some sites there are crazy values in the CS
            maxCPUTime = max( maxCPUTime, 0 )
            maxCPUTime = min( maxCPUTime, 86400 * 12.5 )
            si00 = float( self.queueDict[queueName]['ParametersDict']['SI00'] )
            queueCPUTime = 60. / 250. * maxCPUTime * si00
            self.queueDict[queueName]['ParametersDict']['CPUTime'] = int( queueCPUTime )
          qwDir = os.path.join( self.workingDirectory, queue )
          if not os.path.exists( qwDir ):
            os.mkdir( qwDir )
          self.queueDict[queueName]['ParametersDict']['WorkingDirectory'] = qwDir
          queueDict = dict( ceDict )
          queueDict.update( self.queueDict[queueName]['ParametersDict'] )
          result = ceFactory.getCE( ceName = ce,
                                   ceType = ceType,
                                   ceParametersDict = queueDict )
          if not result['OK']:
            return result
          self.queueDict[queueName]['CE'] = result['Value']
          self.queueDict[queueName]['CEName'] = ce
          self.queueDict[queueName]['CEType'] = ceType
          self.queueDict[queueName]['Site'] = siteName
          self.queueDict[queueName]['QueueName'] = queue
          result = self.queueDict[queueName]['CE'].isValid()
          if not result['OK']:
            self.log.fatal( result['Message'] )
            return result
          if 'BundleProxy' in self.queueDict[queueName]['ParametersDict']:
            self.queueDict[queueName]['BundleProxy'] = True

    return S_OK()

  def execute( self ):
    """ Main execution method
    """
    result = self.submitJobs()
    if not result['OK']:
      self.log.error( 'Errors in the job submission: %s' % result['Message'] )


    if self.updateStatus:
      result = self.updatePilotStatus()
      if not result['OK']:
        self.log.error( 'Errors in updating pilot status: %s' % result['Message'] )

    return S_OK()

  def submitJobs( self ):
    """ Go through defined computing elements and submit jobs if necessary
    """

    # Check if the site is allowed in the mask
    result = jobDB.getSiteMask()
    if not result['OK']:
      return S_ERROR( 'Can not get the site mask' )
    siteMaskList = result['Value']

    for queue in self.queueDict:
      ce = self.queueDict[queue]['CE']
      ceName = self.queueDict[queue]['CEName']
      ceType = self.queueDict[queue]['CEType']
      queueName = self.queueDict[queue]['QueueName']
      siteName = self.queueDict[queue]['Site']
      siteMask = siteName in siteMaskList

      if 'CPUTime' in self.queueDict[queue]['ParametersDict'] :
        queueCPUTime = int( self.queueDict[queue]['ParametersDict']['CPUTime'] )
      else:
        return S_ERROR( 'CPU time limit is not specified for queue %s' % queue )
      if queueCPUTime > self.maxQueueLength:
        queueCPUTime = self.maxQueueLength

      # Get the working proxy
      cpuTime = queueCPUTime + 86400
      result = gProxyManager.getPilotProxyFromDIRACGroup( self.genericPilotDN, self.genericPilotGroup, cpuTime )
      if not result['OK']:
        return result
      self.proxy = result['Value']
      ce.setProxy( self.proxy, cpuTime - 60 )

      result = ce.available()
      if not result['OK']:
        self.log.warn( 'Failed to check the availability of queue %s: %s' % ( queue, result['Message'] ) )
        continue

      totalSlots = result['Value']

      self.log.verbose( result['Message'] )

      ceDict = ce.getParameterDict()
      ceDict[ 'GridCE' ] = ceName
      if not siteMask and 'Site' in ceDict:
        self.log.info( 'Site not in the mask %s' % siteName )
        self.log.info( 'Removing "Site" from matching Dict' )
        del ceDict[ 'Site' ]

      result = taskQueueDB.getMatchingTaskQueues( ceDict )

      if not result['OK']:
        self.log.error( 'Could not retrieve TaskQueues from TaskQueueDB', result['Message'] )
        return result
      taskQueueDict = result['Value']
      if not taskQueueDict:
        self.log.verbose( 'No matching TQs found' )
        continue

      totalTQJobs = 0
      for tq in taskQueueDict:
        totalTQJobs += taskQueueDict[tq]['Jobs']

      pilotsToSubmit = min( totalSlots, totalTQJobs )
      self.log.verbose( 'Available slots=%d, TQ jobs=%d, Pilots to submit=%d' % ( totalSlots, totalTQJobs, pilotsToSubmit ) )

      if pilotsToSubmit > 0:
        self.log.info( 'Going to submit %d pilots to %s queue' % ( pilotsToSubmit, queue ) )

        bundleProxy = self.queueDict[queue].get( 'BundleProxy', False )
        result = self.__getExecutable( queue, pilotsToSubmit, bundleProxy )
        if not result['OK']:
          return result

        executable = result['Value']
        result = ce.submitJob( executable, '', pilotsToSubmit )
        if not result['OK']:
          self.log.error( 'Failed submission to queue %s:' % queue, result['Message'] )
          continue
        # Add pilots to the PilotAgentsDB assign pilots to TaskQueue proportionally to the
        # task queue priorities
        pilotList = result['Value']
        stampDict = {}
        if result.has_key( 'PilotStampDict' ):
          stampDict = result['PilotStampDict']
        tqPriorityList = []
        sumPriority = 0.
        for tq in taskQueueDict:
          sumPriority += taskQueueDict[tq]['Priority']
          tqPriorityList.append( ( tq, sumPriority ) )
        rndm = random.random()*sumPriority
        tqDict = {}
        for pilotID in pilotList:
          rndm = random.random()*sumPriority
          for tq, prio in tqPriorityList:
            if rndm < prio:
              tqID = tq
              break
          if not tqDict.has_key( tqID ):
            tqDict[tqID] = []
          tqDict[tqID].append( pilotID )

        for tqID, pilotList in tqDict.items():
          result = pilotAgentsDB.addPilotTQReference( pilotList,
                                                     tqID,
                                                     self.genericPilotDN,
                                                     self.genericPilotGroup,
                                                     self.localhost,
                                                     ceType,
                                                     '',
                                                     stampDict )
          if not result['OK']:
            self.log.error( 'Failed add pilots to the PilotAgentsDB: %s' % result['Message'] )
            continue
          for pilot in pilotList:
            result = pilotAgentsDB.setPilotStatus( pilot, 'Submitted', ceName,
                                                  'Successfuly submitted by the SiteDirector',
                                                  siteName, queueName )
            if not result['OK']:
              self.log.error( 'Failed to set pilot status: %s' % result['Message'] )
              continue

    return S_OK()

#####################################################################################
  def __getExecutable( self, queue, pilotsToSubmit, bundleProxy = True ):
    """ Prepare the full executable for queue
    """

    proxy = ''
    if bundleProxy:
      proxy = self.proxy
    pilotOptions = self.__getPilotOptions( queue, pilotsToSubmit )
    if pilotOptions is None:
      return S_ERROR( 'Errors in compiling pilot options' )
    executable = self.__writePilotScript( self.workingDirectory, pilotOptions, proxy )
    result = S_OK( executable )
    return result

#####################################################################################    
  def __getPilotOptions( self, queue, pilotsToSubmit ):
    """ Prepare pilot options
    """

    queueDict = self.queueDict[queue]['ParametersDict']

    vo = getVO()
    if not vo:
      self.log.error( 'Virtual Organization is not defined in the configuration' )
      return None
    pilotOptions = [ "-V '%s'" % vo ]
    setup = gConfig.getValue( "/DIRAC/Setup", "unknown" )
    if setup == 'unknown':
      self.log.error( 'Setup is not defined in the configuration' )
      return None
    pilotOptions.append( '-S %s' % setup )
    diracVersion = gConfig.getValue( "/Operations/%s/%s/Versions/PilotVersion" % ( vo, setup ), "unknown" )
    if diracVersion == 'unknown':
      self.log.error( 'PilotVersion is not defined in the configuration' )
      return None
    pilotOptions.append( '-r %s' % diracVersion )
    projectName = gConfig.getValue( "/Operations/%s/%s/Versions/PilotInstallation" % ( vo, setup ), "" )
    if projectName == '':
      self.log.info( 'DIRAC installation will be installed by pilots' )
    else:
      pilotOptions.append( '-l %s' % projectName )

    ownerDN = self.genericPilotDN
    ownerGroup = self.genericPilotGroup
    result = gProxyManager.requestToken( ownerDN, ownerGroup, pilotsToSubmit * 5 )
    if not result[ 'OK' ]:
      self.log.error( ERROR_TOKEN, result['Message'] )
      return S_ERROR( ERROR_TOKEN )
    ( token, numberOfUses ) = result[ 'Value' ]
    pilotOptions.append( '-o /Security/ProxyToken=%s' % token )
    # Use Filling mode
    pilotOptions.append( '-M %s' % 5 )

    # Debug
    pilotOptions.append( '-d' )
    # CS Servers
    csServers = gConfig.getValue( "/DIRAC/Configuration/Servers", [] )
    pilotOptions.append( '-C %s' % ",".join( csServers ) )
    # DIRAC Extensions
    extensionsList = getCSExtensions()
    if extensionsList:
      pilotOptions.append( '-e %s' % ",".join( extensionsList ) )
    # Requested CPU time
    pilotOptions.append( '-T %s' % queueDict['CPUTime'] )
    # SiteName
    pilotOptions.append( '-n %s' % queueDict['Site'] )
    if 'ClientPlatform' in queueDict:
      pilotOptions.append( "-p '%s'" % queueDict['ClientPlatform'] )

    if 'SharedArea' in queueDict:
      pilotOptions.append( "-o '/LocalSite/SharedArea=%s'" % queueDict['SharedArea'] )

    if 'SI00' in queueDict:
      factor = float( queueDict['SI00'] ) / 250.
      pilotOptions.append( "-o '/LocalSite/CPUScalingFactor=%s'" % factor )
      pilotOptions.append( "-o '/LocalSite/CPUNormalizationFactor=%s'" % factor )
    else:
      if 'CPUScalingFactor' in queueDict:
        pilotOptions.append( "-o '/LocalSite/CPUScalingFactor=%s'" % queueDict['CPUScalingFactor'] )
      if 'CPUNormalizationFactor' in queueDict:
        pilotOptions.append( "-o '/LocalSite/CPUNormalizationFactor=%s'" % queueDict['CPUNormalizationFactor'] )

    self.log.verbose( "pilotOptions: ", ' '.join( pilotOptions ) )

    return pilotOptions

#####################################################################################    
  def __writePilotScript( self, workingDirectory, pilotOptions, proxy = '', httpProxy = '' ):
    """ Bundle together and write out the pilot executable script, admixt the proxy if given
    """

    try:
      compressedAndEncodedProxy = ''
      proxyFlag = 'False'
      if proxy:
        compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) ).replace( '\n', '' )
        proxyFlag = 'True'
      compressedAndEncodedPilot = base64.encodestring( bz2.compress( open( self.pilot, "rb" ).read(), 9 ) ).replace( '\n', '' )
      compressedAndEncodedInstall = base64.encodestring( bz2.compress( open( self.install, "rb" ).read(), 9 ) ).replace( '\n', '' )
    except:
      self.log.exception( 'Exception during file compression of proxy, dirac-pilot or dirac-install' )
      return S_ERROR( 'Exception during file compression of proxy, dirac-pilot or dirac-install' )

    localPilot = """#!/bin/bash
/usr/bin/env python << EOF
#
import os, tempfile, sys, shutil, base64, bz2
try:
  pilotWorkingDirectory = tempfile.mkdtemp( suffix = 'pilot', prefix= 'DIRAC_' )
  os.chdir( pilotWorkingDirectory )
  if %(proxyFlag)s:
    open( 'proxy', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedProxy)s" ) ) )
    os.chmod("proxy",0600)
    os.environ["X509_USER_PROXY"]=os.path.join(pilotWorkingDirectory, 'proxy')
  open( '%(pilotScript)s', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedPilot)s" ) ) )
  open( '%(installScript)s', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedInstall)s" ) ) )
  os.chmod("%(pilotScript)s",0700)
  os.chmod("%(installScript)s",0700)
  if "LD_LIBRARY_PATH" not in os.environ:
    os.environ["LD_LIBRARY_PATH"]=""
  if "%(httpProxy)s":
    os.environ["HTTP_PROXY"]="%(httpProxy)s"
  os.environ["X509_CERT_DIR"]=os.path.join(pilotWorkingDirectory, 'etc/grid-security/certificates')
  # TODO: structure the output
  print '==========================================================='
  print 'Environment of execution host'
  for key in os.environ.keys():
    print key + '=' + os.environ[key]
  print '==========================================================='
except Exception, x:
  print >> sys.stderr, x
  sys.exit(-1)
cmd = "python %(pilotScript)s %(pilotOptions)s"
print 'Executing: ', cmd
sys.stdout.flush()
os.system( cmd )

shutil.rmtree( pilotWorkingDirectory )

EOF
""" % { 'compressedAndEncodedProxy': compressedAndEncodedProxy, \
        'compressedAndEncodedPilot': compressedAndEncodedPilot, \
        'compressedAndEncodedInstall': compressedAndEncodedInstall, \
        'httpProxy': httpProxy, \
        'pilotScript': os.path.basename( self.pilot ), \
        'installScript': os.path.basename( self.install ),
        'pilotOptions': ' '.join( pilotOptions ),
        'proxyFlag': proxyFlag }

    fd, name = tempfile.mkstemp( suffix = '_pilotwrapper.py', prefix = 'DIRAC_', dir = workingDirectory )
    pilotWrapper = os.fdopen( fd, 'w' )
    pilotWrapper.write( localPilot )
    pilotWrapper.close()
    return name

  def updatePilotStatus( self ):
    """ Update status of pilots in transient states
    """
    for queue in self.queueDict:
      ce = self.queueDict[queue]['CE']
      ceName = self.queueDict[queue]['CEName']
      queueName = self.queueDict[queue]['QueueName']
      ceType = self.queueDict[queue]['CEType']
      siteName = self.queueDict[queue]['Site']

      result = pilotAgentsDB.selectPilots( {'DestinationSite':ceName,
                                           'Queue':queueName,
                                           'GridType':ceType,
                                           'GridSite':siteName,
                                           'Status':TRANSIENT_PILOT_STATUS} )
      if not result['OK']:
        self.log.error( 'Failed to select pilots: %s' % result['Message'] )
        continue
      pilotRefs = result['Value']
      if not pilotRefs:
        continue

      #print "AT >>> pilotRefs", pilotRefs

      result = pilotAgentsDB.getPilotInfo( pilotRefs )
      if not result['OK']:
        self.log.error( 'Failed to get pilots info: %s' % result['Message'] )
        continue
      pilotDict = result['Value']

      #print "AT >>> pilotDict", pilotDict

      result = ce.getJobStatus( pilotRefs )
      if not result['OK']:
        self.log.error( 'Failed to get pilots status from CE: %s' % result['Message'] )
        continue
      pilotCEDict = result['Value']

      #print "AT >>> pilotCEDict", pilotCEDict

      for pRef in pilotRefs:
        newStatus = ''
        oldStatus = pilotDict[pRef]['Status']
        ceStatus = pilotCEDict[pRef]
        if oldStatus == ceStatus:
          # Status did not change, continue
          continue
        elif ceStatus == "Unknown" and not oldStatus in FINAL_PILOT_STATUS:
          # Pilot finished without reporting, consider it Aborted
          newStatus = 'Aborted'
        elif ceStatus != 'Unknown' :
          # Update the pilot status to the new value
          newStatus = ceStatus

        if newStatus:
          self.log.info( 'Updating status to %s for pilot %s' % ( newStatus, pRef ) )
          result = pilotAgentsDB.setPilotStatus( pRef, newStatus, '', 'Updated by SiteDirector' )
        # Retrieve the pilot output now 
        if newStatus in FINAL_PILOT_STATUS:
          if pilotDict[pRef]['OutputReady'].lower() == 'false' and self.getOutput:
            self.log.info( 'Retrieving output for pilot %s' % pRef )
            pilotStamp = pilotDict[pRef]['PilotStamp']
            pRefStamp = pRef
            if pilotStamp:
              pRefStamp = pRef + ':::' + pilotStamp
            result = ce.getJobOutput( pRefStamp )
            if not result['OK']:
              self.log.error( 'Failed to get pilot output: %s' % result['Message'] )
            else:
              output, error = result['Value']
              result = pilotAgentsDB.storePilotOutput( pRef, output, error )
              if not result['OK']:
                self.log.error( 'Failed to store pilot output: %s' % result['Message'] )

    # The pilot can be in Done state set by the job agent check if the output is retrieved
    for queue in self.queueDict:
      ce = self.queueDict[queue]['CE']

      if not ce.isProxyValid( 120 ):
        result = gProxyManager.getPilotProxyFromDIRACGroup( self.genericPilotDN, self.genericPilotGroup, 1000 )
        if not result['OK']:
          return result
        ce.setProxy( self.proxy, 940 )

      ceName = self.queueDict[queue]['CEName']
      queueName = self.queueDict[queue]['QueueName']
      ceType = self.queueDict[queue]['CEType']
      siteName = self.queueDict[queue]['Site']
      result = pilotAgentsDB.selectPilots( {'DestinationSite':ceName,
                                           'Queue':queueName,
                                           'GridType':ceType,
                                           'GridSite':siteName,
                                           'OutputReady':'False',
                                           'Status':FINAL_PILOT_STATUS} )

      if not result['OK']:
        self.log.error( 'Failed to select pilots: %s' % result['Message'] )
        continue
      pilotRefs = result['Value']
      if not pilotRefs:
        continue
      result = pilotAgentsDB.getPilotInfo( pilotRefs )
      if not result['OK']:
        self.log.error( 'Failed to get pilots info: %s' % result['Message'] )
        continue
      pilotDict = result['Value']
      for pRef in pilotRefs:
        self.log.info( 'Retrieving output for pilot %s' % pRef )
        pilotStamp = pilotDict[pRef]['PilotStamp']
        pRefStamp = pRef
        if pilotStamp:
          pRefStamp = pRef + ':::' + pilotStamp
        result = ce.getJobOutput( pRefStamp )
        if not result['OK']:
          self.log.error( 'Failed to get pilot output: %s' % result['Message'] )
        else:
          output, error = result['Value']
          result = pilotAgentsDB.storePilotOutput( pRef, output, error )
          if not result['OK']:
            self.log.error( 'Failed to store pilot output: %s' % result['Message'] )

      # Check if the accounting is to be sent
      if self.sendAccounting:
        result = pilotAgentsDB.selectPilots( {'DestinationSite':ceName,
                                             'Queue':queueName,
                                             'GridType':ceType,
                                             'GridSite':siteName,
                                             'AccountingSent':'False',
                                             'Status':FINAL_PILOT_STATUS} )

        if not result['OK']:
          self.log.error( 'Failed to select pilots: %s' % result['Message'] )
          continue
        pilotRefs = result['Value']
        if not pilotRefs:
          continue
        result = pilotAgentsDB.getPilotInfo( pilotRefs )
        if not result['OK']:
          self.log.error( 'Failed to get pilots info: %s' % result['Message'] )
          continue
        pilotDict = result['Value']
        result = self.sendPilotAccounting( pilotDict )
        if not result['OK']:
          self.log.error( 'Failed to send pilot agent accounting' )

    return S_OK()

  def sendPilotAccounting( self, pilotDict ):
    """ Send pilot accounting record
    """
    for pRef in pilotDict:
      self.log.verbose( 'Preparing accounting record for pilot %s' % pRef )
      pA = PilotAccounting()
      pA.setEndTime( pilotDict[pRef][ 'LastUpdateTime' ] )
      pA.setStartTime( pilotDict[pRef][ 'SubmissionTime' ] )
      retVal = CS.getUsernameForDN( pilotDict[pRef][ 'OwnerDN' ] )
      if not retVal[ 'OK' ]:
        userName = 'unknown'
        self.log.error( "Can't determine username for dn:", pilotDict[pRef][ 'OwnerDN' ] )
      else:
        userName = retVal[ 'Value' ]
      pA.setValueByKey( 'User', userName )
      pA.setValueByKey( 'UserGroup', pilotDict[pRef][ 'OwnerGroup' ] )
      result = getSiteForCE( pilotDict[pRef][ 'DestinationSite' ] )
      if result['OK'] and result[ 'Value' ].strip():
        pA.setValueByKey( 'Site', result['Value'].strip() )
      else:
        pA.setValueByKey( 'Site', 'Unknown' )
      pA.setValueByKey( 'GridCE', pilotDict[pRef][ 'DestinationSite' ] )
      pA.setValueByKey( 'GridMiddleware', pilotDict[pRef][ 'GridType' ] )
      pA.setValueByKey( 'GridResourceBroker', pilotDict[pRef][ 'Broker' ] )
      pA.setValueByKey( 'GridStatus', pilotDict[pRef][ 'Status' ] )
      if not 'Jobs' in pilotDict[pRef]:
        pA.setValueByKey( 'Jobs', 0 )
      else:
        pA.setValueByKey( 'Jobs', len( pilotDict[pRef]['Jobs'] ) )
      self.log.info( "Adding accounting record for pilot %s" % pilotDict[pRef][ 'PilotID' ] )
      retVal = gDataStoreClient.addRegister( pA )
      if not retVal[ 'OK' ]:
        self.log.error( 'Failed to send accounting info for pilot %s' % pRef )
      else:
        # Set up AccountingSent flag
        result = pilotAgentsDB.setAccountingFlag( pRef )
        if not result['OK']:
          self.log.error( 'Failed to set accounting flag for pilot %s' % pRef )

    self.log.info( 'Committing accounting records for %d pilots' % len( pilotDict ) )
    result = gDataStoreClient.commit()
    if result['OK']:
      for pRef in pilotDict:
        self.log.verbose( 'Setting AccountingSent flag for pilot %s' % pRef )
        result = pilotAgentsDB.setAccountingFlag( pRef )
        if not result['OK']:
          self.log.error( 'Failed to set accounting flag for pilot %s' % pRef )
    else:
      return result

    return S_OK()


