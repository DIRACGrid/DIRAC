########################################################################
# $HeadURL$
# File :   DIRACPilotDirector.py
# Author : Ricardo Graciani
########################################################################
"""
  Dirac PilotDirector class, it uses DIRAC CE backends to submit and monitor pilots.
  It includes:
   - basic configuration for Dirac PilotDirector

  A DIRAC PilotDirector make use directly to CE methods to place the pilots on the
  underlying resources.


"""
__RCSID__ = "$Id$"

import os, sys, tempfile, shutil, time, base64, bz2

from DIRAC.WorkloadManagementSystem.private.PilotDirector import PilotDirector
from DIRAC.Resources.Computing.ComputingElementFactory    import ComputingElementFactory
from DIRAC.Resources.Computing.ComputingElement import getResourceDict
from DIRAC.Core.Security import CS
from DIRAC.FrameworkSystem.Client.ProxyManagerClient      import gProxyManager
from DIRAC import S_OK, S_ERROR, gConfig, rootPath
from DIRAC.Core.Utilities.DictCache import DictCache

ERROR_CE         = 'No CE available'
ERROR_JDL        = 'Could not create Pilot script'
ERROR_SCRIPT     = 'Could not copy Pilot script'

#COMPUTING_ELEMENTS = ['InProcess']
COMPUTING_ELEMENTS = []
WAITING_TO_RUNNING_RATIO = 0.5
MAX_WAITING_JOBS = 50
MAX_NUMBER_JOBS = 10000

class DIRACPilotDirector(PilotDirector):
  """
    DIRAC PilotDirector class
  """
  def __init__( self, submitPool ):
    """
     Define some defaults and call parent __init__
    """
    self.gridMiddleware    = 'DIRAC'

    PilotDirector.__init__( self, submitPool )

    self.computingElementList = COMPUTING_ELEMENTS
    self.computingElementDict = {}
    self.addComputingElement( self.computingElementList )

    self.siteName          = gConfig.getValue('/LocalSite/Site','')
    if not self.siteName:
      self.log.error( 'Can not run a Director if Site Name is not defined' )
      sys.exit()

    self.__failingCECache  = DictCache()
    self.__ticketsCECache  = DictCache()

  def configure(self, csSection, submitPool ):
    """
     Here goes common configuration for DIRAC PilotDirector
    """

    PilotDirector.configure( self, csSection, submitPool )
    self.reloadConfiguration( csSection, submitPool )

    self.__failingCECache.purgeExpired()
    self.__ticketsCECache.purgeExpired()

    for ce in self.__failingCECache.getKeys():
      if ce in self.computingElementDict.keys():
        try:
          del self.computingElementDict[ce]
        except:
          pass
    if self.computingElementDict:
      self.log.info( ' ComputingElements:', ', '.join(self.computingElementDict.keys()) )
    else:
      return

    # FIXME: this is to start testing
    ceName, computingElementDict = self.computingElementDict.items()[0]

    self.computingElement = computingElementDict['CE']

    self.log.debug( self.computingElement.getCEStatus() )

    self.log.info( ' SiteName:', self.siteName )


  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    PilotDirector.configureFromSection( self, mySection )

    self.computingElementList = gConfig.getValue( mySection+'/ComputingElements'      , self.computingElementList )
    self.addComputingElement( self.computingElementList )

    self.siteName             = gConfig.getValue( mySection+'/SiteName'               , self.siteName )


  def addComputingElement(self, ceList):
    """
      Check if a CE object for the current CE is available,
      instantiate one if necessary
    """
    for CE in ceList:
      if CE not in self.computingElementDict:
        ceFactory = ComputingElementFactory( )
        ceInstance = ceFactory.getCE( ceName = CE )
        if not ceInstance['OK']:
          self.log.error('Can not create CE object:', ceInstance['Message'])
          return
        self.computingElementDict[CE] = ceInstance['Value'].ceConfigDict
        # add the 'CE' instance at the end to avoid being overwritten
        self.computingElementDict[CE]['CE'] = ceInstance['Value']


  def _submitPilots( self, workDir, taskQueueDict, pilotOptions, pilotsToSubmit,
                     ceMask, submitPrivatePilot, privateTQ, proxy, pilotsPerJob ):
    """
      This method does the actual pilot submission to the DIRAC CE
      The logic is as follows:
      - If there are no available CE it return error
      - If there is no queue available in the CE's, it returns error
      - It creates a temp directory
      - It prepare a PilotScript
    """

    taskQueueID = taskQueueDict['TaskQueueID']
    ownerDN = taskQueueDict['OwnerDN']

    submittedPilots = 0

    # if self.computingElement not in self.computingElementDict:
    #  # Since we can exclude CEs from the list, it may become empty
    #  return S_ERROR( ERROR_CE )

    pilotRequirements = []
    pilotRequirements.append( ( 'CPUTime', taskQueueDict['CPUTime'] ) )
    # do we need to care about anything else?
    pilotRequirementsString = str( pilotRequirements )

    # Check that there are available queues for the Jobs:
    if self.enableListMatch:
      availableQueues = []
      # now = Time.dateTime()
      cachedAvailableQueues = self.listMatchCache.get( pilotRequirementsString )
      if cachedAvailableQueues == False:
        availableQueues = self._listQueues( pilotRequirements )
        if availableQueues != False:
          self.listMatchCache.add( pilotRequirementsString, self.listMatchDelay, availableQueues )
          self.log.verbose( 'Available Queues for TaskQueue ',  "%s: %s" % ( taskQueueID, str(availableQueues) ) )
      else:
        availableQueues = cachedAvailableQueues

    if not availableQueues:
      return S_ERROR( ERROR_CE + ' TQ: %d' % taskQueueID )

    baseDir = os.getcwd()
    workingDirectory = tempfile.mkdtemp( prefix= 'TQ_%s_' % taskQueueID, dir = workDir )
    self.log.verbose( 'Using working Directory:', workingDirectory )
    os.chdir( workingDirectory )

    # set the Site Name
    pilotOptions.append( "-n '%s'" % self.siteName)

    # submit pilots for every CE available

    for CE in self.computingElementDict.keys():
      ceName = CE
      computingElement = self.computingElementDict[CE]['CE']

      # add possible requirements from Site and CE
      for req, val in getResourceDict( ceName ).items():
        pilotOptions.append( "-o '/AgentJobRequirements/%s=%s'" % ( req, val ) )

      ceConfigDict = self.computingElementDict[CE]

      if 'ClientPlatform' in ceConfigDict:
        pilotOptions.append( "-p '%s'" % ceConfigDict['ClientPlatform'])

      if 'SharedArea' in ceConfigDict:
        pilotOptions.append( "-o '/LocalSite/SharedArea=%s'" % ceConfigDict['SharedArea'] )

      if 'CPUScalingFactor' in ceConfigDict:
        pilotOptions.append( "-o '/LocalSite/CPUScalingFactor=%s'" % ceConfigDict['CPUScalingFactor'] )

      if 'CPUNormalizationFactor' in ceConfigDict:
        pilotOptions.append( "-o '/LocalSite/CPUNormalizationFactor=%s'" % ceConfigDict['CPUNormalizationFactor'] )

        self.log.info( "pilotOptions: ", ' '.join(pilotOptions))

      httpProxy = ''
      if 'HttpProxy' in ceConfigDict:
        httpProxy = ceConfigDict['HttpProxy']

      pilotDir = ''
      if 'JobExecDir' in ceConfigDict:
        pilotExecDir = ceConfigDict['JobExecDir']

      try:
        pilotScript = self._writePilotScript( workingDirectory, pilotOptions, proxy, httpProxy, pilotExecDir )
      except:
        self.log.exception( ERROR_SCRIPT )
        try:
          os.chdir( baseDir )
          shutil.rmtree( workingDirectory )
        except:
          pass
        return S_ERROR( ERROR_SCRIPT )

      self.log.info("Pilots to submit: ", pilotsToSubmit)
      while submittedPilots < pilotsToSubmit:
        # Find out how many pilots can be submitted
        ret = computingElement.available( )
        if not ret['OK']:
          self.log.error('Can not determine if pilot should be submitted: ', ret['Message'])
          break
        maxPilotsToSubmit = ret['Value']
        self.log.info("Submit Pilots: ", maxPilotsToSubmit)
        if not maxPilotsToSubmit:
          break
        # submit the pilots and then check again
        for i in range( min(maxPilotsToSubmit,pilotsToSubmit-submittedPilots) ):
          submission = computingElement.submitJob(pilotScript, '', '')
          if not submission['OK']:
            self.log.error('Pilot submission failed: ', submission['Message'])
            # cleanup
            try:
              os.chdir( baseDir )
              shutil.rmtree( workingDirectory )
            except:
              pass
            return S_ERROR('Pilot submission failed after ' + str(submittedPilots) + ' pilots submitted successful')
          submittedPilots += 1
          # let the batch system some time to digest the submitted job
          time.sleep(1)

      #next CE

    try:
      os.chdir( baseDir )
      shutil.rmtree( workingDirectory )
    except:
      pass

    return S_OK(submittedPilots)

  def _listQueues( self, pilotRequirements ):
    """
     For each defined CE return the list of Queues with available, running and waiting slots,
     matching the requirements of the pilots.
     Currently only CPU time is considered
    """
    availableQueues = []
    result = self.computingElement.available( pilotRequirements )
    if not result['OK']:
      self.log.error( 'Can not determine available queues', result['Message'] )
      return False
    return result['Value']


  def _writePilotScript( self, workingDirectory, pilotOptions, proxy, httpProxy, pilotExecDir ):
    """
     Prepare the script to execute the pilot
     For the moment it will do like Grid Pilots, a full DIRAC installation

     It assumes that the pilot script will have access to the submit working directory
    """
    try:
      compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) ).replace('\n','')
      compressedAndEncodedPilot = base64.encodestring( bz2.compress( open( self.pilot, "rb" ).read(), 9 ) ).replace('\n','')
      compressedAndEncodedInstall = base64.encodestring( bz2.compress( open( self.install, "rb" ).read(), 9 ) ).replace('\n','')
    except:
      self.log.exception('Exception during file compression of proxy, dirac-pilot or dirac-install')
      return S_ERROR('Exception during file compression of proxy, dirac-pilot or dirac-install')

    localPilot = """#!/bin/bash
/usr/bin/env python << EOF
#
import os, tempfile, sys, shutil, base64, bz2
try:
  pilotExecDir = '%(pilotExecDir)s'
  if not pilotExecDir:
    pilotExecDir = None
  pilotWorkingDirectory = tempfile.mkdtemp( suffix = 'pilot', prefix = 'DIRAC_', dir = pilotExecDir )
  os.chdir( pilotWorkingDirectory )
  open( 'proxy', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedProxy)s" ) ) )
  open( '%(pilotScript)s', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedPilot)s" ) ) )
  open( '%(installScript)s', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedInstall)s" ) ) )
  os.chmod("proxy",0600)
  os.chmod("%(pilotScript)s",0700)
  os.chmod("%(installScript)s",0700)
  if "LD_LIBRARY_PATH" not in os.environ:
    os.environ["LD_LIBRARY_PATH"]=""
  os.environ["X509_USER_PROXY"]=os.path.join(pilotWorkingDirectory, 'proxy')
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
""" % { 'compressedAndEncodedProxy': compressedAndEncodedProxy,
        'compressedAndEncodedPilot': compressedAndEncodedPilot,
        'compressedAndEncodedInstall': compressedAndEncodedInstall,
        'httpProxy': httpProxy,
        'pilotScript': os.path.basename(self.pilot),
        'installScript': os.path.basename(self.install),
        'pilotOptions': ' '.join( pilotOptions ),
        'pilotExecDir': pilotExecDir }

    fd, name = tempfile.mkstemp( suffix = '_pilotwrapper.py', prefix = 'DIRAC_', dir=workingDirectory)
    pilotWrapper = os.fdopen(fd, 'w')
    pilotWrapper.write( localPilot )
    pilotWrapper.close()

    return name

  def _getPilotProxyFromDIRACGroup( self, ownerDN, ownerGroup, requiredTimeLeft ):
    """
    Download a limited pilot proxy with VOMS extensions depending on the group
    """
    #Assign VOMS attribute
    vomsAttr = CS.getVOMSAttributeForGroup( ownerGroup )
    if not vomsAttr:
      self.log.info( "Downloading a proxy without VOMS extensions for %s@%s" % ( ownerDN, ownerGroup ) )
      return gProxyManager.downloadProxy( ownerDN, ownerGroup, limited = True,
                                          requiredTimeLeft = requiredTimeLeft )
    else:
      self.log.info( "Downloading a proxy with '%s' VOMS extension for %s@%s" % ( vomsAttr, ownerDN, ownerGroup ) )
      return gProxyManager.downloadVOMSProxy( ownerDN,
                                     ownerGroup,
                                     limited = True,
                                     requiredTimeLeft = requiredTimeLeft,
                                     requiredVOMSAttribute = vomsAttr )
