########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/private/DIRACPilotDirector.py,v 1.19 2009/08/19 14:36:19 ffeldhau Exp $
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
__RCSID__ = "$Id: DIRACPilotDirector.py,v 1.19 2009/08/19 14:36:19 ffeldhau Exp $"

import os, sys, tempfile, shutil, time, base64, bz2

from DIRAC.WorkloadManagementSystem.private.PilotDirector import PilotDirector
from DIRAC.Resources.Computing.ComputingElementFactory    import ComputingElementFactory
from DIRAC.Core.Security import CS
from DIRAC.FrameworkSystem.Client.ProxyManagerClient      import gProxyManager
from DIRAC import S_OK, S_ERROR, DictCache, gConfig, rootPath

ERROR_CE         = 'No CE available'
ERROR_JDL        = 'Could not create Pilot script'
ERROR_SCRIPT     = 'Could not copy Pilot script'

COMPUTING_ELEMENTS = ['InProcess']
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

    self.computingElements = COMPUTING_ELEMENTS
    # To run a DIRAC Pilot Director we require Site Name to be properly defined in the 
    # local configuration file
    self.siteName          = gConfig.getValue('/LocalSite/Site','')
    if not self.siteName:
      self.log.error( 'Can not run a Director if Site Name is not defined' )
      sys.exit()

    self.__failingCECache  = DictCache()
    self.__ticketsCECache  = DictCache()
    
    self.clientPlatform = gConfig.getValue('LocalSite/ClientPlatform', '')
    
    self.sharedArea = gConfig.getValue('LocalSite/SharedArea')  
      
    self.waitingToRunningRatio = gConfig.getValue('LocalSite/WaitingToRunningRatio', WAITING_TO_RUNNING_RATIO)
    self.maxWaitingJobs = gConfig.getValue('LocalSite/MaxWaitingJobs', MAX_WAITING_JOBS)
    self.maxNumberJobs = gConfig.getValue('LocalSite/MaxNumberJobs', MAX_NUMBER_JOBS)
    self.httpProxy = gConfig.getValue('LocalSite/HttpProxy', '')
    
    self.installURL = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/scripts/dirac-install'
    self.pilotURL = 'http://isscvs.cern.ch/cgi-bin/cvsweb.cgi/~checkout~/DIRAC3/DIRAC/WorkloadManagementSystem/PilotAgent/dirac-pilot?rev=HEAD;content-type=text%2Fplain;cvsroot=dirac'

  def configure(self, csSection, submitPool ):
    """
     Here goes common configuration for DIRAC PilotDirector
    """

    PilotDirector.configure( self, csSection, submitPool )
    self.reloadConfiguration( csSection, submitPool )

    self.__failingCECache.purgeExpired()
    self.__ticketsCECache.purgeExpired()

    for ce in self.__failingCECache.getKeys():
      if ce in self.computingElements:
        try:
          self.computingElements.remove( ce )
        except:
          pass
    if self.computingElements:
      self.log.info( ' ComputingElements:', ', '.join(self.computingElements) )

    # FIXME: this is to start testing
    ceFactory = ComputingElementFactory(self.computingElements[0])
    ceName = self.computingElements[0]
    ceInstance = ceFactory.getCE()
    if not ceInstance['OK']:
      self.log.warn(ceInstance['Message'])
      try:
        os.chdir( baseDir )
        shutil.rmtree( workingDirectory )
      except:
        pass
      return ceInstance

    self.computingElement = ceInstance['Value']

    self.log.debug(self.computingElement.getDynamicInfo())

    if self.siteName:
      self.log.info( ' SiteName:', self.siteName )


  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    PilotDirector.configureFromSection( self, mySection )

    self.computingElements    = gConfig.getValue( mySection+'/ComputingElements'      , self.computingElements )
    self.siteName             = gConfig.getValue( mySection+'/SiteName'               , self.siteName )


  def _submitPilots( self, workDir, taskQueueDict, pilotOptions, pilotsToSubmit,
                     ceMask, submitPrivatePilot, privateTQ, proxy, pilotsPerJob ):
    """
      This method does the actual pilot submission to the DIRAC CE
      The logic is as follows:
      - If there are no available CE it return error
      - It creates a temp directory
      - Prepare a PilotScript
    """
    taskQueueID = taskQueueDict['TaskQueueID']
    ownerDN = taskQueueDict['OwnerDN']
    
    submittedPilots = 0
    
    if not self.computingElements:
      # Since we can exclude CEs from the list, it may become empty
      return S_ERROR( ERROR_CE )

    baseDir = os.getcwd()
    workingDirectory = tempfile.mkdtemp( prefix= 'TQ_%s_' % taskQueueID, dir = workDir )
    self.log.verbose( 'Using working Directory:', workingDirectory )
    os.chdir( workingDirectory )

    # set the Site Name
    if self.siteName:
      pilotOptions.append( "-n '%s'" % self.siteName)
      
    if self.clientPlatform:
      pilotOptions.append( "-p '%s'" % self.clientPlatform)
      
    if self.sharedArea:
      pilotOptions.append( "-o '/LocalSite/SharedArea=%s'" % self.sharedArea )
      
    #pilotOptions.append( "-o '/DIRAC/Configuration/Servers=dips://volhcb04.cern.ch:9135/Configuration/Server'" )
     
    self.log.info( "pilotOptions: ", ' '.join(pilotOptions))

    try:
      pilotScript = self._writePilotScript( workingDirectory, pilotOptions, proxy )
    except:
      self.log.exception( ERROR_SCRIPT )
      try:
        os.chdir( baseDir )
        shutil.rmtree( workingDirectory )
      except:
        pass
      return S_ERROR( ERROR_SCRIPT )
  
    self.log.info("Pilots to submit: ", pilotsToSubmit)
    for pilots in range(int(pilotsToSubmit)):
      ret = self._submitPilot()
      if not ret['OK']:
        self.log.error('Connot determine if pilot should be submitted: ', ret['Message'])
        break
      submitPilot = ret['Value']
      self.log.info("Submit Pilots: ", submitPilot)
      if submitPilot == False:
        break
      submission = self.computingElement.submitJob(pilotScript, '', '', '')
      if not submission['OK']:
        self.log.error('Pilot submission failed: ', submission['Message'])
        break
      submittedPilots += 1
    
    try:
      os.chdir( baseDir )
      #shutil.rmtree( workingDirectory )
    except:
      pass
    
    return S_OK(submittedPilots)

  def _writePilotScript( self, workingDirectory, pilotOptions, proxy ):
    """
     Prepare the script to execute the pilot
     For the moment it will do like Grid Pilots, a full DIRAC installation
    """
    try:
      compressedAndEncodedProxy = base64.b64encode( bz2.compress( proxy.dumpAllToString()['Value'] ) )
      compressedAndEncodedPilot = base64.b64encode( bz2.compress( open( self.pilot, "rb" ).read(), 9 ) )
      compressedAndEncodedInstall = base64.b64encode( bz2.compress( open( self.install, "rb" ).read(), 9 ) )
    except:
      self.log.exception('Exception during file compression of proxy, dirac-pilot or dirac-install')
      return S_ERROR('Exception during file compression of proxy, dirac-pilot or dirac-install')
    
    localPilot = """#!/usr/bin/env python
#
import os, tempfile, sys, shutil, base64, bz2
try:
  pilotWorkingDirectory = tempfile.mkdtemp( suffix = 'pilot', prefix= 'DIRAC_' )
  os.chdir( pilotWorkingDirectory )
  open( 'proxy', "w" ).write(bz2.decompress( base64.b64decode( "%(compressedAndEncodedProxy)s" ) ) )
  open( 'dirac-pilot', "w" ).write(bz2.decompress( base64.b64decode( "%(compressedAndEncodedPilot)s" ) ) )
  open( 'dirac-install', "w" ).write(bz2.decompress( base64.b64decode( "%(compressedAndEncodedInstall)s" ) ) )
  os.chmod("proxy",0600)
  os.chmod("dirac-pilot",0700)
  os.chmod("dirac-install",0700)
  os.environ["LD_LIBRARY_PATH"]=""
  os.environ["X509_USER_PROXY"]=os.path.join(pilotWorkingDirectory, 'proxy')
  os.environ["HTTP_PROXY"]="%(proxy)s"
  print os.environ
except Exception, x:
  print >> sys.stderr, x
  sys.exit(-1)
cmd = "python %(pilotScript)s %(pilotOptions)s"
print 'Executing:', cmd
sys.stdout.flush()
os.system( cmd )

#shutil.rmtree( pilotWorkingDirectory )

""" % { 'compressedAndEncodedProxy': compressedAndEncodedProxy,
        'compressedAndEncodedPilot': compressedAndEncodedPilot,
        'compressedAndEncodedInstall': compressedAndEncodedInstall,
        'proxy': self.httpProxy, \
        'pilotScript': os.path.basename(self.pilot), \
        'pilotOptions': ' '.join( pilotOptions ) }

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
      return S_ERROR( "No voms attribute assigned to group %s" % ownerGroup )
    return gProxyManager.downloadVOMSProxy( ownerDN,
                                   ownerGroup,
                                   limited = True,
                                   requiredTimeLeft = requiredTimeLeft,
                                   requiredVOMSAttribute = vomsAttr )    

  def _submitPilot(self):
    # first check status of the CE and determine how many pilots may be submitted
    submitPilot = False
        
    ret = self.computingElement.getDynamicInfo()
      
    if not ret['OK']:
      self.log.error('Failed to retrieve status information of the CE', ret['Message'])
    
    result = ret['Value']
    
    try:
      waitingJobs = int(result['WaitingJobs'])
      runningJobs = int(result['RunningJobs'])
    except:
      self.log.exception("getDynamicInfo didn't return integer values for WaitingJobs and/or RunningJobs")
      return S_ERROR("getDynamicInfo didn't return integer values for WaitingJobs and/or RunningJobs")
    
    if runningJobs == 0:
      if waitingJobs < self.maxWaitingJobs:
        submitPilot = True
    else:
      waitingToRunningRatio = float(waitingJobs) / float(runningJobs)
      # as jobs are in waiting state when they're submitted, we can submit a pilot if there is only 
      # one waiting job
      if waitingToRunningRatio < self.waitingToRunningRatio or waitingJobs == 1 :
        submitPilot = True
    
    totalNumberJobs = runningJobs + waitingJobs
      
    if (totalNumberJobs + 1) > self.maxNumberJobs:
      submitPilot = False
    
    return S_OK(submitPilot)