########################################################################
# $HeadURL:  $
# File :   SiteDirector.py
# Author : A.T.
########################################################################

"""  The Site Director is a simple agent performing pilot job submission to particular sites.
"""

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Resources.Computing.ComputingElement import getResourceDict
from DIRAC.Core.Utilities.ThreadPool                       import ThreadPool
from DIRAC.Resources.Computing.ComputingElementFactory    import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client.ServerUtils     import pilotAgentsDB, taskQueueDB
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
import os, base64, bz2, tempfile, random, socket
import DIRAC

__RCSID__ = "$Id: $"

DIRAC_PILOT = os.path.join( DIRAC.rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'dirac-pilot.py' )
DIRAC_INSTALL = os.path.join( DIRAC.rootPath, 'DIRAC', 'Core', 'scripts', 'dirac-install.py' )
TRANSIENT_PILOT_STATUS = ['Submitted','Waiting','Running','Scheduled','Ready']
FINAL_PILOT_STATUS = ['Aborted','Failed','Done']

class SiteDirector( AgentModule ):

  def initialize( self ):
    """ Standard constructor
    """
    self.am_setOption( "PollingTime", 60.0 )
    self.am_setOption( "maxPilotWaitingHours", 6 )
    
    # Get the site description dictionary
    siteName = self.am_getOption('Site','Unknown')
    if siteName == 'Unknown':
      siteName = gConfig.getValue('/DIRAC/Site','Unknown')   
      if siteName == 'Unknown':
        return S_ERROR('Unknown site')  
    self.siteName = siteName
    
    self.genericPilotDN = self.am_getOption('GenericPilotDN','Unknown')
    self.genericPilotGroup = self.am_getOption('GenericPilotGroup','Unknown')
    self.pilot = DIRAC_PILOT
    self.install = DIRAC_INSTALL
    
    # Flags
    self.updateStatus = self.am_getOption('UpdatePilotStatus',True)
    self.getOutput = self.am_getOption('GetPilotOutput',True)
    
    self.localhost = socket.getfqdn()
    
    self.queueDict = {}
    result = self.getQueues()
    if not result['OK']:
      return result
        
    return S_OK()
  
  def getQueues(self):
    """ Get the list of relevant CEs and their descriptions
    """
    
    ceFactory = ComputingElementFactory()
    
    ceTypes = self.am_getOption('CETypes',[])
    ceList = self.am_getOption('CE',[])    
    if not ceList:
      # Look up CE definitions in the site CS description
      gridType = self.siteName.split('.')[0]
      result = gConfig.getSections('/Resources/Sites/%s/%s/CEs' % (gridType,self.siteName))
      if not result['OK']:
        return S_ERROR('Failed to look up the CS for the site %s CEs' % self.siteName)
      if not result['Value']:
        return S_ERROR('No CEs found for site %s' % self.siteName)
      ceTotalList = result['Value']    
      for ce in ceTotalList:
        ceType = gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/CEType' % (gridType,self.siteName,ce), 'Unknown')
        result = gConfig.getOptionsDict('/Resources/Sites/%s/%s/CEs/%s' % (gridType,self.siteName,ce) )
        if not result['OK']:
          return S_ERROR('Failed to look up the CS for ce %s' % ce )
        ceDict = result['Value']
        if ceType in ceTypes:
          ceList.append((ce,ceType,ceDict))
    
    self.queueDict = {}
    for ce,ceType,ceDict in ceList:
      section = '/Resources/Sites/%s/%s/CEs/%s/Queues' % (gridType,self.siteName,ce)    
      result = gConfig.getSections(section)
      if not result['OK']:
        return S_ERROR('Failed to look up the CS for queues')
      if not result['Value']:
        return S_ERROR('No Queues found for site %s, ce %s' % (self.siteName,ce))
      
      queues = result['Value']
      for queue in queues:
        result = gConfig.getOptionsDict('%s/%s' % (section,queue) )
        if not result['OK']:
          return S_ERROR('Failed to look up the CS for ce,queue %s,%s' % (ce,queue) )
        queueName = '%s_%s' % (ce,queue)
        self.queueDict[queueName] = {}
        self.queueDict[queueName]['ParametersDict'] = result['Value']
        self.queueDict[queueName]['ParametersDict']['Queue'] = queue
        self.queueDict[queueName]['ParametersDict']['Site'] = self.siteName
        queueDict = dict(ceDict)
        queueDict.update(self.queueDict[queueName]['ParametersDict'])
        result = ceFactory.getCE(ceName=queueName,
                                 ceType=ceType, 
                                 ceParametersDict = queueDict)
        if not result['OK']:
          return result
        self.queueDict[queueName]['CE'] = result['Value']
        self.queueDict[queueName]['CEName'] = ce
        self.queueDict[queueName]['QueueName'] = queue
        result = self.queueDict[queueName]['CE'].isValid()
        if not result['OK']:
          self.log.fatal( result['Message'] )
          return result
            
    return S_OK()  
    
  def execute(self):
    """ Main execution method
    """

    result = self.submitJobs()
    if not result['OK']:
      self.log.error('Erros in the job submission: %s' % result['Message'])
      
    if self.updateStatus:  
      result = self.updatePilotStatus()
      if not result['OK']:
        self.log.error('Erros in updating pilot status: %s' % result['Message'])  
      
    return S_OK()  

  def submitJobs(self):
    """ Go through defined computing elements and submit jobs if necessary
    """
      
    for queue in self.queueDict:
      ce = self.queueDict[queue]['CE']
      ceName = self.queueDict[queue]['CEName']
      queueName = self.queueDict[queue]['QueueName']
      result = ce.available()
      if not result['OK']:
        self.log.warn('Failed to check the availability of queue %s: %s' (queue,result['message']))
        continue
            
      totalSlots = result['Value']
      
      ceDict = ce.getParameterDict()
      result = taskQueueDB.getMatchingTaskQueues( ceDict )
      
      if not result['OK']:
        self.log.error( 'Could not retrieve TaskQueues from TaskQueueDB', result['Message'] )
        return result
      taskQueueDict = result['Value']
      
      print taskQueueDict
      if not taskQueueDict:
        continue
      
      totalTQJobs = 0
      for tq in taskQueueDict:
        totalTQJobs += taskQueueDict[tq]['Jobs']
        
      pilotsToSubmit = min(totalSlots,totalTQJobs)  
      
      pilotsToSubmit = 1
      
      if pilotsToSubmit > 0:
        self.log.info('Going to submit %d pilots to %s queue' % (pilotsToSubmit,queue) )
  
        result = self.__getExecutable(queue,pilotsToSubmit)
        if not result['OK']:
          return result
        
        executable = result['Value']
        result = ce.submitJob(executable,'',pilotsToSubmit)
        if not result['OK']:
          self.log.error('Failed submission to queue %s: %s' (queue,result['Message']))
        
        # Add pilots to the PilotAgentsDB assign pilots to TaskQueue proportionally to the
        # task queue priorities
        pilotList = result['Value']
        nPilots = len(pilotList)
        tqPriorityList = []
        sumPriority = 0.
        for tq in taskQueueDict:
          sumPriority += taskQueueDict[tq]['Priority']
          tqPriorityList.append((tq,sumPriority))
        rndm = random.random()*sumPriority   
        tqDict = {}    
        for pilotID in pilotList:
          rndm = random.random()*sumPriority       
          for tq,prio in tqPriorityList: 
            if rndm < prio:
              tqID = tq
              break
          if not tqDict.has_key(tqID):
            tqDict[tqID] = []
          tqDict[tqID].append(pilotID)    
          
        for tqID,pilotList in tqDict.items():    
          result = pilotAgentsDB.addPilotTQReference(pilotList,
                                                     tqID,
                                                     self.genericPilotDN,
                                                     self.genericPilotGroup,
                                                     self.localhost,
                                                     'DIRAC',
                                                     '')
          if not result['OK']:
            self.log.error('Failed add pilots to the PilotAgentsDB: %s' % result['Message'])
            continue
          for pilot in pilotList: 
            result = pilotAgentsDB.setPilotStatus(pilot,'Submitted',ceName,'Successfuly submitted by the SiteDirector','',queueName)
            if not result['OK']:
              self.log.error('Failed to set pilot status: %s' % result['Message'])
              continue
          
    return S_OK()  
  
#####################################################################################
  def __getExecutable(self,queue,pilotsToSubmit):
    """ Prepare the full executable for queue
    """ 
    
    result = gProxyManager.getPilotProxyFromDIRACGroup(self.genericPilotDN, self.genericPilotGroup, 1000 )
    if not result['OK']:
      return result
    
    proxy = result['Value']
    
    pilotOptions = self.__getPilotOptions(queue,pilotsToSubmit)
    workingDirectory = '/Users/atsareg/Documents/workspace/DIRAC_SVN/test/CREATIS/SiteDirector'
    executable = self.__writePilotScript(workingDirectory, pilotOptions, proxy)
    return S_OK(executable)
 
#####################################################################################    
  def __getPilotOptions(self,queue,pilotsToSubmit):
    """ Prepare pilot options
    """  
    
    queueDict = self.queueDict[queue]['ParametersDict']
    
    print queueDict
    
    pilotOptions = [ "-V '%s'" % gConfig.getValue( "/DIRAC/VirtualOrganization", "lhcb" ) ]
    ownerDN = self.genericPilotDN
    ownerGroup = self.genericPilotGroup
    result = gProxyManager.requestToken( ownerDN, ownerGroup, pilotsToSubmit*5 )
    if not result[ 'OK' ]:
      self.log.error( ERROR_TOKEN, result['Message'] )
      return S_ERROR( ERROR_TOKEN )
    ( token, numberOfUses ) = result[ 'Value' ]
    pilotOptions.append( '-o /Security/ProxyToken=%s' % token )
    # Use Filling mode
    pilotOptions.append( '-M %s' % 5 )

    # Debug
    pilotOptions.append( '-d' )
    # Setup.
    pilotOptions.append( '-S %s' % queueDict['Setup'] )
    # CS Servers
    csServers = gConfig.getValue( "/DIRAC/Configuration/Servers", [] )
    pilotOptions.append( '-C %s' % ",".join( csServers ) )
    # DIRAC Extensions
    extensionsList = gConfig.getValue( "/DIRAC/Extensions", [] )
    if extensionsList:
      pilotOptions.append( '-e %s' % ",".join( extensionsList ) )
    # Requested version of DIRAC
    pilotOptions.append( '-r %s' % 'v5r8' )
    # Requested CPU time
    pilotOptions.append( '-T %s' % queueDict['CPUTime'] )
    # SiteName
    pilotOptions.append( '-n %s' % queueDict['Site'] )
    if 'ClientPlatform' in queueDict:
      pilotOptions.append( "-p '%s'" % queueDict['ClientPlatform'])

    if 'SharedArea' in queueDict:
      pilotOptions.append( "-o '/LocalSite/SharedArea=%s'" % queueDict['SharedArea'] )

    if 'CPUScalingFactor' in queueDict:
      pilotOptions.append( "-o '/LocalSite/CPUScalingFactor=%s'" % queueDict['CPUScalingFactor'] )

    if 'CPUNormalizationFactor' in queueDict:
      pilotOptions.append( "-o '/LocalSite/CPUNormalizationFactor=%s'" % queueDict['CPUNormalizationFactor'] )

    self.log.info( "pilotOptions: ", ' '.join(pilotOptions))

    return pilotOptions
    
#####################################################################################    
  def __writePilotScript(self,workingDirectory, pilotOptions, proxy='', httpProxy=''):
    """ Bundle together and write out the pilot executable script, admixt the proxy if given
    """
    
    try:
      compressedAndEncodedProxy = ''
      proxyFlag = 'False'
      if proxy:
        compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) ).replace('\n','')
        proxyFlag = 'True'
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
        'pilotScript': os.path.basename(self.pilot), \
        'installScript': os.path.basename(self.install),
        'pilotOptions': ' '.join( pilotOptions ),
        'proxyFlag': proxyFlag }

    fd, name = tempfile.mkstemp( suffix = '_pilotwrapper.py', prefix = 'DIRAC_', dir=workingDirectory)
    pilotWrapper = os.fdopen(fd, 'w')
    pilotWrapper.write( localPilot )
    pilotWrapper.close()

    return name
    
  def updatePilotStatus(self):
    """ Update status of pilots in transient states
    """
    for queue in self.queueDict:
      ce = self.queueDict[queue]['CE']
      ceName = self.queueDict[queue]['CEName']
      queueName = self.queueDict[queue]['QueueName']
      result = pilotAgentsDB.selectPilots({'DestinationSite':ceName,
                                           'Queue':queueName,'GridType':'DIRAC',
                                           'GridSite':self.siteName})
      if not result['OK']:
        self.log.error('Failed to select pilots: %s' % result['Message'])
        continue
      
      pilotRefs = result['Value']
      if not pilotRefs:
        continue
      
      #print "AT >>> pilotRefs", pilotRefs
      
      result = pilotAgentsDB.getPilotInfo(pilotRefs)
      if not result['OK']:
        self.log.error('Failed to get pilots info: %s' % result['Message'])
        continue
      pilotDict = result['Value']
      
      #print "AT >>> pilotDict", pilotDict
      
      result = ce.getJobStatus(pilotRefs)
      if not result['OK']:
        self.log.error('Failed to get pilots status from CE: %s' % result['Message'])
        continue
      pilotCEDict = result['Value']
      
      #print "AT >>> pilotCEDict", pilotCEDict
      
      for pRef in pilotRefs:
        newStatus = ''
        if pilotDict[pRef]['Status'] == pilotCEDict[pRef]:
          # Status did not change, continue
          continue
        elif pilotCEDict[pRef] == "Unknown" and not pilotDict[pRef]['Status'] in FINAL_PILOT_STATUS:
          # Pilot finished without reporting, consider it Aborted
          newStatus = 'Aborted'
        elif pilotCEDict[pRef] != 'Unknown' :
          # Update the pilot status to the new value
          newStatus = pilotCEDict[pRef]     
        
        if newStatus:
          self.log.info('Updating status to %s for pilot %s' % (newStatus,pRef) )  
          result = pilotAgentsDB.setPilotStatus(pRef,newStatus,'','Updated by SiteDirector')
          
        # Retrieve the pilot output now            
        if pilotDict[pRef]['OutputReady'].lower() == 'false' and self.getOutput:
          self.log.info('Retrieving output for pilot %s' % pRef )  
          result = ce.getJobOutput(pRef)
          if not result['OK']:
            self.log.error('Failed to get pilot output: %s' % result['Message'])
          else:
            output,error = result['Value']
            result = pilotAgentsDB.storePilotOutput(pRef,output,error)
            if not result['OK']:
              self.log.error('Failed to store pilot output: %s' % result['Message'])  
                 
    return S_OK()      
    
    