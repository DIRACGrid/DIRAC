########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/InputDataAgent.py,v 1.34 2008/12/04 15:26:57 acasajus Exp $
# File :   InputDataAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Input Data Agent queries the file catalogue for specified job input data and adds the
      relevant information to the job optimizer parameters to be used during the
      scheduling decision.

"""

__RCSID__ = "$Id: InputDataAgent.py,v 1.34 2008/12/04 15:26:57 acasajus Exp $"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.SiteSEMapping                    import getSitesForSE
from DIRAC.Core.Utilities.Shifter                          import setupShifterProxyInEnv
from DIRAC                                                 import gConfig, S_OK, S_ERROR

import os, re, time, string

class InputDataAgent(OptimizerModule):

  #############################################################################
  def initializeOptimizer(self):
    """Initialize specific parameters for JobSanityAgent.
    """
    self.failedMinorStatus = self.am_getOption( '/FailedJobStatus', 'Input Data Not Available' )
    #this will ignore failover SE files
    self.diskSE            = self.am_getOption( '/DiskSE',['-disk','-DST','-USER'] )
    self.tapeSE            = self.am_getOption( '/TapeSE',['-tape','-RDST','-RAW'] )

    #Define the shifter proxy needed
    self.am_setModuleParam( "shifterProxy", "ProductionManager" )

    try:
      from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
      self.fileCatalog = LcgFileCatalogCombinedClient()
    except Exception,x:
      msg = 'Failed to create LcgFileCatalogClient with exception:'
      self.log.fatal(msg,str(x))
      return S_ERROR(msg+str(x))

    return S_OK()

  #############################################################################
  def checkJob(self,job,classAdJob):
    """This method controls the checking of the job.
    """

    result = self.jobDB.getInputData(job)
    if not result['OK']:
      self.log.warn('Failed to get input data from JobdB for %s' % (job) )
      self.log.warn(result['Message'])
      return result
    if not result['Value']:
      self.log.verbose('Job %s has no input data requirement' % (job) )
      return self.setNextOptimizer(job)

    self.log.verbose('Job %s has an input data requirement and will be processed' % (job))
    inputData = result['Value']
    result = self.__resolveInputData(job,inputData)
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    resolvedData = result['Value']
    result = self.setOptimizerJobInfo(job,self.am_getModuleParam( 'optimizerName' ),resolvedData)
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    return self.setNextOptimizer(job)


  #############################################################################
  def __resolveInputData( self, job, inputData ):
    """This method checks the file catalogue for replica information.
    """
    lfns = [string.replace(fname,'LFN:','') for fname in inputData]
    start = time.time()
    replicas = self.fileCatalog.getReplicas(lfns)
    timing = time.time() - start
    self.log.info('LFC Lookup Time: %.2f seconds ' % (timing) )
    if not replicas['OK']:
      self.log.warn(replicas['Message'])
      return replicas

    badLFNCount = 0
    badLFNs = []
    catalogResult = replicas['Value']

    if catalogResult.has_key('Failed'):
      for lfn,cause in catalogResult['Failed'].items():
        badLFNCount+=1
        badLFNs.append('LFN:%s Problem: %s' %(lfn,cause))

    if catalogResult.has_key('Successful'):
      for lfn,reps in catalogResult['Successful'].items():
        if not reps:
          badLFNs.append('LFN:%s Problem: Null replica value' %(lfn))
          badLFNCount+=1

    if badLFNCount:
      self.log.info('Found %s problematic LFN(s) for job %s' % (badLFNCount,job) )
      param = string.join(badLFNs,'\n')
      self.log.info(param)
      result = self.setJobParam(job,self.am_getModuleParam( 'optimizerName' ),param)
      if not result['OK']:
        self.log.warn(result['Message'])
      return S_ERROR('Input Data Not Available')

    inputData = catalogResult['Successful']
    siteCandidates = self.__getSiteCandidates(inputData)
    if not siteCandidates['OK']:
      self.log.warn(siteCandidates['Message'])
      return siteCandidates

    guids = True
    guidDict = self.fileCatalog.getFileMetadata(lfns)
    if not guidDict['OK']:
      self.log.warn(guidDict['Message'])
      guids = False

    failed = guidDict['Value']['Failed']
    if failed:
      self.log.warn('Failed to establish some GUIDs')
      self.log.warn(failed)
      guids = False

    if guids:
      for lfn,reps in replicas['Value']['Successful'].items():
        guidDict['Value']['Successful'][lfn].update(reps)

      replicas = guidDict

    result = {}
    result['Value'] = replicas
    result['SiteCandidates'] = siteCandidates['Value']
    return S_OK(result)

  #############################################################################
  def __getSiteCandidates(self,inputData):
    """This method returns a list of possible site candidates based on the
       job input data requirement.  For each site candidate, the number of files
       on disk and tape is resolved.
    """
    fileSEs = {}
    for lfn,replicas in inputData.items():
      siteList = []
      for se in replicas.keys():
        sites = getSitesForSE(se)
        if sites['OK']:
          siteList += sites['Value']
      fileSEs[lfn] = siteList

    siteCandidates = []
    i = 0
    for file,sites in fileSEs.items():
      if not i:
        siteCandidates = sites
      else:
        tempSite = []
        for site in siteCandidates:
          if site in sites:
            tempSite.append(site)
        siteCandidates = tempSite
      i += 1

    if not len(siteCandidates):
      return S_ERROR('No candidate sites available')

    #In addition, check number of files on tape and disk for each site
    #for optimizations during scheduling
    siteResult = {}
    for site in siteCandidates: siteResult[site]={'disk':0,'tape':0}

    for lfn,replicas in inputData.items():
      for se,surl in replicas.items():
        sites = getSitesForSE(se)
        if sites['OK']:
          for site in sites['Value']:
            if site in siteCandidates:
              for disk in self.diskSE:
                if re.search(disk+'$',se):
                  siteResult[site]['disk'] = siteResult[site]['disk']+1
              for tape in self.tapeSE:
                if re.search(tape+'$',se):
                  siteResult[site]['tape'] = siteResult[site]['tape']+1

    return S_OK(siteResult)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
