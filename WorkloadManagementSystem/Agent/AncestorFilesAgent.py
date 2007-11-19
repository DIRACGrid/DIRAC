########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/Attic/AncestorFilesAgent.py,v 1.5 2007/11/19 13:24:20 paterson Exp $
# File :   AncestorFilesAgent.py
# Author : Stuart Paterson
########################################################################

"""   The LHCb AncestorFilesAgent queries the Bookkeeping catalogue for ancestor
      files if the JDL parameter AncestorDepth is specified.  The ancestor files
      are subsequently checked in the LFC and the result is stored for the
      scheduling decision.  The Ancestor Files agent


"""

__RCSID__ = "$Id: AncestorFilesAgent.py,v 1.5 2007/11/19 13:24:20 paterson Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.ConfigurationSystem.Client.Config               import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
#this won't function until the getAncestors call is available...
#from DIRAC.Core.Utilities.genCatalog                       import getAncestors
from DIRAC                                                 import S_OK, S_ERROR
import os, re, time, string

OPTIMIZER_NAME = 'AncestorFiles'

class AncestorFilesAgent(Optimizer):

  #############################################################################
  def __init__(self):
    """ Constructor, takes system flag as argument.
    """
    Optimizer.__init__(self,OPTIMIZER_NAME,enableFlag=True)

  #############################################################################
  def initialize(self):
    """Initialize specific parameters for AncestorFilesAgent.
    """
    result = Optimizer.initialize(self)

    #until the BK interface is available, can disable the optimizer and pass jobs through
    self.disableAncestorCheck = True
    self.failedMinorStatus    = gConfig.getValue(self.section+'/FailedJobStatus','genCatalog Error')
    return result

  #############################################################################
  def checkJob(self):
    """ The main agent execution method
    """
    self.log.info('Job %s will be processed by %sAgent' % (job,self.optimizerName))
    result = self.getAncestorDepth(job)
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    ancestorDepth=result['Value']
    if ancestorDepth:
      self.log.info('Job %s has ancestor depth of %s' % (job,ancestorDepth))
      if int(ancestorDepth) > 0:
        result = self.resolveAncestorDepth(job,ancestorDepth)
        if not result['OK']:
          return result
    else:
      self.log.info('Job %s has no AncestorDepth (>0) specified in JDL' %(job))
      result = self.setNextOptimizer(job)
      if not result['OK']:
        self.log.error(result['Message'])
      return result

    return result

  #############################################################################
  def resolveAncestorDepth(self,job,ancestorDepth):
    """This method controls the checking of input data with ancestors and updates
       the optimizer information for the scheduling.
    """
    # Check if the job is suitable for the AncestorFilesAgent
    result = self.jobDB.getInputData(job)
    if result['OK']:
      if result['Value']:
        self.log.info('Job %s has an input data requirement and Ancestor Files will be checked' % (job))
        inputData = result['Value']

        if not self.disableAncestorCheck:
          result = self.setInputDataWithAncestors(job,inputData,ancestorDepth)
          if not result['OK']:
            self.log.error(result['Message'])
            return result
          ancestorFiles = result['Value']
          result = self.setOptimizerJobInfo(job,self.optimizerName,ancestorFiles)
          if not result['OK']:
            self.log.error(result['Message'])
            return result
        else:
          self.log.info('Ancestor files check is disabled, passing job to next optimizer')

        result = self.setNextOptimizer(job)
        if not result['OK']:
          self.log.error(result['Message'])
        return result
      else:
        self.log.debug('Job %s has no input data requirement' % (job))
        result = self.setNextOptimizer(job)
        if not result['OK']:
          self.log.error(result['Message'])
        return result
    else:
      self.log.error('Failed to get input data from JobdB for %s' %(job) )
      self.log.error(result['Message'])
      return result

  ############################################################################
  def setInputDataWithAncestors(self,inputData,ancestorDepth):
    """Extend the list of LFNs with the LFNs for their ancestor files
       for the generation depth specified in the job.
    """

    inputData = [ i.replace('LFN:','') for i in inputData]

    start = time.time()
    self.log.info('Need to remove when getAncestors call available')
    result = S_ERROR()
    #result = getAncestors(inputData,ancestorDepth)
    self.log.info('getAncestors lookup time %.2f' %(time.time()-start))

    if not result['OK']:
      self.log.error('Failed to get ancestor LFNs')
      if result.has_key('Message'):
        self.log.info('----------BK-Result------------')
        self.log.info(result['Message'])
        self.log.info('--------End-BK-Result----------')
        return result
      else:
        print result
        return S_ERROR('Failed to get ancestor LFNs')

    ancestorFiles = result['PFNs']
    for lfn,reps in ancestorFiles.items():
      inputData.append(lfn)

    result = self.jobDB.setInputData(job,inputData)
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    result = S_OK(ancestorFiles)
    return result

  #############################################################################
  def getAncestorDepth(self,job):
    """This method checks for any JDL parameters that require the treatment
       of further optimizers.  The JDL would already have been checked by the
       JobSanityAgent at this point.
    """
    jobID = str(job)
    self.log.debug("Checking JDL for job: "+jobID)
    retVal = self.jobDB.getJobJDL(jobID)

    if not retVal['OK']:
      result = S_ERROR()
      result['Value'] = "Job JDL not found in JobDB"
      return result

    jdl = retVal['Value']
    classadJob = ClassAd('['+jdl+']')
    ancestorDepth = classadJob.get_expression('AncestorDepth').replace('"','').replace('Unknown','')
    if not ancestorDepth:
      ancestorDepth=0
    result = S_OK()
    result['Value'] = ancestorDepth
    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
