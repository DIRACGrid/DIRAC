########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobPathAgent.py,v 1.3 2007/11/19 10:07:40 paterson Exp $
# File :   JobPathAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Job Path Agent determines the chain of Optimizing Agents that must 
      work on the job prior to the scheduling decision.

      Initially this takes jobs in the received state and starts the jobs on the
      optimizer chain.  The next development will be to explicitly specify the 
      path through the optimizers. 

"""
__RCSID__ = "$Id: JobPathAgent.py,v 1.3 2007/11/19 10:07:40 paterson Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.ConfigurationSystem.Client.Config               import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC                                                 import S_OK, S_ERROR
import string,re

OPTIMIZER_NAME = 'JobPath'

class JobPathAgent(Optimizer):

  #############################################################################
  def __init__(self):
    """ Constructor, takes system flag as argument.
    """
    Optimizer.__init__(self,OPTIMIZER_NAME,'Received',enableFlag=True)
    
  #############################################################################    
  def initialize(self):
    """Initialize specific parameters for JobPathAgent.
    """ 
    result = Optimizer.initialize(self)  
    self.basePath  = gConfig.getValue(self.section+'/BasePath','JobPath,JobSanity,')
    self.ancestors = gConfig.getValue(self.section+'/Ancestors','AncestorFiles')
    self.inputData = gConfig.getValue(self.section+'/InputData','InputData')
    self.procDB    = gConfig.getValue(self.section+'/ProcDB','ProcessingDB')
    self.endPath   = gConfig.getValue(self.section+'/EndPath','JobScheduling,TaskQueue')    
    return result                               

  #############################################################################
  def checkJob(self,job):
    """This method controls the checking of the job.
    """
    self.log.info('Job %s will be processed by %sAgent' % (job,self.optimizerName))
    jdl = self.jobDB.getJobJDL(job,original=True)
    if jdl['OK'] and len(jdl['Value']):
      jdl = jdl['Value']
      result = self.setJobPath(job,jdl)
    else:
      self.log.error('Could not obtain original JDL for job %s' %(job))
      return S_ERROR('JDL not found in JobDB')

    return result

  #############################################################################
  def setJobPath(self,job,jdl):
    """This method controls the checking of the job.
    """
    classadJob = ClassAd('['+jdl+']')
    if not classadJob.isOK():
      self.log.debug("Warning: illegal JDL for job %s, will be marked problematic" % (job))
      result = S_ERROR('Problematic JDL')

    #Check if job defines a path itself
    jobPath = classadJob.get_expression('JobPath').replace('"','').replace('Unknown','')
    if jobPath:
      self.log.info('Job %s defines its own optimizer chain %s' %(job,jobPath))
      result = self.processJob(job,string.split(jobPath,',')) 
      return S_OK('Job defines own path') #in this case next job will be checked before returning 

    #If no path, construct based on JDL 
    path = self.basePath
    
    result = self.jobDB.getInputData(job)
    if not result['OK']:
      self.log.error('Failed to get input data from JobdB for %s' % (job) )
      self.log.error(result['Message'])
      return S_OK()

    if len(result['Value']):
      self.log.info('Job %s has an input data requirement' % (job))   
      ancestorDepth = classadJob.get_expression('AncestorDepth').replace('"','').replace('Unknown','')
      if ancestorDepth:
        self.log.info('Job %s has specified ancestor depth' % (job)) 
        path += self.ancestors+','+self.inputData+','
      else:
        path += self.inputData+','

      jobType = classadJob.get_expression('JobType').replace('"','').replace('Unknown','')
      if jobType=='processing':
        self.log.info('Job %s is of processing type' % (job)) 
        path += self.procDB+','
    else:
      self.log.info('Job %s has no input data requirement' % (job))      

    path += self.endPath
    self.log.info('Constructed path for job %s is: %s' %(job,path))
    result = self.processJob(job,string.split(path,','))
    return S_OK('Job path constructed')


  #############################################################################    
  def processJob(self,job,chain):
    """Set job path and send to next optimizer
    """
    result = self.setOptimizerChain(job,chain)   
    if not result['OK']:
      self.log.error(result['Message'])  
    result = self.setNextOptimizer(job)
    if not result['OK']:
      self.log.error(result['Message']) 
    result = self.setJobParam(job,'JobPath',string.join(chain,','))  
    if not result['OK']:
      self.log.error(result['Message'])
    return result
    
  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
