########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobPolicyAgent.py,v 1.2 2008/01/20 16:32:50 paterson Exp $
# File :   JobPolicyAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Job Policy Agent populates all job JDL with default VO parameters
      specified in the DIRAC/VOPolicy CS section.

"""

__RCSID__ = "$Id: JobPolicyAgent.py,v 1.2 2008/01/20 16:32:50 paterson Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC                                                 import gConfig, S_OK, S_ERROR

import os, re, time, string

OPTIMIZER_NAME = 'JobPolicy'

class JobPolicyAgent(Optimizer):

  #############################################################################
  def __init__(self):
    """ Constructor, takes system flag as argument.
    """
    Optimizer.__init__(self,OPTIMIZER_NAME,enableFlag=True)

  #############################################################################
  def initialize(self):
    """Initialize specific parameters for JobSanityAgent.
    """
    result = Optimizer.initialize(self)
    return result

  #############################################################################
  def checkJob(self,job):
    """This method controls the checking of the job.
    """

    voPolicyDict = gConfig.getOptionsDict('/DIRAC/VOPolicy')
    if not voPolicyDict['OK']:
      self.log.warn('Could not obtain /DIRAC/VOPolicy from CS')
      self.log.verbose(voPolicyDict)
      return voPolicyDict

    self.log.info("Checking JDL for job: %s" %(job))
    retVal = self.jobDB.getJobJDL(job)
    if not retVal['OK']:
      result = S_ERROR()
      result['Message'] = "Job JDL not found in JobDB"
      return result

    jdl = retVal['Value']
    if not jdl:
      result = S_ERROR('Null JDL returned from JobDB')
      return result

    classadJob = ClassAd('['+jdl+']')
    if not classadJob.isOK():
      self.log.debug("Warning: illegal JDL for job %s, will be marked problematic" % (job))
      result = S_ERROR()
      result['Value'] = "Illegal Job JDL"
      return result

    voPolicy = voPolicyDict['Value']
    jdlModified = False
    for param,val in voPolicy.items():
      if classadJob.lookupAttribute(param):
        self.log.verbose('%s already exists in job %s JDL' %(param,job))
      else:
        jdlModified = True
        self.log.verbose('Insterting default %s = %s in JDL for job %s' %(param,val,job))
        classadJob.insertAttributeString(param,val)

    if jdlModified:
      self.log.verbose('Storing modified JDL for %s' %(job))
      newJDL = classadJob.asJDL()
      result = self.jobDB.setJobJDL(int(job),newJDL)
      if not result['OK']:
        self.log.warn('Problem storing modified job JDL')
        self.log.warn(result)
        return result

    self.log.info('Processed job %s, JDL modified flag %s' %(job,jdlModified))

    result = self.setNextOptimizer(job)
    if not result['OK']:
      self.log.warn(result['Message'])
    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
