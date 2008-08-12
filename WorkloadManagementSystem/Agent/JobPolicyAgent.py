########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobPolicyAgent.py,v 1.4 2008/08/12 17:32:43 rgracian Exp $
# File :   JobPolicyAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Job Policy Agent populates all job JDL with default VO parameters
      specified in the DIRAC/VOPolicy CS section.

"""

__RCSID__ = "$Id: JobPolicyAgent.py,v 1.4 2008/08/12 17:32:43 rgracian Exp $"

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
  def checkJob( self, job, jdl = None, classad = None ):
    """This method controls the checking of the job.
    """

    self.log.info("Checking JDL for job: %s" %(job))

    result = self.getJDLandClassad( job, jdl, classad )
    if not result['OK']:
      return result

    JDL = result['JDL']
    classadJob = result['Classad']
    
    return self.setNextOptimizer(job)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#