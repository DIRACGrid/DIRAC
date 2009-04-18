########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobPolicyAgent.py,v 1.6 2009/04/18 18:26:57 rgracian Exp $
# File :   JobPolicyAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Job Policy Agent populates all job JDL with default VO parameters
      specified in the DIRAC/VOPolicy CS section.

"""

__RCSID__ = "$Id: JobPolicyAgent.py,v 1.6 2009/04/18 18:26:57 rgracian Exp $"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC                                                 import gConfig, S_OK, S_ERROR

import os, re, time, string

class JobPolicyAgent(OptimizerModule):

  #############################################################################
  def checkJob( self, job, classAdJob ):
    """This method controls the checking of the job.
    """

    self.log.info("Checking JDL for job: %s" %(job))
    return self.setNextOptimizer(job)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#