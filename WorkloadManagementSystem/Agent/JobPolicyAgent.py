########################################################################
# $HeadURL$
# File :   JobPolicyAgent.py
# Author : Stuart Paterson
########################################################################
"""
  The Job Policy Agent populates all job JDL with default VO parameters
  specified in the DIRAC/VOPolicy CS section.

"""

__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule

class JobPolicyAgent( OptimizerModule ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle      
  """

  #############################################################################
  def checkJob( self, job, classAdJob ):
    """This method controls the checking of the job.
    """

    self.log.info( "Checking JDL for job: %s" % ( job ) )
    return self.setNextOptimizer( job )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
