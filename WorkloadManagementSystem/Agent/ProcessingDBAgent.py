########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/ProcessingDBAgent.py,v 1.7 2008/12/02 10:07:29 acasajus Exp $
# File :   ProcessingDBAgent.py
# Author : Stuart Paterson
########################################################################

"""   The ProcessingDB Agent queries the file catalogue for specified job input data and adds the
      relevant information to the job optimizer parameters to be used during the
      scheduling decision.

"""

__RCSID__ = "$Id: ProcessingDBAgent.py,v 1.7 2008/12/02 10:07:29 acasajus Exp $"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC.ConfigurationSystem.Client.Config               import gConfig
from DIRAC                                                 import S_OK, S_ERROR

import os, re, time, string

class ProcessingDBAgent(OptimizerModule):

  #############################################################################
  def initializeOptimizer(self):
    """ Initialization of the Agent.
    """
    self.jobTypeToCheck       = 'processing'

    #disabled until available
    self.disableProcDBCheck   = self.am_getCSOption( 'ProcDBFlag', True )
    self.failedMinorStatus    = self.am_getCSOption( 'FailedJobStatus', 'ProcessingDB Error' )

    self.PDBFileCatalog = None
    if not self.disableProcDBCheck:
      dbURL = self.am_getCSOption( 'ProcDBURL', 'processingdb.cern.ch' )
      try:
        from DIRAC.DataManagement.Client.ProcDBCatalogClient import ProcDBCatalogClient
        self.PDBFileCatalog = ProcDBCatalogClient(dbURL)
        self.log.debug("Instantiating ProcessingDB Catalog in mode %s %s %s" % (mode,dbURL) )
      except Exception,x:
        msg = "Failed to create ProcDBFileCatalogClient"
        self.log.fatal( msg, str(x) )
        return S_ERROR( msg )

    return S_OK()

  #############################################################################
  def checkJob( self, job,classAdJob ):
    """This method controls the checking of the job.
    """

    result = self.jobDB.getInputData(job)
    if not result['OK']:
      self.log.error('Failed to get input data from JobdB ', "For job %s: %s" % (job, result['Message'] ) )
      return S_ERROR( "Failed to get input data" )
    if not result['Value']:
      self.log.info('Job %s has no input data requirement' % (job) )
      return self.setNextOptimizer(job)

    self.log.debug('Job %s has an input data requirement ' % (job))
    inputData = result['Value']
    #check job is of correct type
    result = self.getJobType(job)
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    jobType = result['Value']
    if jobType != self.jobTypeToCheck:
      self.log.info('Job %s is of type %s and will be ignored' % (job,jobType))
      return self.setNextOptimizer( job )

    self.log.info('Job %s is of type %s and will be processed' % (job,jobType))
    if not self.disableProcDBCheck:
      procDBResult = self.checkProcDB(job,inputData)
      if not procDBResult['OK']:
        self.log.error(procDBResult['Message'])
        return procDBResult
      result = self.setOptimizerJobInfo(job,self.optimizerName,procDBResult)
      if not result['OK']:
        self.log.error(result['Message'])
        return result
    return self.setNextOptimizer(job)

  #############################################################################
  def getJobType(self,job):
    """This method checks the JobDB for the type of the job being checked.
    """
    self.log.debug("self.jobDB.getJobAttribute("+str(job)+",JobType)")
    result = self.jobDB.getJobAttribute(job,'JobType')
    return result

  #############################################################################
  def checkProcDB(self,job,inputData):
    """This method checks the file catalogue for replica information.  This should
       also add the single site candidate to the optimizer information to be used
       in the scheduling decision.
    """

    lfns = [string.replace(fname,'LFN:','') for fname in inputData]
    start = time.time()
    result = self.PDBFileCatalog.getPfnsByLfnList(lfns)
    timing = time.time() - start
    self.log.info(self.optimizerName+' Lookup Time: %s seconds ' % (timing) )
    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
