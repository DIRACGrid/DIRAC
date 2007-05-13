########################################################################
# $Header $
########################################################################

""" JobReceiverHandler is the implementation of the JobReceiver service
    in the DISET framework
    
    The following methods are available in the Service interface
    
    submitJob()
    rescheduleJob()
    
"""

__RCSID__ = "$Id $"

import types
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.ProxyRepositoryDB import ProxyRepositoryDB

# This is a global instance of the JobDB class
jobDB = False
proxyRepository = False

gLogger.initialize('JobReceiver','/Services/JobReceiver/Test')  

def initializeJobReceiverHandler( serviceInfo ):
  global jobDB
  result = cfgSvc.getOption('DIRAC/Setup')
  if not result['OK']:
    return S_ERROR('Failed to obtain the Configuration information')
  setup = result['Value']
  jobDB = JobDB(setup)
  proxyRepository = ProxyRepositoryDB(setup)
  return S_OK()

class JobReceiverHandler( RequestHandler ):

  ###########################################################################
  types_submitJob = [ StringType, StringType ]
  def export_submitJob( self, JDL, proxy ):
    """ Submit a single job to DIRAC 
    """    

    # Get the new jobID first
    #gActivityClient.addMark( "getJobId" )
    result_jobID  = jobDB.getJobId()
    if not result['OK']:
      return S_ERROR('Failed to acquire a new JobID')
      
    jobID = int(result_jobID['Value'])
    gLogger.info( "Served jobID %s" % jobID )
    # Now add a new job
    #gActivityClient.addMark( "submitJob" )

    gLogger.info( "Submitting job %s" % jobID )

    classAdJob = ClassAd('['+JDL+']')
    classAdJob.insertAttributeInt('JobID',jobID)
    newJDL = classAdJob.asJDL()
    result  = jobDB.addJobToDB( newJDL, jobID, self.sDN )
    if not result['OK']:
        return result
    gLogger.info('Job %s added to the JobDB' % str(jobID) )

    resProxy = proxyRepository.storeProxy(proxy,self.sDN,self.sGroup)
    if not resProxy['OK']:
      gLogger.error("Failed to store the user proxy for job %s" % jobID)
      return S_ERROR("Failed to store the user proxy for job %s" % jobID)

    return S_OK(result['JobID'])        

###########################################################################
  types_rescheduleJob = [ IntType, StringType ]
  def export_rescheduleJob(self, jobID, proxy = None):
    """  Reschedule a single job. If the optional proxy parameter is given
         it will be used to refresh the proxy in the Proxy Repository
    """  

    #gActivityClient.addMark( "rescheduleJob" )

    result  = jobDB.rescheduleJob( jobID )
    gLogger.debug( str( result ) )
    if not result['OK']:
        return result

    if proxy:
      resProxy = proxyRepository.storeProxy(proxy,self.sDN,self.sGroup)
      if not resProxy['OK']:
        gLogger.error("Failed to store the user proxy for job %s" % jobID)
        return S_ERROR("Failed to store the user proxy for job %s" % jobID)

    res = S_OK(result['JobID'])
    res['RescheduleCounter'] = result['RescheduleCounter']
    return res
