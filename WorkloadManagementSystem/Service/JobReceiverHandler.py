########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/Attic/JobReceiverHandler.py,v 1.4 2007/06/27 12:35:06 atsareg Exp $
########################################################################

""" JobReceiverHandler is the implementation of the JobReceiver service
    in the DISET framework
    
    The following methods are available in the Service interface
    
    submitJob()
    rescheduleJob()
    
"""

__RCSID__ = "$Id: JobReceiverHandler.py,v 1.4 2007/06/27 12:35:06 atsareg Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.ProxyRepositoryDB import ProxyRepositoryDB

# This is a global instance of the JobDB class
jobDB = False
proxyRepository = False

def initializeJobReceiverHandler( serviceInfo ):

  global jobDB
  global proxyRepository
  
  jobDB = JobDB()
  proxyRepository = ProxyRepositoryDB()
  return S_OK()

class JobReceiverHandler( RequestHandler ):

  ###########################################################################
  types_submitJob = [ StringType, StringType ]
  def export_submitJob( self, JDL, proxy ):
    """ Submit a single job to DIRAC 
    """    

    # Get the new jobID first
    #gActivityClient.addMark( "getJobId" )
    result_jobID  = jobDB.getJobID()
    if not result_jobID['OK']:
      return S_ERROR('Failed to acquire a new JobID')
      
    jobID = int(result_jobID['Value'])
    gLogger.info( "Served jobID %s" % jobID )
    # Now add a new job
    #gActivityClient.addMark( "submitJob" )

    gLogger.info( "Submitting job %s" % jobID )

    classAdJob = ClassAd('['+JDL+']')
    classAdJob.insertAttributeInt('JobID',jobID)
    newJDL = classAdJob.asJDL()
    result = self.getRemoteCredentials()
    DN = result['DN']
    group = result['group']
    result  = jobDB.addJobToDB( jobID, JDL=newJDL, ownerDN=DN, ownerGroup=group)
    if not result['OK']:
      return result
    gLogger.info('Job %s added to the JobDB' % str(jobID) )

    resProxy = proxyRepository.storeProxy(proxy,DN,group)
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
