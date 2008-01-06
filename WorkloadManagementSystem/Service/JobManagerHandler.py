########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/JobManagerHandler.py,v 1.3 2008/01/06 21:00:49 atsareg Exp $
########################################################################

""" JobManagerHandler is the implementation of the JobManager service
    in the DISET framework
    
    The following methods are available in the Service interface
    
    submitJob()
    rescheduleJob()
    deleteJob()
    killJob()
    
"""

__RCSID__ = "$Id: JobManagerHandler.py,v 1.3 2008/01/06 21:00:49 atsareg Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.ProxyRepositoryDB import ProxyRepositoryDB
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import JobPolicy

# This is a global instance of the JobDB class
jobDB = False
proxyRepository = False

def initializeJobManagerHandler( serviceInfo ):

  global jobDB
  global proxyRepository
  
  jobDB = JobDB()
  proxyRepository = ProxyRepositoryDB()
  return S_OK()

class JobManagerHandler( RequestHandler ):

  ###########################################################################
  types_submitJob = [ StringType, StringType ]
  def export_submitJob( self, JDL, proxy ):
    """ Submit a single job to DIRAC WMS
    """    

    self.policy = JobPolicy()
    result = self.getRemoteCredentials()
    userDN = result['DN']
    userGroup = result['group'] 
      
    # Check job submission permission  
    result = self.policy.getJobPolicy(userDN,userGroup)      
    if result['OK']:
      policyDict = result['Value']
      if not policyDict['Submit']:
        return S_ERROR('Job submission not authorized')
    else:
      return S_ERROR('Failed to get job policies')    

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
    result  = jobDB.insertJobIntoDB(jobID,newJDL)
    if not result['OK']:
      return result
    result  = jobDB.addJobToDB( jobID, JDL=newJDL, ownerDN=userDN, 
                                ownerGroup=userGroup) 
    if not result['OK']:
      return result

    result = jobDB.setJobJDL(jobID,newJDL)
    if not result['OK']:
      return result

    gLogger.info('Job %s added to the JobDB' % str(jobID) )

    resProxy = proxyRepository.storeProxy(proxy,userDN,userGroup)
    if not resProxy['OK']:
      gLogger.error("Failed to store the user proxy for job %s" % jobID)
      return S_ERROR("Failed to store the user proxy for job %s" % jobID)

    return S_OK(jobID)   
    
###########################################################################
  types_invalidateJob = [ IntType ]
  def invalidateJob(self,jobID):
    """ Make job with jobID invalid, e.g. because of the sandbox submission
        errors.
    """    
    
    pass
    
###########################################################################
  def __get_job_list(self,jobInput):
    """ Evaluate the jobInput into a list of ints
    """
   
    if type(jobInput) == IntType:
      return [jobInput]
    if type(jobInput) == StringType:
      try:
        ijob = int(jobInput)
        return [ijob]
      except:
        return []
    if type(jobInput) == ListType:   
      try: 
        ljob = [ int(x) for x in jobInput ] 
        return ljob
      except:
        return []     
   	
    return []

###########################################################################
  def __evaluate_rights(self,jobList,userDN,userGroup,right):
    """ Get access rights to jobID for the user userDN/userGroup 
    """   

    self.policy = JobPolicy()
    self.policy.setJobDB(jobDB)
    validJobList = []
    invalidJobList = []
    nonauthJobList = []
    for jobID in jobList:
      result = self.policy.getUserRightsForJob(jobID,userDN,userGroup)
      if result['OK']:
        if result['Value'][right]:
          validJobList.append(jobID)
        else:
          nonauthJobList.append(jobID)
      else:
        invalidJobList.append(jobID)  
           
    return validJobList,invalidJobList,nonauthJobList               

###########################################################################
  types_rescheduleJob = [ ]
  def export_rescheduleJob(self, jobIDs, proxy = None):
    """  Reschedule a single job. If the optional proxy parameter is given
         it will be used to refresh the proxy in the Proxy Repository
    """  

    jobList = self.__get_job_list(jobIDs)
    if not jobList:
      return S_ERROR('Invalid job specification: '+str(jobIDs))

    result = self.getRemoteCredentials()
    userDN = result['DN']
    userGroup = result['group'] 
        
    validJobList,invalidJobList,nonauthJobList = self.__evaluate_rights(jobList,
                                                                        userDN,
                                                                        userGroup,
                                                                        'Reschedule')
                                                                        
    if validJobList:
      if proxy:
        resProxy = proxyRepository.storeProxy(proxy,userDN,userGroup)
        if not resProxy['OK']:
          gLogger.error("Failed to store the user proxy for job %s" % jobID)

    for jobID in validJobList:  
      result  = jobDB.rescheduleJob( jobID )
      gLogger.debug( str( result ) )
      if not result['OK']:
          return result

    result = S_OK(validJobList)
    if invalidJobList or nonauthJobList:
      result = S_ERROR('Some jobs failed deletion')
      if invalidJobList:
        result['InvalidJobIDs'] = invalidJobList
      if nonauthJobList:
        result['NonauthorizedJobIDs'] = nonauthJobList  
      
    return result 


###########################################################################
  types_deleteJob = [  ]
  def export_deleteJob(self, jobIDs):
    """  Delete jobs specified in the jobIDs list
    """ 
    
    jobList = self.__get_job_list(jobIDs)
    if not jobList:
      return S_ERROR('Invalid job specification: '+str(jobIDs))

    result = self.getRemoteCredentials()
    userDN = result['DN']
    userGroup = result['group']  
        
    validJobList,invalidJobList,nonauthJobList = self.__evaluate_rights(jobList,
                                                                        userDN,
                                                                        userGroup,
                                                                        'Delete') 
            
    for jobID in validJobList:    
      result = jobDB.setJobStatus(jobID,'Deleted','Checking accounting')
      
    result = S_OK(validJobList)
    if invalidJobList or nonauthJobList:
      result = S_ERROR('Some jobs failed deletion')
      if invalidJobList:
        result['InvalidJobIDs'] = invalidJobList
      if nonauthJobList:
        result['NonauthorizedJobIDs'] = nonauthJobList  
      
    return result     
    
###########################################################################
  types_killJob = [  ]
  def export_killJob(self, jobIDs):
    """  Kill a single running job
    """ 
    
    jobList = self.__get_job_list(jobIDs)
    if not jobList:
      return S_ERROR('Invalid job specification: '+str(jobIDs))

    result = self.getRemoteCredentials()
    userDN = result['DN']
    userGroup = result['group']  
    
    validJobList,invalidJobList,nonauthJobList = self.__evaluate_rights(jobList,
                                                                        userDN,
                                                                        userGroup,
                                                                        'Kill')
    for jobID in validJobList:    
      # kill jobID
      pass
      
    result = S_OK(validJobList)
    if invalidJobList or nonauthJobList:
      result = S_ERROR('Some jobs failed deletion')
      if invalidJobList:
        result['InvalidJobIDs'] = invalidJobList
      if nonauthJobList:
        result['NonauthorizedJobIDs'] = nonauthJobList  
      
    return result                                                                        
