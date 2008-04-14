########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/DiracProduction.py,v 1.15 2008/04/14 16:59:06 paterson Exp $
# File :   DiracProduction.py
# Author : Stuart Paterson
########################################################################

from DIRAC.Core.Base import Script
Script.parseCommandLine()

"""DIRAC Production Management Class (Under Development)

   The DIRAC Production class allows to submit jobs using the
   Production Management System.

   Helper functions are to be documented with example usage.
"""

__RCSID__ = "$Id: DiracProduction.py,v 1.15 2008/04/14 16:59:06 paterson Exp $"

import string, re, os, time, shutil, types, copy
import pprint

from DIRAC.Core.Workflow.Parameter                  import *
from DIRAC.Core.Workflow.Module                     import *
from DIRAC.Core.Workflow.Step                       import *
from DIRAC.Core.Workflow.Workflow                   import *
from DIRAC.Core.Workflow.WorkflowReader             import *
from DIRAC.Interfaces.API.Job                       import Job
from DIRAC.Interfaces.API.Dirac                     import Dirac
from DIRAC.Core.DISET.RPCClient                     import RPCClient
from DIRAC.Core.Utilities.File                      import makeGuid
from DIRAC.Core.Utilities.GridCredentials           import getGridProxy,getVOMSAttributes,getCurrentDN
from DIRAC.Core.Utilities.Time                      import toString
from DIRAC                                          import gConfig, gLogger, S_OK, S_ERROR

COMPONENT_NAME='DiracProduction'

class DiracProduction:

  #############################################################################
  def __init__(self):
    """Instantiates the Workflow object and some default parameters.
    """
    self.log = gLogger.getSubLogger(COMPONENT_NAME)
    gLogger.setLevel('verbose')
    self.section    = COMPONENT_NAME
    self.scratchDir = gConfig.getValue('/LocalSite/ScratchDir','/tmp')
    self.scratchDir = gConfig.getValue('/LocalSite/ScratchDir','/tmp')
    self.submittedStatus = gConfig.getValue(self.section+'/ProcDBSubStatus','Submitted')
    self.createdStatus = gConfig.getValue(self.section+'/ProcDBCreatedStatus','Created')
    self.defaultOwnerGroup = gConfig.getValue(self.section+'/DefaultOwnerGroup','lhcb_prod')
    self.prodClient = RPCClient('ProductionManagement/ProductionManager')
    self.diracAPI = Dirac()
    self.pPrint = pprint.PrettyPrinter()
    self.toCleanUp = []
    self.prodHeaders = {'AgentType':'SubmissionMode','Status':'Status','CreationDate':'Created', \
                        'TransformationName':'Name','Type':'Type'}
    self.prodAdj = 22
    self.proxy = None

  #############################################################################
  def getAllProductions(self,printOutput=False):
    """Returns a dictionary of production IDs and metadata. If printOutput is
       specified, a high-level summary of the productions is printed.
    """
    result = self.prodClient.getProductionSummary()
    if not result['OK']:
      return result

    if not printOutput:
      return result

    adj=self.prodAdj
    headers = self.prodHeaders.values()
    prodDict=result['Value']
    top = ''
    for i in headers:
      top+=i.ljust(adj)
    message = ['ProductionID'.ljust(adj)+top+'\n']
    for prodID,params in prodDict.items():
      line = str(prodID).ljust(adj)
      for key,name in self.prodHeaders.items():
        for n,v in params.items():
          if n==key:
            line+=v.ljust(adj)
      message.append(line)

    print string.join(message,'\n')
    return result

  #############################################################################
  def getProduction(self,productionID,printOutput=False):
    """Returns the metadata associated with a given production ID.
    """
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string or long for production ID')

    result = self.prodClient.getProductionInfo(long(productionID))
    if not result['OK']:
      return result

    if printOutput:
      adj = self.prodAdj
      prodInfo = result['Value']['Value']
      headers = self.prodHeaders.values()
      prodDict=result['Value']
      top = ''
      for i in headers:
        top+=i.ljust(adj)
      message = ['ProductionID'.ljust(adj)+top+'\n']
      #very painful to make this consistent, better improved first on the server side
      message.append(productionID.ljust(adj)+prodInfo['Status'].ljust(adj)+prodInfo['Type'].ljust(adj)+prodInfo['AgentType'].ljust(adj)+toString(prodInfo['CreationDate']).ljust(adj)+prodInfo['Name'].ljust(adj))
      print string.join(message,'\n')

    return S_OK(result['Value']['Value'])

  #############################################################################
  def getActiveProductions(self,printOutput=False):
    """Returns a dictionary of active production IDs and their status, e.g. automatic, manual.
    """
    result = self.prodClient.getAllProductions()
    if not result['OK']:
      return result
    prodList = result['Value']
    currentProductions = {}
    for prodDict in prodList:
      self.log.debug(prodDict)
      if prodDict.has_key('AgentType') and prodDict.has_key('TransID'):
        prodID = prodDict['TransID']
        status = prodDict['AgentType']
        currentProductions[prodID] = status
        if status.lower() == 'automatic':
          self.log.verbose('Found active production %s eligible to submit jobs' %prodID)

    if printOutput:
      self.__prettyPrint(currentProductions)

    return S_OK(currentProductions)

  #############################################################################
  def getProductionSummary(self,productionID=None,printOutput=False):
    """Returns a summary for the productions in the system. If production ID is
       specified, the result is restricted to this value.
    """
    if productionID:
      if not type(productionID)==type(long(1)):
        if not type(productionID) == type(" "):
          return self.__errorReport('Expected string or long for production ID')

    result = self.prodClient.getProductionSummary()
    if not result['OK']:
      return result

    if productionID:
      if result['Value'].has_key(long(productionID)):
        newResult = S_OK()
        newResult['Value']={}
        newResult['Value'][long(productionID)] = result['Value'][long(productionID)]
        result=newResult
      else:
        prods = result['Value'].keys()
        self.log.info('Specified productionID was not found, the list of active productions is:\n%s' %(prods))
        return S_ERROR('Production ID %s was not found' %(productionID))

    if printOutput:
      self.__prettyPrint(result['Value'])

    return result

  #############################################################################
  def getProductionJobSummary(self,productionID=None,printOutput=False):
    """Returns a job summary for the productions in the system. If production ID is
       specified, the result is restricted to this value.
    """
    return S_OK()

  #############################################################################
  def selectProductionJobs(self):
    """Wraps around DIRAC API selectJobs and provides more limited options.
    """
    return S_OK()

  #############################################################################
  def getJobSummary(self):
    """Wraps around DIRAC API getJobSummary and provides more limited options.
    """
    return S_OK()

  #############################################################################
  def submitProduction(self,productionID,numberOfJobs,site=''):
    """Calls the production manager service to retrieve the necessary information
       to construct jobs, these are then submitted via the API.
    """
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string or long for production ID')

    if type(numberOfJobs) == type(" "):
      try:
        numberOfJobs = int(numberOfJobs)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for number of jobs to submit')

    userID = self.__getCurrentUser()
    if not userID['OK']:
      return self.__errorReport(userID,'Could not establish user ID from proxy credential or configuration')

    result = self.prodClient.getJobsToSubmit(long(productionID),int(numberOfJobs),str(site))
    if not result['OK']:
      return self.__errorReport(result,'Problem while requesting data from ProductionManager')

    submission = self.__createProductionJobs(productionID,result['Value'],numberOfJobs,userID['Value'],site)
    return submission

  #############################################################################
  def __createProductionJobs(self,prodID,prodDict,number,userID,site=None):
    """Wrapper to submit job to WMS.
    """
    #{'Body':<workflow_xml>,'JobDictionary':{<job_number(int)>:{paramname:paramvalue}}}
    if not prodDict.has_key('Body') or not prodDict.has_key('JobDictionary'):
      return self.__errorReport(prodDict,'Unexpected result from ProductionManager service')

    submitted =  []
    failed = []
    xmlString = prodDict['Body']
    #creating a /tmp/guid/ directory for job submission files
    jfilename = self.__createJobDescriptionFile(xmlString)
    prodJob = Job(jfilename)
    jobDict = prodDict['JobDictionary']
    self.log.verbose(jobDict)
    for jobNumber,paramsDict in jobDict.items():
      for paramName,paramValue in paramsDict.items():
        self.log.verbose('ProdID: %s, JobID: %s, ParamName: %s, ParamValue: %s' %(prodID,jobNumber,paramName,paramValue))
        if paramName=='InputData':
          self.log.verbose('Setting input data to %s' %paramValue)
          prodJob.setInputData(paramValue)
        if paramName=='Site':
          if site and not site==paramValue:
            return self.__errorReport('Specified destination site %s does not match allocated site %s' %(site,paramName))
          self.log.verbose('Setting destination site to %s' %(paramValue))
          prodJob.setDestination(paramValue)
        if paramName=='TargetSE':
          self.log.verbose('Job is targeted to SE: %s' %(paramValue))
      self.log.verbose('Setting job owner to %s' %(userID))
      prodJob.setOwner(userID)
      jobGroupName = str(prodID).zfill(8)
      self.log.verbose('Adding default job group of %s' %(jobGroupName))
      prodJob.setJobGroup(jobGroupName)
      constructedName = str(prodID).zfill(8)+'_'+str(jobNumber).zfill(8)
      self.log.verbose('Setting job name to %s' %constructedName)
      prodJob.setName(constructedName)
      prodJob._setParamValue('PRODUCTION_ID',str(prodID).zfill(8))
      prodJob._setParamValue('JOB_ID',str(jobNumber).zfill(8))
      ###self.log.debug(prodJob.createCode()) #never create the code, it resolves global vars ;)
      updatedJob = self.__createJobDescriptionFile(prodJob._toXML())
      newJob = Job(updatedJob)
      self.log.verbose('Final XML file is %s' %updatedJob)
      subResult = self.__submitJob(newJob)
      self.log.verbose(subResult)
      if subResult['OK']:
        jobID = subResult['Value']
        submitted.append(jobID)
        #Now update status in the processing DB
        result = self.prodClient.setJobStatusAndWmsID(long(prodID),long(jobNumber),self.submittedStatus,str(jobID))
        if not result['OK']:
          self.log.warn('Could not report submitted status to ProcDB for job %s %s %s' %(prodID,jobNumber,jobID))
          self.log.warn(result)
      else:
        failed.append(jobNumber)
        self.log.warn('Job submission failed for productionID %s and prodJobID %s setting prodJob status to %s' %(prodID,jobNumber,self.createdStatus))
        result = self.prodClient.setJobStatus(long(prodID),long(jobNumber),self.createdStatus)
        if not result['OK']:
          self.log.warn(result)

    self.__cleanUp()
    result = S_OK()
    self.log.info('Job Submission Summary: ProdID=%s, Requested=%s, Submitted=%s, Failed=%s' %(prodID,number,len(submitted),len(failed)))
    result['Value'] = {'Successful':submitted,'Failed':failed}
    return result

  #############################################################################
  def __getCurrentUser(self):
    self.proxy = getGridProxy()
    if not self.proxy:
      return self.__errorReport('No proxy found in local environment')
    else:
      self.log.verbose('Current proxy is %s' %self.proxy)

    nickname = getVOMSAttributes(self.proxy,'nickname')
    if nickname['OK']:
      owner = nickname['Value']
      self.log.verbose('Established user nickname from current proxy ( %s )' %(owner))
      if owner:
        self.log.verbose('Obtained nickname from VOMS proxy => %s' %owner)
        return S_OK(owner)
      else:
        self.log.verbose('Could not get nickname from current proxy credential, trying CS')

    activeDN = getCurrentDN()
    if not activeDN['OK']:
      return self.__errorReport(result,'Could not get current DN from proxy')

    dn = activeDN['Value']
    userKeys = gConfig.getSections('/Users')
    if not userKeys['OK']:
      return self.__errorReport(result,'Could not get current list of DIRAC users')

    currentUser = None
    for user in userKeys['Value']:
      dnDict = gConfig.getOptionsDict('/Users/%s' %(user))
      if dnDict['OK']:
        if dn == dnDict['Value']['DN']:
          currentUser = user
          self.log.verbose('Found user nickname from CS: %s => %s' %(dn,user))

    if not currentUser:
      return self.__errorReport('Could not get nickname for user DN = %s from CS' %(dn))
    else:
      return S_OK(currentUser)

  #############################################################################
  def __createJobDescriptionFile(self,xmlString):
    guid = makeGuid()
    tmpdir = self.scratchDir+'/'+guid
    self.log.verbose('Created temporary directory for preparing submission %s' % (tmpdir))
    os.mkdir(tmpdir)

    jfilename = tmpdir+'/jobDescription.xml'
    jfile=open(jfilename,'w')
    print >> jfile , xmlString
    jfile.close()
    self.toCleanUp.append(tmpdir)
    return jfilename

  #############################################################################
  def __cleanUp(self):
    for i in self.toCleanUp:
      self.log.debug('Removing temporary directory %s' %i)
      if os.path.exists(i):
        shutil.rmtree(i)
    return S_OK()

  #############################################################################
  def __submitJob(self,prodJob):
    """Wrapper to submit job to WMS.
    """
    self.log.verbose('Attempting to submit job to WMS')
    submitted = self.diracAPI.submit(prodJob)
    if not submitted['OK']:
      self.log.warn('Problem during submission of job')
    return submitted

  #############################################################################
  def __errorReport(self,error,message=None):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error

    self.log.warn(error)
    return S_ERROR(message)

  #############################################################################
  def __prettyPrint(self,object):
    """Internal function to pretty print an object.
    """
    print self.pPrint.pformat(object)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
