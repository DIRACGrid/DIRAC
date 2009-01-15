########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/DiracProduction.py,v 1.52 2009/01/15 09:49:07 paterson Exp $
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

__RCSID__ = "$Id: DiracProduction.py,v 1.52 2009/01/15 09:49:07 paterson Exp $"

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
from DIRAC.Core.Utilities.Time                      import toString
from DIRAC.Core.Security.X509Chain                  import X509Chain
from DIRAC.Core.Security                            import Locations, CS
from DIRAC                                          import gConfig, gLogger, S_OK, S_ERROR

#temporary solution to client side LFN construction
from WorkflowLib.Utilities.Tools import *

COMPONENT_NAME='DiracProduction'

class DiracProduction:

  #############################################################################
  def __init__(self):
    """Instantiates the Workflow object and some default parameters.
    """
    self.log = gLogger.getSubLogger(COMPONENT_NAME)
    #gLogger.setLevel('verbose')
    self.section    = COMPONENT_NAME
    self.scratchDir = gConfig.getValue('/LocalSite/ScratchDir','/tmp')
    self.scratchDir = gConfig.getValue('/LocalSite/ScratchDir','/tmp')
    self.submittedStatus = gConfig.getValue(self.section+'/ProcDBSubStatus','Submitted')
    self.createdStatus = gConfig.getValue(self.section+'/ProcDBCreatedStatus','Created')
    self.defaultOwnerGroup = gConfig.getValue(self.section+'/DefaultOwnerGroup','lhcb_prod')
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
    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.getProductionSummary()
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
    """Returns the metadata associated with a given production ID. Protects against
       LFN: being prepended and different types of production ID.
    """
    if type(productionID)==type(2):
      productionID=int(productionID)
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string, long or int for production ID')

    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.getProductionInfo(int(productionID))
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
    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.getAllProductions()
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
      self._prettyPrint(currentProductions)

    return S_OK(currentProductions)

  #############################################################################
  def getProductionLoggingInfo(self,productionID,printOutput=False):
    """The logging information for the given production is returned.  This includes
       the operation performed, any messages associated with the operation and the
       DN of the production manager performing it.
    """
    if type(productionID)==type(2):
      productionID=long(productionID)
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string, long or int for production ID')

    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.getTransformationLogging(long(productionID))
    if not result['OK']:
      self.log.warn('Could not get transformation logging information for productionID %s' %(productionID))
      return result
    if not result['Value']:
      self.log.warn('No logging information found for productionID %s' %(productionID))
      return S_ERROR('No logging info found')

    if not printOutput:
      return result

    message = ['ProdID'.ljust(int(0.5*self.prodAdj))+'Message'.ljust(3*self.prodAdj)+'DateTime [UTC]'.ljust(self.prodAdj)+'AuthorCN'.ljust(2*self.prodAdj)]
    for line in result['Value']:
      message.append(str(line['TransID']).ljust(int(0.5*self.prodAdj))+line['Message'].ljust(3*self.prodAdj)+toString(line['MessageDate']).ljust(self.prodAdj)+line['AuthorDN'].split('/')[-1].ljust(2*self.prodAdj))

    print '\nLogging summary for productionID '+str(productionID)+'\n\n'+string.join(message,'\n')

    return result

  #############################################################################
  def getProductionSummary(self,productionID=None,printOutput=False):
    """Returns a detailed summary for the productions in the system. If production ID is
       specified, the result is restricted to this value. If printOutput is specified,
       the result is printed to the screen.
    """
    if type(productionID)==type(2):
      productionID=long(productionID)
    if productionID:
      if not type(productionID)==type(long(1)):
        if not type(productionID) == type(" "):
          return self.__errorReport('Expected string or long for production ID')

    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.getProductionSummary()
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
      self._prettyPrint(result['Value'])

    return result

  #############################################################################
  def getProductionApplicationSummary(self,productionID,status=None,minorStatus=None,printOutput=False):
    """Returns an application status summary for the productions in the system. If printOutput is
       specified, the result is printed to the screen.  This queries the WMS
       for the given productionID and provides an up-to-date snapshot of the application status
       combinations and associated WMS JobIDs.
    """
    if type(productionID)==type(2):
      productionID=long(productionID)
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string, long or int for production ID')

    statusDict = self.getProdJobMetadata(productionID,status,minorStatus)
    if not statusDict['OK']:
      self.log.warn('Could not get production metadata information')
      return statusDict

    jobIDs = statusDict['Value'].keys()
    if not jobIDs:
      return S_ERROR('No JobIDs with matching conditions found')

    self.log.verbose('Considering %s jobs with selected conditions' %(len(jobIDs)))
    #now need to get the application status information
    monClient = RPCClient('WorkloadManagement/JobMonitoring',timeout=120)
    result = monClient.getJobsApplicationStatus(jobIDs)
    if not result['OK']:
      self.log.warn('Could not get application status for jobs list')
      return result

    appStatus = result['Value']
#    self._prettyPrint(appStatus)
#    self._prettyPrint(statusDict['Value'])
    #Now format the result.
    summary = {}
    submittedJobs=0
    doneJobs = 0
    for job,atts in statusDict['Value'].items():
      for key,val in atts.items():
        if key=='Status':
          uniqueStatus = val.capitalize()
          if not summary.has_key(uniqueStatus):
            summary[uniqueStatus]={}
          if not summary[uniqueStatus].has_key(atts['MinorStatus']):
            summary[uniqueStatus][atts['MinorStatus']]={}
          if not summary[uniqueStatus][atts['MinorStatus']].has_key(appStatus[job]['ApplicationStatus']):
            summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]={}
            summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['Total']=1
            submittedJobs+=1
            if uniqueStatus=='Done':
              doneJobs+=1
            summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['JobList'] = [job]
          else:
            if not summary[uniqueStatus][atts['MinorStatus']].has_key(appStatus[job]['ApplicationStatus']):
              summary[uniqueStatus][atts['MinorStatus']]={}
              summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]={}
              summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['Total']=1
              submittedJobs+=1
              if uniqueStatus=='Done':
                doneJobs+=1
              summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['JobList'] = [job]
            else:
              current = summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['Total']
              summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['Total'] = current+1
              submittedJobs+=1
              if uniqueStatus=='Done':
                doneJobs+=1
              jobList = summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['JobList']
              jobList.append(job)
              summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['JobList'] = jobList

    if not printOutput:
      result = S_OK()
      if not status and not minorStatus:
        result['Totals'] = {'Submitted':int(submittedJobs),'Done':int(doneJobs)}
      result['Value'] = summary
      return result

    #If a printed summary is requested
    statAdj = int(0.5*self.prodAdj)
    mStatAdj = int(2.0*self.prodAdj)
    totalAdj = int(0.5*self.prodAdj)
    exAdj = int(0.5*self.prodAdj)
    message = '\nJob Summary for ProductionID %s considering status %s' %(productionID,status)
    if minorStatus:
      message+='and MinorStatus = %s' %(minorStatus)

    message += ':\n\n'
    message += 'Status'.ljust(statAdj)+'MinorStatus'.ljust(mStatAdj)+'ApplicationStatus'.ljust(mStatAdj)+'Total'.ljust(totalAdj)+'Example'.ljust(exAdj)+'\n'
    for stat,metadata in summary.items():
      message += '\n'
      for minor,appInfo in metadata.items():
        message += '\n'
        for appStat,jobInfo in appInfo.items():
          message += stat.ljust(statAdj)+minor.ljust(mStatAdj)+appStat.ljust(mStatAdj)+str(jobInfo['Total']).ljust(totalAdj)+str(jobInfo['JobList'][0]).ljust(exAdj)+'\n'

    print message
    #self._prettyPrint(summary)
    if status or minorStatus:
      return S_OK(summary)

    result = self.getProductionProgress(productionID)
    if not result['OK']:
      self.log.warn('Could not get production progress information')
      return result

    if result['Value'].has_key('Created'):
      createdJobs = int(result['Value']['Created'])+submittedJobs
    else:
      createdJobs=submittedJobs

    percSub = int(100*submittedJobs/createdJobs)
    percDone = int(100*doneJobs/createdJobs)
    print '\nCurrent status of production %s:\n' %productionID
    print 'Submitted'.ljust(12)+str(percSub).ljust(3)+'%  ( '+str(submittedJobs).ljust(7)+'Submitted / '.ljust(15)+str(createdJobs).ljust(7)+' Created jobs )'
    print 'Done'.ljust(12)+str(percDone).ljust(3)+'%  ( '+str(doneJobs).ljust(7)+'Done / '.ljust(15)+str(createdJobs).ljust(7)+' Created jobs )'
    result = S_OK()
    result['Totals'] = {'Submitted':int(submittedJobs),'Created':int(createdJobs),'Done':int(doneJobs)}
    result['Value'] = summary
    #self.pPrint(result)
    return result

  #############################################################################
  def getProductionJobSummary(self,productionID,status=None,minorStatus=None,printOutput=False):
    """Returns a job summary for the productions in the system. If printOutput is
       specified, the result is printed to the screen.  This queries the WMS
       for the given productionID and provides an up-to-date snapshot of the job status
       combinations and associated WMS JobIDs.
    """
    if type(productionID)==type(2):
      productionID=long(productionID)
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string, long or int for production ID')

    statusDict = self.getProdJobMetadata(productionID,status,minorStatus)
    if not statusDict['OK']:
      self.log.warn('Could not get production metadata information')
      return statusDict

    #Now format the result.
    summary = {}
    submittedJobs=0
    doneJobs = 0
    for job,atts in statusDict['Value'].items():
      for key,val in atts.items():
        if key=='Status':
          uniqueStatus = val.capitalize()
          if not summary.has_key(uniqueStatus):
            summary[uniqueStatus]={}
          if not summary[uniqueStatus].has_key(atts['MinorStatus']):
            summary[uniqueStatus][atts['MinorStatus']]={}
            summary[uniqueStatus][atts['MinorStatus']]['Total'] = 1
            submittedJobs+=1
            if uniqueStatus=='Done':
              doneJobs+=1
            summary[uniqueStatus][atts['MinorStatus']]['JobList'] = [job]
          else:
            current = summary[uniqueStatus][atts['MinorStatus']]['Total']
            summary[uniqueStatus][atts['MinorStatus']]['Total'] = current+1
            submittedJobs+=1
            if uniqueStatus=='Done':
              doneJobs+=1
            jobList = summary[uniqueStatus][atts['MinorStatus']]['JobList']
            jobList.append(job)
            summary[uniqueStatus][atts['MinorStatus']]['JobList'] = jobList

    if not printOutput:
      result = S_OK()
      if not status and not minorStatus:
        result['Totals'] = {'Submitted':int(submittedJobs),'Done':int(doneJobs)}
      result['Value'] = summary
      return result

    #If a printed summary is requested
    statAdj = int(0.5*self.prodAdj)
    mStatAdj = int(2.0*self.prodAdj)
    totalAdj = int(0.5*self.prodAdj)
    exAdj = int(0.5*self.prodAdj)
    message = '\nJob Summary for ProductionID %s considering' %(productionID)
    if status:
      message+=' Status = %s' %(status)
    if minorStatus:
      message+=' MinorStatus = %s' %(minorStatus)
    if not status and not minorStatus:
      message+=' all status combinations'

    message += ':\n\n'
    message += 'Status'.ljust(statAdj)+'MinorStatus'.ljust(mStatAdj)+'Total'.ljust(totalAdj)+'Example'.ljust(exAdj)+'\n'
    for stat,metadata in summary.items():
      message += '\n'
      for minor,jobInfo in metadata.items():
        message += stat.ljust(statAdj)+minor.ljust(mStatAdj)+str(jobInfo['Total']).ljust(totalAdj)+str(jobInfo['JobList'][0]).ljust(exAdj)+'\n'

    print message
    #self._prettyPrint(summary)
    if status or minorStatus:
      return S_OK(summary)

    result = self.getProductionProgress(productionID)
    if not result['OK']:
      return result

    if result['Value'].has_key('Created'):
      createdJobs = int(result['Value']['Created'])+submittedJobs
    else:
      createdJobs=submittedJobs

    percSub = int(100*submittedJobs/createdJobs)
    percDone = int(100*doneJobs/createdJobs)
    print '\nCurrent status of production %s:\n' %productionID
    print 'Submitted'.ljust(12)+str(percSub).ljust(3)+'%  ( '+str(submittedJobs).ljust(7)+'Submitted / '.ljust(15)+str(createdJobs).ljust(7)+' Created jobs )'
    print 'Done'.ljust(12)+str(percDone).ljust(3)+'%  ( '+str(doneJobs).ljust(7)+'Done / '.ljust(15)+str(createdJobs).ljust(7)+' Created jobs )'
    result = S_OK()
    result['Totals'] = {'Submitted':int(submittedJobs),'Created':int(createdJobs),'Done':int(doneJobs)}
    result['Value'] = summary
    return result

  #############################################################################
  def getProductionSiteSummary(self,productionID,site=None,printOutput=False):
    """Returns a site summary for the productions in the system. If printOutput is
       specified, the result is printed to the screen.  This queries the WMS
       for the given productionID and provides an up-to-date snapshot of the sites
       that jobs were submitted to.
    """
    if type(productionID)==type(2):
      productionID=long(productionID)
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string, long or int for production ID')

    statusDict = self.getProdJobMetadata(productionID,None,None,site)
    if not statusDict['OK']:
      self.log.warn('Could not get production metadata information')
      return statusDict

    summary = {}
    submittedJobs=0
    doneJobs = 0

    for job,atts in statusDict['Value'].items():
      for key,val in atts.items():
        if key=='Site':
          uniqueSite = val
          currentStatus = atts['Status'].capitalize()
          if not summary.has_key(uniqueSite):
            summary[uniqueSite]={}
          if not summary[uniqueSite].has_key(currentStatus):
            summary[uniqueSite][currentStatus]={}
            summary[uniqueSite][currentStatus]['Total'] = 1
            submittedJobs+=1
            if currentStatus=='Done':
              doneJobs+=1
            summary[uniqueSite][currentStatus]['JobList'] = [job]
          else:
            current = summary[uniqueSite][currentStatus]['Total']
            summary[uniqueSite][currentStatus]['Total'] = current+1
            submittedJobs+=1
            if currentStatus=='Done':
              doneJobs+=1
            jobList = summary[uniqueSite][currentStatus]['JobList']
            jobList.append(job)
            summary[uniqueSite][currentStatus]['JobList'] = jobList

    if not printOutput:
      result = S_OK()
      if not site:
        result = self.getProductionProgress(productionID)
        if not result['OK']:
          return result
        createdJobs = result['Value']['Created']
        result['Totals'] = {'Submitted':int(submittedJobs),'Done':int(doneJobs)}
      result['Value'] = summary
      return result

    #If a printed summary is requested
    siteAdj = int(1.0*self.prodAdj)
    statAdj = int(0.5*self.prodAdj)
    totalAdj = int(0.5*self.prodAdj)
    exAdj = int(0.5*self.prodAdj)
    message = '\nSummary for ProductionID %s' %(productionID)
    if site:
      message+=' at Site %s' %(site)
    else:
      message+=' at all Sites'
    message += ':\n\n'
    message += 'Site'.ljust(siteAdj)+'Status'.ljust(statAdj)+'Total'.ljust(totalAdj)+'Example'.ljust(exAdj)+'\n'
    for siteStr,metadata in summary.items():
      message += '\n'
      for stat,jobInfo in metadata.items():
        message += siteStr.ljust(siteAdj)+stat.ljust(statAdj)+str(jobInfo['Total']).ljust(totalAdj)+str(jobInfo['JobList'][0]).ljust(exAdj)+'\n'

    print message
    #self._prettyPrint(summary)
    result = self.getProductionProgress(productionID)

    if not result['OK']:
      return result

    if result['Value'].has_key('Created'):
      createdJobs = int(result['Value']['Created'])+submittedJobs
    else:
      createdJobs=submittedJobs

    percSub = int(100*submittedJobs/createdJobs)
    percDone = int(100*doneJobs/createdJobs)
    if not site:
      print '\nCurrent status of production %s:\n' %productionID
      print 'Submitted'.ljust(12)+str(percSub).ljust(3)+'%  ( '+str(submittedJobs).ljust(7)+'Submitted / '.ljust(15)+str(createdJobs).ljust(7)+' Created jobs )'
      print 'Done'.ljust(12)+str(percDone).ljust(3)+'%  ( '+str(doneJobs).ljust(7)+'Done / '.ljust(15)+str(createdJobs).ljust(7)+' Created jobs )'
    result = S_OK()
    result['Totals'] = {'Submitted':int(submittedJobs),'Created':int(createdJobs),'Done':int(doneJobs)}
    result['Value'] = summary
    return result

  #############################################################################
  def getProductionProgress(self,productionID=None,printOutput=False):
    """Returns the status of jobs as seen by the production management infrastructure.
    """
    if type(productionID)==type(2):
      productionID=long(productionID)
    if productionID:
      if not type(productionID)==type(long(1)):
        if not type(productionID) == type(" "):
          return self.__errorReport('Expected string, long or int for production ID')

    if not productionID:
      result = self.getActiveProductions()
      if not result['OK']:
        return result
      productionID = result['Value'].keys()
    else:
      productionID = [productionID]

    productionID = [ str(x) for x in productionID ]
    self.log.verbose('Will check progress for production(s):\n%s' %(string.join(productionID,', ')))
    progress = {}
    for prod in productionID:
      prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
      #result = prodClient.getJobStats(int(prod))
      #self._prettyPrint(result)
      result = prodClient.getJobWmsStats(int(prod))
      progress[int(prod)] = result['Value']

    if not printOutput:
      return result
    idAdj = int(self.prodAdj)
    statAdj = int(self.prodAdj)
    countAdj = int(self.prodAdj)
    message = 'ProductionID'.ljust(idAdj)+'Status'.ljust(statAdj)+'Count'.ljust(countAdj)+'\n\n'
    for prod,info in progress.items():
      for status,count in info.items():
        message += str(prod).ljust(idAdj)+status.ljust(statAdj)+str(count).ljust(countAdj)+'\n'
      message+='\n'

    print message
    return result

  #############################################################################
#  def getProductionFileMask(self,productionID=None,printOutput=False):
#    """Returns the regular expressions used to define data for productions.
#    """
#    #TODO: write
#    return S_OK()

  #############################################################################
  def production(self,productionID,command,printOutput=False,disableCheck=True):
    """Allows basic production management by supporting the following commands:
       - start : set production status to Active, job submission possible
       - stop : set production status to Stopped, no job submissions
       - automatic: set production submission mode to Automatic, e.g. submission via Agent
       - manual: set produciton submission mode to manual, e.g. dirac-production-submit
    """
    commands = {'start':['Active','Manual'],'stop':['Stopped','Manual'],'automatic':['Active','Automatic'],'manual':['Active','Manual']}
    if type(productionID)==type(2):
      productionID=long(productionID)
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string, long or int for production ID')

    if not type(command)==type(" "):
      return self.__errorReport('Expected string, for command')
    if not command.lower() in commands.keys():
      return self.__errorReport('Expected one of: %s for command string' %(string.join(commands.keys(),', ')))

    self.log.verbose('Requested to change production %s with command "%s"' %(productionID,command.lower().capitalize()))
    if not disableCheck:
      result = self.__promptUser('Do you wish to change production %s with command "%s"? ' %(productionID,command.lower().capitalize()))
      if not result['OK']:
        self.log.info('Action cancelled')
        return S_OK('Action cancelled')

    actions = commands[command]
    self.log.info('Setting production status to %s and submission mode to %s for productionID %s' %(actions[0],actions[1],productionID))
    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.setTransformationStatus(long(productionID), actions[0])
    if not result['OK']:
      self.log.warn('Problem updating transformation status with result:\n%s' %result)
      return result
    self.log.verbose('Setting transformation status to %s successful' %(actions[0]))
    result = prodClient.setTransformationAgentType(long(productionID), actions[1])
    if not result['OK']:
      self.log.warn('Problem updating transformation agent type with result:\n%s' %result)
      return result
    self.log.verbose('Setting transformation agent type to %s successful' %(actions[1]))
    return S_OK('Production %s status updated' %productionID)

  #############################################################################
  def deleteProduction(self,productionID,printOutput=False):
    """ Deletes a production from the production management system ONLY. To be
        used with extreme care after BK replica flags / LFC entries / Logs are
        cleaned up.
    """
    if type(productionID)==type(2):
      productionID=long(productionID)
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string, long or int for production ID')

    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.deleteProduction(productionID)
    if result['OK'] and printOutput:
      print 'Production %s is deleted from the production management system' %productionID

    return result

  #############################################################################
  def productionFileSummary(self,productionID,selectStatus=None,outputFile=None,orderOutput=True,printSummary=False,printOutput=False):
    """ Allows to investigate the input files for a given production transformation
        and provides summaries / selections based on the file status if desired.
    """
    adj = 12
    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    fileSummary = prodClient.getFilesForTransformation(int(productionID),orderOutput)
    if not fileSummary['OK']:
      return fileSummary

    toWrite=''
    totalRecords = 0
    summary = {}
    selected = 0
    if fileSummary['OK']:
      for lfnDict in fileSummary['Value']:
        totalRecords+=1
        record = ''
        recordStatus = ''
        for n,v in lfnDict.items():
          record += str(n)+' = '+str(v).ljust(adj)+' '
          if n=='Status':
            recordStatus=v
            if selectStatus==recordStatus:
              selected+=1
            if summary.has_key(v):
              new = summary[v]+1
              summary[v]=new
            else:
              summary[v]=1

        if outputFile and selectStatus:
          if selectStatus==recordStatus:
            toWrite+=record+'\n'
            if printOutput:
              print record
        elif outputFile:
          toWrite+=record+'\n'
          if printOutput:
            print record
        else:
          if printOutput:
            print record

    if printSummary:
      print '\nSummary for %s files in production %s\n' %(totalRecords,productionID)
      print 'Status'.ljust(adj)+' '+'Total'.ljust(adj)+'Percentage'.ljust(adj)+'\n'
      for n,v in summary.items():
        percentage = int(100*int(v)/totalRecords)
        print str(n).ljust(adj)+' '+str(v).ljust(adj)+' '+str(percentage).ljust(2)+' % '
      print '\n'

    if selectStatus and not selected:
      return S_ERROR('No files were selected for production %s and status "%s"' %(productionID,selectStatus))
    elif selectStatus and selected:
      print '%s / %s files (%s percent) were found for production %s in status "%s"' %(selected,totalRecords,int(100*int(selected)/totalRecords),productionID,selectStatus)

    if outputFile:
      if os.path.exists(outputFile):
        print 'Requested output file %s already exists, please remove this file to continue' %outputFile
        return fileSummary

      fopen = open(outputFile,'w')
      fopen.write(toWrite)
      fopen.close()
      if not selectStatus:
        print 'Wrote %s lines to file %s' %(totalRecords,outputFile)
      else:
        print 'Wrote %s lines to file %s for status "%s"'  %(selected,outputFile,selectStatus)

    return fileSummary

  #############################################################################
  def checkFilesStatus(self,lfns,productionID='',printOutput=False):
    """Checks the given LFN(s) status in the productionDB.  All productions
       are considered by default but can restrict to productionID.
    """
    if type(productionID)==type(2):
      productionID=long(productionID)
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string, long or int for production ID')

    if type(lfns)==type(" "):
      lfns = lfns.replace('LFN:','')
    elif type(lfns)==type([]):
      try:
        lfns = [str(lfnName.replace('LFN:','')) for lfnName in lfns]
      except Exception,x:
        return self.__errorReport(str(x),'Expected strings for LFN(s)')
    else:
      return self.__errorReport('Expected single string or list of strings for LFN(s)')

    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    fileStatus = prodClient.getFileSummary(lfns,long(productionID))
    if printOutput:
      self._prettyPrint(fileStatus['Value'])
    return fileStatus

  #############################################################################
  def setFileStatus(self,lfns,productionID,status,printOutput=False):
    """ Set status for the given files in the lfns list for production specified by its ID
    """

    if type(productionID)==type(2):
      productionID=long(productionID)
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string, long or int for production ID')

    if type(lfns) in types.StringTypes:
      lfnList = [lfns]
    elif type(lfns) == types.ListType:
      lfnList = lfns
    else:
      return self.__errorReport('Expected string or list for LFNs')

    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.setFileStatusForTransformation(productionID,[(status,lfnList)])

    if printOutput:
      if not result['OK']:
        print "Failed to update status for file",lfn
      for lfn,message in result['Value']['Successful'].items():
        print "Successful:",lfn,":",message
      for lfn,message in result['Value']['Failed'].items():
        print "Failed:",lfn,":",message

    return result

  #############################################################################
  def getWMSProdJobID(self,jobID,printOutput=False):
    """This method takes the DIRAC WMS JobID and returns the Production JobID information.
    """
    result = self.diracAPI.attributes(jobID)
    if not result['OK']:
      return result
    if not result['Value'].has_key('JobName'):
      return S_ERROR('Could not establish ProductionID / ProductionJobID, missing JobName')

    wmsJobName = result['Value']['JobName']
    prodID = wmsJobName.split('_')[0]
    prodJobID = wmsJobName.split('_')[1]
    info = {'WMSJobID':jobID,'JobName':wmsJobName,'ProductionID':prodID,'JobID':prodJobID}
    if printOutput:
      self._prettyPrint(info)
    return S_OK(info)

  #############################################################################
  def getProdJobOutputSandbox(self,jobID):
    """Wraps around DIRAC API getOutputSandbox(), takes single jobID or list of jobIDs.
    """
    #TODO: write with wrapper zfilled prod directory
    return self.diracAPI.getOutputSandbox(jobID)

  #############################################################################
  def getProdJobInputSandbox(self,jobID):
    """Wraps around DIRAC API getInputSandbox(), takes single jobID or list of jobIDs.
    """
    #TODO: write with wrapper zfilled prod directory
    return self.diracAPI.getInputSandbox(jobID)

  #############################################################################
  def getProdJobStatus(self,jobID):
    """Wraps around DIRAC API status(), takes single jobID or list of jobIDs.
    """
    return self.diracAPI.status(jobID)

  #############################################################################
  def rescheduleProdJobs(self,jobID):
    """Wraps around DIRAC API reschedule(), takes single jobID or list of jobIDs.
    """
    return self.diracAPI.reschedule(jobID)

  #############################################################################
  def deleteProdJobs(self,jobID):
    """Wraps around DIRAC API delete(), takes single jobID or list of jobIDs.
    """
    #Notification of the production management infrastructure to be added
    return self.diracAPI.delete(jobID)

  #############################################################################
  def getProdJobInfo(self,productionID,jobID,printOutput=False):
    """Retrieve production job information from Production Manager service.
    """
    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    jobInfo = prodClient.getJobInfo(productionID,jobID)
    if not jobInfo['OK']:
      return jobInfo

    if printOutput:
      self._prettyPrint(jobInfo['Value'])
    return jobInfo

  #############################################################################
  def getProdJobSummary(self,jobID,outputFile=None,printOutput=False):
    """Wraps around DIRAC API getJobSummary to provide more detailed information.
    """
    return self.diracAPI.getJobSummary(jobID,outputFile,printOutput)

  #############################################################################
  def getProdJobLoggingInfo(self,jobID):
    """Wraps around DIRAC API getJobLoggingInfo to provide more detailed information.
       Takes single WMS JobID.
    """
    return self.diracAPI.loggingInfo(jobID)

  #############################################################################
  def getProdJobParameters(self,jobID):
    """Wraps around DIRAC API parameters(), takes single jobID or list of jobIDs.
    """
    return self.diracAPI.parameters(jobID)

  #############################################################################
  def selectProductionJobs(self,ProductionID,Status=None,MinorStatus=None,ApplicationStatus=None,Site=None,Owner=None,Date=None):
    """Wraps around DIRAC API selectJobs(). Arguments correspond to the web page
       selections. By default, the date is today.
    """
    return self.diracAPI.selectJobs(Status,MinorStatus,ApplicationStatus,Site,Owner,str(ProductionID).zfill(8),Date)

  #############################################################################
  def submitProduction(self,productionID,numberOfJobs,site=''):
    """Calls the production manager service to retrieve the necessary information
       to construct jobs, these are then submitted via the API.
    """
    if type(productionID)==type(2):
      productionID=long(productionID)
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

    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.getJobsToSubmit(long(productionID),int(numberOfJobs),str(site))
    if not result['OK']:
      return self.__errorReport(result,'Problem while requesting data from ProductionManager')

    submission = self.__createProductionJobs(productionID,result['Value'],numberOfJobs,userID['Value'],site)
    return submission

  #############################################################################
  def extendProduction(self,productionID,numberOfJobs,printOutput=False):
    """ Extend Simulation type Production by number of jobs.
        Usage: extendProduction <ProductionNameOrID> nJobs
    """
    if type(productionID)==type(2):
      productionID=long(productionID)
    if not type(productionID)==type(long(1)):
      if not type(productionID) == type(" "):
        return self.__errorReport('Expected string or long for production ID')

    if type(numberOfJobs) == type(" "):
      try:
        numberOfJobs = int(numberOfJobs)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for number of jobs to submit')

    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    result = prodClient.extendProduction(long(productionID),numberOfJobs)
    if not result['OK']:
      return self.__errorReport(result,'Could not extend production %s by %s jobs' %(productionID,numberOfJobs))

    if printOutput:
      print 'Extended production %s by %s jobs' %(productionID,numberOfJobs)

    return result

  #############################################################################
  def __createProductionJobs(self,prodID,prodDict,number,userID,site=None):
    """Wrapper to submit job to WMS.
    """
    #{'Body':<workflow_xml>,'JobDictionary':{<job_number(int)>:{paramname:paramvalue}}}
    if not prodDict.has_key('Body') or not prodDict.has_key('JobDictionary'):
      return self.__errorReport(prodDict,'Unexpected result from ProductionManager service')

    jobDict = prodDict['JobDictionary']
    if not jobDict:
      self.log.warn('Null job dictionary returned:\n %s' %prodDict)
      return S_ERROR('Null job dictionary returned from ProductionManager service')

    submitted =  []
    failed = []
    xmlString = prodDict['Body']
    #creating a /tmp/guid/ directory for job submission files
    jfilename = self.__createJobDescriptionFile(xmlString)
    prodJob = Job(jfilename)
    self.log.verbose(jobDict)
    jobs_available = len(jobDict)
    inputData = None
    for jobNumber,paramsDict in jobDict.items():
      for paramName,paramValue in paramsDict.items():
        self.log.verbose('ProdID: %s, JobID: %s, ParamName: %s, ParamValue: %s' %(prodID,jobNumber,paramName,paramValue))
        if paramName=='InputData':
          if paramValue:
            self.log.verbose('Setting input data to %s' %paramValue)
            prodJob.setInputData(paramValue)
            inputData = paramValue
        if paramName=='Site':
          if site and not site==paramValue and paramValue.lower()!='any':
            return self.__errorReport('Specified destination site %s does not match allocated site %s' %(site,paramName))
          if paramValue.lower()=='any' and site:
            destsite = site
            self.log.verbose('Setting destination site to %s' %(destsite))
            prodJob.setDestination(destsite)
#          else:
#            destsite = paramValue
#            self.log.verbose('Setting destination site to %s' %(destsite))
#            prodJob.setDestination(destsite)

        if paramName=='TargetSE':
          self.log.verbose('Job is targeted to SE: %s' %(paramValue))
      self.log.verbose('Setting job owner to %s' %(userID))
      prodJob.setOwner(userID)
      prodJob.setOwnerGroup(self.defaultOwnerGroup)
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
      result = self.__getOutputLFNs(prodID,jobNumber,inputData,updatedJob) #prodJob._toXML()
      if result['OK']:
        newProdJob = Job(updatedJob)
        for name,output in result['Value'].items():
          newProdJob._addJDLParameter(name,string.join(output,';'))
        updatedJob = self.__createJobDescriptionFile(newProdJob._toXML())
      else:
        self.log.error('Could not create production LFN',result['Message'])
      newJob = Job(updatedJob)
      self.log.verbose('Final XML file is %s' %updatedJob)
      subResult = self.__submitJob(newJob)
#      subResult = S_ERROR('blocking submission')
      self.log.verbose(subResult)
      prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
      if subResult['OK']:
        jobID = subResult['Value']
        submitted.append(jobID)
        #Now update status in the processing DB
        result = prodClient.setJobStatusAndWmsID(long(prodID),long(jobNumber),self.submittedStatus,str(jobID))
        if not result['OK']:
          self.log.warn('Could not report submitted status to ProcDB for job %s %s %s' %(prodID,jobNumber,jobID))
          self.log.warn(result)
      else:
        failed.append(jobNumber)
        self.log.error('Production Job Submission Failed',subResult['Message'])
        self.log.info('Job submission failed for productionID %s and prodJobID %s setting prodJob status to %s' %(prodID,jobNumber,self.createdStatus))
        result = prodClient.setJobStatus(long(prodID),long(jobNumber),self.createdStatus)
        if not result['OK']:
          self.log.warn(result)

    self.__cleanUp()
    result = S_OK()
    self.log.info('Job Submission Summary: ProdID=%s, Requested=%s, Available=%s, Submitted=%s, Failed=%s' %(prodID,number,jobs_available,len(submitted),len(failed)))
    result['Value'] = {'Successful':submitted,'Failed':failed}
    return result

  #############################################################################
  def __getOutputLFNs(self,productionID,jobID,inputData,jobDescription):
    """ Temporary function that attempts to calculate the output LFN structure
        based on workflow conventions.
    """
    job = Job(jobDescription)
    wfMode = None
    wfConfigVersion = None
    wfMask = None
    wfType = None
    fileTupleList = []
    for p in job.workflow.parameters:
      if p.getName() == "dataType":
        wfMode = p.getValue()
      if p.getName() == "configVersion":
        wfConfigVersion = p.getValue()
      if p.getName() == "outputDataFileMask":
        wfMask = p.getValue()
        if re.search(';',wfMask):
          wfMask = wfMask.split(';')
        else:
          wfMask = [wfMask]
      if p.getName() == 'JobType':
        wfType = p.getValue()

    self.log.verbose('WFMode = %s, WFConfigVersion = %s, WFMask = %s, WFType=%s' %(wfMode,wfConfigVersion,wfMask,wfType))
    if not wfMode or not wfConfigVersion or not wfType:
      return S_ERROR('Insufficient parameters to construct LFN(s)')

    #borrowed from the JobInfoFromXML utility
    code = job.createCode()
    listoutput = []
    for line in code.split("\n"):
      if line.count("listoutput"):
        listoutput += eval(line.split("#")[0].split("=")[-1])

    for info in listoutput:
      fileTupleList.append((info['outputDataName'],info['outputDataType']))

    lfnRoot = ''
    if inputData:
      lfnRoot = getLFNRoot(inputData,wfType)
    else:
      lfnRoot = getLFNRoot('',wfType,wfConfigVersion)

    if not lfnRoot:
      return S_ERROR('LFN root could not be constructed')

    #Get all LFN(s) to both output data and BK lists at this point (fine for BK)
    outputData = []
    bkLFNs = []
    for fileTuple in fileTupleList:
      lfn = makeProductionLfn(str(jobID).zfill(8),lfnRoot,fileTuple,wfMode,str(productionID).zfill(8))
      outputData.append(lfn)
      bkLFNs.append(lfn)

    #Get log file path - unique for all modules
    logPath = makeProductionPath(str(jobID).zfill(8),lfnRoot,'LOG',wfMode,str(productionID).zfill(8),log=True)
    logFilePath = ['%s/%s' %(logPath,str(jobID).zfill(8))]
    logTargetPath = ['%s/%s_%s.tar' %(logPath,str(productionID).zfill(8),str(jobID).zfill(8))]
    #[ aside, why does makeProductionPath not append the jobID itself ????
    #  this is really only used in one place since the logTargetPath is just written to a text file (should be reviewed)... ]

    #Strip output data according to file mask
    if wfMask:
      newOutputData = []
      newBKLFNs = []
      for od in outputData:
        for type in wfMask:
          if re.search('.%s$' %type,od):
            newOutputData.append(od)
      for bk in bkLFNs:
        for type in wfMask:
          if re.search('.%s$' %type,bk):
            newBKLFNs.append(bk)
      outputData = newOutputData
      bkLFNs = newBKLFNs

    if not outputData:
      self.log.info('No output data LFN(s) constructed')
    else:
      self.log.verbose('Created the following output data LFN(s):\n%s' %(string.join(outputData,'\n')))
    self.log.verbose('Log file path is:\n%s' %logFilePath)
    self.log.verbose('Log target path is:\n%s' %logTargetPath)
    if bkLFNs:
      self.log.verbose('BookkeepingLFN(s) are:\n%s' %(string.join(bkLFNs,'\n')))
    jobOutputs = {'ProductionOutputData':outputData,'LogFilePath':logFilePath,'LogTargetPath':logTargetPath,'BookkeepingLFNs':bkLFNs}
    return S_OK(jobOutputs)

  #############################################################################
  def __getCurrentUser(self):
    self.proxy = Locations.getProxyLocation()
    if not self.proxy:
      return self.__errorReport('No proxy found in local environment')
    else:
      self.log.verbose('Current proxy is %s' %self.proxy)

    chain = X509Chain()
    result = chain.loadProxyFromFile( self.proxy )
    if not result[ 'OK' ]:
      return self.__errorReport("Can't load user proxy %s" % self.proxy, result[ 'Message' ] )

    result = chain.getIssuerCert()
    if not result[ 'OK' ]:
      return self.__errorReport( "Can't load user proxy %s" % self.proxy, result[ 'Message' ] )
    issuerCert = result[ 'Value' ]
    dn = issuerCert.getSubjectDN()[ 'Value' ]
    result = CS.getUsernameForDN( dn )
    if not result[ 'OK' ]:
      return self.__errorReport( "Can't get username for dn %s" % dn, result[ 'Message' ] )
    return result

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
  def getProdJobMetadata(self,productionID,status=None,minorStatus=None,site=None):
    """Function to get the WMS job metadata for selected fields. Given a production ID will return
       the current WMS status information for all jobs in that production starting from the creation
       date.
    """
    result = self.getProduction(long(productionID))
    if not result['OK']:
      self.log.warn('Problem getting production metadata for ID %s:\n%s' %(productionID,result))
      return result

    if not result['Value'].has_key('CreationDate'):
      self.log.warn('Could not establish creation date for production %s with metadata:\n%s' %(productionID,result))
      return result
    creationDate = toString(result['Value']['CreationDate']).split()[0]
    result = self.selectProductionJobs(productionID,Status=status,MinorStatus=minorStatus,Site=site,Date=creationDate)
    if not result['OK']:
      self.log.warn('Problem selecting production jobs for ID %s:\n%s' %(productionID,result))
      return result

    jobsList = result['Value']
    return self.diracAPI.status(jobsList)

  #############################################################################
  def __errorReport(self,error,message=None):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error

    self.log.warn(error)
    return S_ERROR(message)

  #############################################################################
  def _prettyPrint(self,object):
    """Helper function to pretty print an object.
    """
    print self.pPrint.pformat(object)

  #############################################################################
  def __promptUser(self,message):
    """Internal function to pretty print an object.
    """
    self.log.info('%s %s' %(message,'[yes/no] : '))
    response = raw_input('%s %s' %(message,'[yes/no] : '))
    responses = ['yes','y','n','no']
    if not response.strip() or response=='\n':
      self.log.info('Possible responses are: %s' %(string.join(responses,', ')))
      response = raw_input('%s %s' %(message,'[yes/no] : '))

    if not response.strip().lower() in responses:
      self.log.info('Problem interpreting input "%s", assuming negative response.' %(response))
      return S_ERROR(response)

    if response.strip().lower()=='y' or response.strip().lower()=='yes':
      return S_OK(response)
    else:
      return S_ERROR(response)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
