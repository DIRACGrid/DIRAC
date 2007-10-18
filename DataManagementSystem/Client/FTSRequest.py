import re, time, commands
from types import *

from DIRAC.Utility.ProductionUtilities                       import *
from DIRAC.Utility.Pfn                                       import pfnparse, pfnunparse
from DIRAC.InformationServices.ConfigurationService.CSClient import cfgSvc
from DIRAC.Utility.Utils import exeCommand
from DIRAC.DataMgmt.Storage.StorageElement                   import StorageElement

class FTSRequest:

  def __init__(self):
    
    self.finalStates = ['Done','Canceled','Failed','Hold','Finished','FinishedDirty']
    self.failedStates = ['Canceled','Failed','Hold','Finished','FinishedDirty']
    self.successfulStates = ['Finished','Done']
    self.fileStates = ['Done','Canceled','Failed','Hold','Active','Finishing','Pending','Ready','Submitted','Waiting','Finished']

    self.fts_pwd = 'lhcbftstest'
    self.isOK = False

    self.completedFiles = []
    self.failedFiles = []
    self.activeFiles = [] 
    self.newlyCompletedFiles = []
    self.newlyFailedFiles = []

    self.ftsGUID = False
    self.ftsServer = False
    self.statusSummary = {}
    self.fileDict = {}

    self.isTerminal = None
    self.requestStatus = None
    self.percentageComplete = None

    self.sourceSE = None 
    self.targetSE = None
    self.spaceToken = None
    
    """
    self.fileDict = fileDict
    if self.fileDict:
      self.lfns = self.fileDict.keys()
      for lfn in self.lfns: 
        if self.fileDict[lfn].has_key('State'):
          fileState = self.fileDict[lfn]['State']
          if fileState == 'Done':
            self.completedFiles.append(lfn)
          elif fileState in self.failedStates:
            self.failedFiles.append(lfn) 
    """
####################################################################
#
#  These are the methods used for submitting FTS transfers
#
  def submit(self):
    """
       Submits to the FTS the set of files specified in the initialisation.
       
       OPERATION: Creates temporary file conaining source and destination SURLs.
                  Resolves from FTS server controlling the desired channel.
                  Submit the FTS request through the CLI (checking the returned GUID).
    """
    result = self.__createSURLPairFile()
    if result['OK']:
      if not self.ftsServer:
        result = self.__resolveFTSEndpoint()
      if result['OK']:
        result = self.__submitFTSTransfer()
        if result['OK']:
          resDict = {'ftsGUID':self.ftsGUID,'ftsServer':self.ftsServer}
          result['Value'] = resDict
        else:
          print result['Message']
      else:
        print result['Message']
    else:
      print result['Message']
    return result        

  def __checkSupportedProtocols(self):
    """
      This method is used to gauge the possibility of performing FTS transfers between two SEs.
      The FTS uses SRM functionality and therefore both SEs must support the SRM protocol.
                                                                                                                                                             
      OPERATION: Both SE elements are initialised using the SE names supplied.
                 A check is performed to see whether the required protocol is availalble at both sites.
    """
    matchedProtocols = []
    supportedProtocols = ['srm']
    for protocol in supportedProtocols:
      if protocol in self.sourceStorage.getProtocols() and protocol in self.targetStorage.getProtocols():
        matchedProtocols.append(protocol)
    if len(matchedProtocols) > 0:
      return S_OK()
    return S_ERROR()

  def __createSURLPairFile(self):
    """
       Create and populate a temporary file containing SURL pairs specified.

       OPERATION: Create temporary file.
                  Populate it with the source and destination SURL pairs.
    """
                                                                                                                                                          
    try:
      tempfile = os.tmpnam()
    except RunTimeWarning:
      pass
    surlFile = open(tempfile,'w')
                                                                                                                                                          
    for lfn in self.fileDict.keys():
      sourceSURL = self.fileDict[lfn]['Source']
      surlDict = pfnparse(sourceSURL)
      surlDict['port']='8443/srm/managerv2?SFN='
      sourceSURL = pfnunparse(surlDict)
                                                                                                                                                          
      targetSURL = self.fileDict[lfn]['Destination']
      surlDict = pfnparse(targetSURL)
      surlDict['port']='8443/srm/managerv2?SFN='
      targetSURL = pfnunparse(surlDict)
                                                                                                                                                          
      surlString = '%s %s\n' % (sourceSURL,targetSURL)
      surlFile.write(surlString)
    surlFile.close()
    self.surlFile = surlFile.name
    return S_OK()

  def __resolveFTSEndpoint(self):
    """
       Resolve which FTS Server is to be used for submission.
       All transfers to and from CERN are managed by the CERN FTS. 
       Otherwise the transfers are handled by the target site's FTS Server.

       OPERATION: Determine from the target and source SE which server to use.
                  Obtain the URL for the server from the CS. 
    """
    if re.search('CERN',self.sourceSE) or re.search('CERN',self.targetSE):
      ep = 'CERN'
    else:
      ep = targetSE.split('-')[0]
                                                                                                                                                          
    try:
      endpoint = cfgSvc.get('FtsEndPoints',ep,None)
    except Exception, x:
      return S_ERROR('Failed to obtain endpoint from CS')
                                                                                                                                                          
    if not endpoint:
      return S_ERROR('Failed to find FTS endpoint for channel supplied')
    else:
      self.ftsServer = endpoint
      return S_OK()

  def __submitFTSTransfer(self):
    """
       Submits the request to the FTS via the CLI which if successful returns a GUID.
       The CLI options supplied are:
         -s  FTS server to submit to.
         -p  Password stored on the MyProxy server for the client DN. 
         -f  Location of the request file.
 
       OPERATION: Constuct the system call to be made and execute it.
                  Check that the returned string is a GUID.
    """
    if self.spaceToken: 
      comm = '/opt/glite/bin/glite-transfer-submit -s %s -p %s -f %s -m myproxy-fts.cern.ch -t %s' % (self.ftsServer,self.fts_pwd,self.surlFile,self.spaceToken)
    else:
      comm = '/opt/glite/bin/glite-transfer-submit -s %s -p %s -f %s -m myproxy-fts.cern.ch' % (self.ftsServer,self.fts_pwd,self.surlFile)
    print comm
    status, output, error, pythonerror = exeCommand(comm)
    #returns a non zero status if error
    if not status == 0:
      return S_ERROR(error)

    guid = output.replace('\n','')
    if self.__isGUID(guid):
      result = S_OK()
      self.ftsGUID = guid
    else:
      result = S_ERROR(error)
    return result
                                                                                                                                                          
  def __isGUID(self, guid):
    """
       Checks whether a supplied GUID is of the correct format.
       The guid is a string of 36 characters long split into 5 parts of length 8-4-4-4-12.

       INPUT:     guid - string to be checked .
       OPERATION: Split the string on '-', checking each part is correct length.
       OUTPUT:    Returns 1 if the supplied string is a GUID.
                  Returns 0 otherwise.
    """
    guidSplit = guid.split('-')
    if len(guid) == 36 \
      and len(guidSplit[0]) == 8 \
        and len(guidSplit[1]) == 4 \
          and len(guidSplit[2]) == 4 \
            and len(guidSplit[3]) ==4 \
              and len(guidSplit[4]) == 12:
      return 1
    else:
      return 0

####################################################################
#
#  These are the methods used for obtaining a summary of the status
#

  def updateSummary(self):
    """
      Obtains summary information on a submitted FTS request.

      OPERATION: Query the FTS server through the CLI for request.
                 Obtains the request status (self.requestStatus).
                 Determines whether state is terminal (self.isTerminal).
                 Calculates percentage complete (self.percentageComplete).
                 Obtains number of files in a given state (self.statusSummary).
    """
    result = self.__getSummary()
    if result['OK']:
      summaryDict = result['Value']

      self.requestStatus = summaryDict['Status']
 
      if self.requestStatus in self.finalStates:
        self.isTerminal = True

      completedFiles = 0
      for state in self.successfulStates: 
        completedFiles += int(summaryDict[state])
      
      totalFiles = float(summaryDict['Files'])
      self.percentageComplete = 100*(completedFiles/totalFiles) 

      for status in self.fileStates:
        if summaryDict[status] != '0':
          self.statusSummary[status] = int(summaryDict[status])
    return result    

  def __getSummary(self):
    """
       Obtains summary of request via the CLI.
       The CLI options supplied are:
         --verbose Gives a break down of the file states
         -s  FTS server to submit to.

       OPERATION: Constuct the system call to be made and execute it.
                  Parse the output to create a dictionary with the key value pairs.
       OUTPUT:    The result['Value'] contains a summary dictionary for the request.
                  It has the following keys relating to the overall request:
                    'Status','Submit time','Priority','Client DN','Request ID','Reason','Files','VOName','Channel'
                  And the following keys with reference to the files state:
                    'Canceled','Failed','Finished','Submitted','Ready','Done','Pending','Waiting','Active','Finishing','Hold'
    """
    if not self.ftsServer:
      return S_ERROR('FTS Server information not supplied with request')
    else:
      comm = '/opt/glite/bin/glite-transfer-status --verbose -s %s %s' % (self.ftsServer,self.ftsGUID)
      status , output , error, pythonerror = exeCommand(comm)
      #returns a non zero status if error
      if not status == 0:
        return S_ERROR(error)

      lines = output.splitlines()
      res = {}
      for line in lines:
        line = line.split(':\t')
        key = line[0].replace('\t','')
        value = line[1].replace('\t','')
        res[key] = value
      result = S_OK()
      result['Value'] = res
      return result

####################################################################
#
#  These are the methods to parse FTS output for full file status
#

  def updateFileStates(self):
    """
      Obtains summary information on a submitted FTS request.
                                                                                                                                                          
      OPERATION: Query the FTS server through the CLI for detailed request information.
                 Updates the status, timing information and failure reason (self.fileDict)
                 Updates newly completed transfers (self.newlyCompletedFiles,self.completedFiles)
                 Updates newly failed transfers (self.newlyFailedFiles,self.failedFiles)
                 Sets active files (self.activeFiles)  
    """
    result = self.__updateRequestDetails()

    if self.requestStatus in self.finalStates:
      self.isTerminal = True

    self.activeFiles = []
    if result['OK']:
      for lfn in self.lfns:
        if self.fileDict[lfn]['State'] in self.successfulStates:
          if lfn not in self.completedFiles:
            self.newlyCompletedFiles.append(lfn)
            self.completedFiles.append(lfn)
        elif self.fileDict[lfn]['State'] in self.failedStates:
          if lfn not in self.failedFiles:
            self.newlyFailedFiles.append(lfn) 
            self.failedFiles.append(lfn)
        else:
          self.activeFiles.append(lfn)       
    self.percentageComplete = 100*(len(self.completedFiles)/float(len(self.lfns)))    
    return result

  def __updateRequestDetails(self):
    """
       Obtains full details of request via the CLI.
       The CLI options supplied are:
         -s  FTS server to submit to.
         -l  To obtain detailed information

       OPERATION: Constuct the system call to be made and execute it.
                  Parse the output to create a dictionary containing the information for each file.
                  For each LFN the following information is obtained (self.fileDict):
                    'Duration','Reason','Retries'
    """
    comm = '/opt/glite/bin/glite-transfer-status -s %s -l %s' % (self.ftsServer,self.ftsGUID)
    status , output , error, pythonerror = exeCommand(comm)
    #returns a non zero status if error
    if not status == 0:
      return S_ERROR(error)
    fileDetails = output.split('\n\n')
    #For each of the files in the request 
    for fileDetail in fileDetails:
      dict = {}
      for line in fileDetail.splitlines():
        if re.search(':',line):
          line = line.replace("'",'')
          line = line.replace("<",'')
          line = line.replace(">",'')
          key = line.split(':',1)[0].strip()
          value = line.split(':',1)[1].strip()
          if key == 'Source' or key == 'Destination':
            value = value.replace(':8443/srm/managerv2?SFN=','') 
          dict[key] = value
        else:
          self.requestStatus = line
      for lfn in self.lfns:
        if re.search(lfn,dict['Source']):
          for key,value in dict.items():
            self.fileDict[lfn][key] = value
    return S_OK()

####################################################################
#
#  These are the set methods to prepare a monitor
#

  def setFTSGUID(self,guid):
    self.ftsGUID = guid

  def setFTSServer(self,server):
    self.ftsServer = server

  def isSummaryQueryReady(self):
    if self.ftsServer and self.ftsGUID:
      result = S_OK()
    else:
      result = S_ERROR()
    return result
  
  def isDetailedQueryReady(self):
    if self.ftsServer and self.ftsGUID and self.lfns:
      result = S_OK()
    else:
      result = S_ERROR()
    return result

####################################################################
#
#  These are the set methods to prepare a submission 
#

  def setSpaceToken(self,token):
    self.spaceToken = token
 
  def setLFNs(self,lfns):
    self.lfns = lfns
    for lfn in self.lfns:
      if not self.fileDict.has_key(lfn):
        self.fileDict[lfn] = {}

  def setSourceSURL(self,lfn,surl):
    self.fileDict[lfn]['Source'] = surl  

  def setDestinationSURL(self,lfn,surl):
    self.fileDict[lfn]['Destination'] = surl

  def getDestinationSURL(self,lfn):
    if self.fileDict.has_key(lfn):
      if self.fileDict[lfn].has_key('Destination'):
        return self.fileDict[lfn]['Destination']

  def setSourceSE(self,se):
    self.sourceSE = se
    self.sourceStorage = StorageElement(self.sourceSE)

  def setTargetSE(self,se):
    self.targetSE = se
    self.targetStorage = StorageElement(self.targetSE)

  def isSubmissionReady(self):
    if self.sourceSE and self.targetSE and self.lfns:
      result = self.__checkSupportedProtocols()
      if not result['OK']:
        result = S_ERROR('SEs do not support SRM')
      for lfn in self.lfns:
        if not self.fileDict[lfn].has_key('Source') or not self.fileDict[lfn].has_key('Destination'):
          errStr = '%s is missing Source or Destination SURL' % lfn 
          result = S_ERROR(errStr)
    else:
      if not self.sourceSE:
        result = S_ERROR('Source SE not supplied')
      elif not self.targetSE:
        result = S_ERROR('Target SE not supplied')
      elif not self.lfns: 
        result = S_ERROR('LFNs not supplied')
    return result

####################################################################
#
#  These are the get methods to obtain metadata on the request
#

  def setFileCompleted(self,lfn):
    self.fileDict[lfn]['State'] = 'Done'
    self.completedFiles.append(lfn)

  def setFileFailed(self,lfn):
    self.fileDict[lfn]['State'] = 'Failed'
    self.failedFiles.append(lfn)

  def getTransferTime(self,lfn):
    return self.fileDict[lfn]['Duration']

  def getCompleted(self):
    return self.completedFiles

  def getNewlyCompleted(self):
    return self.newlyCompletedFiles 

  def getFailed(self):
    return self.failedFiles
 
  def getNewlyFailed(self):
    return self.newlyFailedFiles 

  def isRequestOK(self):
    return self.isOK

  def getRequestStatus(self):
    return self.requestStatus

  def getStatusSummary(self):
    outStr = ''
    for status in self.statusSummary.keys():
      outStr = '%s: %s,' % (status,self.statusSummary[status])
    outStr = outStr.strip(',') 
    return outStr

  def isRequestTerminal(self):
    return self.isTerminal
 
  def getPercentageComplete(self):
    return self.percentageComplete
