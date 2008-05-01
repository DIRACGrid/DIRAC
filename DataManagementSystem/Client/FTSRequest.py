from DIRAC  import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.Utilities.File import checkGuid
#from DIRAC.DataManagementSystem.Storage.StorageElement import StorageElement
import re,os

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
    self.lfns = []

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
    # Check that we have all the required params for submission
    res = self.isSubmissionReady()
    if not res['OK']:
      return res
    # Create the file containing the source and destination SURLs
    res = self.__createSURLPairFile()
    if not res['OK']:
      return res
    # Make sure that we have the correct FTS server to submit to
    res = self.__resolveFTSEndpoint()
    if not res['OK']:
      return res
    # Submit the fts request through the CLI
    res = self.__submitFTSTransfer()
    if not res['OK']:
      return res
    resDict = {'ftsGUID':self.ftsGUID,'ftsServer':self.ftsServer}
    return S_OK(resDict)

  def __checkSupportedProtocols(self):
    """
      This method is used to gauge the possibility of performing FTS transfers between two SEs.
      The FTS uses SRM functionality and therefore both SEs must support the SRM protocol.

      OPERATION: Both SE elements are initialised using the SE names supplied.
                 A check is performed to see whether the required protocol is availalble at both sites.
    """
    return S_OK()
    """
    #this should be removed when the StorageElement is ready
    matchedProtocols = []
    supportedProtocols = ['srm']
    for protocol in supportedProtocols:
      if protocol in self.sourceStorage.getProtocols() and protocol in self.targetStorage.getProtocols():
        matchedProtocols.append(protocol)
    if len(matchedProtocols) > 0:
      return S_OK()
    return S_ERROR()
    """

  def __createSURLPairFile(self):
    """
       Create and populate a temporary file containing SURL pairs specified.

       OPERATION: Create temporary file.
                  Populate it with the source and destination SURL pairs.
    """
    try:
      tempfile = os.tmpnam()
    except RuntimeWarning:
      pass
    surlFile = open(tempfile,'w')

    for lfn in self.fileDict.keys():
      sourceSURL = self.fileDict[lfn]['Source']
      res = pfnparse(sourceSURL)
      surlDict = res['Value']
      surlDict['port']='8443/srm/managerv2?SFN='
      res = pfnunparse(surlDict)
      sourceSURL = res['Value']

      targetSURL = self.fileDict[lfn]['Destination']
      res = pfnparse(targetSURL)
      surlDict = res['Value']
      surlDict['port']='8443/srm/managerv2?SFN='
      res = pfnunparse(surlDict)
      targetSURL = res['Value']

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
    t1Sites = ['CNAF','GRIDKA','IN2P3','NIKHEF','PIC','RAL']
    sourceSite = self.sourceSE.split('-')[0].split('_')[0]
    targetSite = self.targetSE.split('-')[0].split('_')[0]
    if (sourceSite == 'CERN') or (targetSite == 'CERN'):
      # one of the two CERN fts servers should be used 
      if (sourceSite in t1Sites) or (targetSite in t1Sites):
	# the transfer is either two or from a tier1			
        ep = 'CERNT1'
      else:
	# the transfer is either two or from a tier2			
        ep = 'CERNT2'
    else:
      # a tier1 fts server should be used
      if (sourceSite in t1Sites) and (targetSite in t1Sites):
        # this is an t1-t1 transfer and should be managed by the target
        ep = targetSite
      elif sourceSite in t1Sites:
        # this is a t1->t2 transfer
        ep = sourceSite 
      else:
        # this is a t2->t1 transfer
        ep = targetSite
 
    try:
      configPath = '/Resources/FTSEndpoints/%s' % ep
      endpointURL = gConfig.getValue(configPath)
      if not endpointURL:
        errStr = "FTSRequest.__resolveFTSEndpoint: Failed to find FTS endpoint, check CS entry for '%s'." % ep
        return S_ERROR(errStr)
      self.ftsServer = endpointURL
      return S_OK(endpointURL)
    except Exception, x:
      return S_ERROR('FTSRequest.__resolveFTSEndpoint: Failed to obtain endpoint details from CS')

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
      comm = 'glite-transfer-submit -s %s -p %s -f %s -m myproxy-fts.cern.ch -t %s' % (self.ftsServer,self.fts_pwd,self.surlFile,self.spaceToken)
    else:
      comm = 'glite-transfer-submit -s %s -p %s -f %s -m myproxy-fts.cern.ch' % (self.ftsServer,self.fts_pwd,self.surlFile)
    res = shellCall(120,comm)
    if not res['OK']:
      return res
    returnCode,output,errStr = res['Value']
    if not returnCode == 0:
      return S_ERROR(errStr)
    guid = output.replace('\n','')
    if not checkGuid(guid):
      return S_ERROR(error)
    self.ftsGUID = guid
    return res

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
    res = self.isSummaryQueryReady()
    if not res['OK']:
      return res
    res = self.__getSummary()
    if res['OK']:
      summaryDict = res['Value']
      # Set the status of the request
      self.requestStatus = summaryDict['Status']
      if self.requestStatus in self.finalStates:
        self.isTerminal = True
      # Calculate the number of files completed
      completedFiles = 0
      for state in self.successfulStates:
        completedFiles += int(summaryDict[state])
      # Calculate the percentage of the request that is completed
      totalFiles = float(summaryDict['Files'])
      self.percentageComplete = 100*(completedFiles/totalFiles)
      # Create the status summary dictionary
      for status in self.fileStates:
        if summaryDict[status] != '0':
          self.statusSummary[status] = int(summaryDict[status])
    return res

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
    comm = 'glite-transfer-status --verbose -s %s %s' % (self.ftsServer,self.ftsGUID)
    res = shellCall(180,comm)
    if not res['OK']:
      return res
    returnCode,output,errStr = res['Value']
    # Returns a non zero status if error
    if not returnCode == 0:
      return S_ERROR(errStr)
    # Parse the output to get a summary dictionary
    lines = output.splitlines()
    summaryDict = {}
    for line in lines:
      line = line.split(':\t')
      key = line[0].replace('\t','')
      value = line[1].replace('\t','')
      summaryDict[key] = value
    return S_OK(summaryDict)

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
    res = self.isDetailedQueryReady()
    if not res['OK']:
      return res

    res = self.__updateRequestDetails()
    if self.requestStatus in self.finalStates:
      self.isTerminal = True

    self.activeFiles = []
    if res['OK']:
      for lfn in self.lfns:
        if self.fileDict[lfn].has_key('State'):
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
    return res

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
    comm = 'glite-transfer-status -s %s -l %s' % (self.ftsServer,self.ftsGUID)
    res = shellCall(180,comm)
    if not res['OK']:
      return res
    returnCode,output,errStr = res['Value']
    # Returns a non zero status if error
    if not returnCode == 0:
      return S_ERROR(errStr)


    """
    output = output.replace('\r','')
    output = output.replace('DESTINATION error during PREPARATION phase: [GENERAL_FAILURE] Not able to find the version of castor in the database Original error was ORA-00904: "SCHEMAVERSION": invalid identifier\n\n','DESTINATION error during PREPARATION phase: [GENERAL_FAILURE] Not able to find the version of castor in the database Original error was ORA-00904: "SCHEMAVERSION": invalid identifier\n')
    output = output.replace(' not found)\n\n',' not found)\n')
    output = output.replace("TRANSFER error during TRANSFER phase: [GRIDFTP] the server sent an error response: 425 425 Can't open data connection. .\n\n",'TRANSFER error during TRANSFER phase: [GRIDFTP] the server sent an error response: 425 425 Cant open data connection.\n')
    output = output.replace("TRANSFER error during TRANSFER phase: [GRIDFTP] the server sent an error response: 451 451 rfio read failure: Connection closed by remote end.\n\n","TRANSFER error during TRANSFER phase: [GRIDFTP] the server sent an error response: 451 451 rfio read failure: Connection closed by remote end.\n")
    """

    requiredKeys = ['Source','Destination','State','Retries','Reason','Duration']
    fileDetails = output.split('\n\n  Source:')
    # For each of the files in the request
    for fileDetail in fileDetails:
      dict = {}
      fileDetail = '  Source:%s' % fileDetail
      for line in fileDetail.splitlines():
        if re.search(':',line):
          line = line.replace("'",'')
          line = line.replace("<",'')
          line = line.replace(">",'')
        key = line.split(':',1)[0].strip()
        if key in requiredKeys:
          value = line.split(':',1)[1].strip()
          if key == 'Source':
            value = value.replace(':8443/srm/managerv2?SFN=','')
          if key == 'Destination':
            value = value.replace(':8443/srm/managerv2?SFN=','')
          dict[key] = value
        else:
          if dict.has_key('Reason'):
            dict['Reason'] = '%s %s' % (dict['Reason'],line)
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

  def getFTSGUID(self):
    return self.ftsGUID

  def setFTSServer(self,server):
    self.ftsServer = server

  def getFTSServer(self):
    return self.ftsServer

  def isSummaryQueryReady(self):
    if self.ftsServer:
      if self.ftsGUID:
         return S_OK()
      else:
        errStr = 'FTSRequest.isSummaryQueryReady: The FTS GUID must be supplied in the FTSRequest.'
        return S_ERROR(errStr)
    else:
      errStr = 'FTSRequest.isSummaryQueryReady: The FTS server must be supplied in the FTSRequest.'
      return S_ERROR(errStr)

  def isDetailedQueryReady(self):
    if self.ftsServer:
      if self.ftsGUID:
        if self.lfns:
          return S_OK()
        else:
          errStr = 'FTSRequest.isDetailedQueryReady: The LFNs must be supplied in the FTSRequest.'
          return S_ERROR(errStr)
      else:
        errStr = 'FTSRequest.isDetailedQueryReady: The FTS GUID must be supplied in the FTSRequest.'
        return S_ERROR(errStr)
    else:
      errStr = 'FTSRequest.isDetailedQueryReady: The FTS server must be supplied in the FTSRequest.'
      return S_ERROR(errStr)

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

  def setLFN(self,lfn):
    self.lfns.append(lfn)
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
    #self.sourceStorage = StorageElement(self.sourceSE)

  def setTargetSE(self,se):
    self.targetSE = se
    #self.targetStorage = StorageElement(self.targetSE)

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
    return int(self.fileDict[lfn]['Duration'])

  def getFailReason(self,lfn):
    return self.fileDict[lfn]['Reason']

  def getRetries(self,lfn):
    return int(self.fileDict[lfn]['Retries'])

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
      outStr = '%s\n%s: %s' % (outStr,status.ljust(10),self.statusSummary[status])
    return outStr

  def isRequestTerminal(self):
    return self.isTerminal

  def getPercentageComplete(self):
    return self.percentageComplete
