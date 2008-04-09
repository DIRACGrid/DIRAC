"""
The Data Management Request contains all the necessary information for
a data management operation
"""

import xml.dom.minidom, time
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.RequestManagementSystem.Client.Request import Request

class DataManagementRequest(Request):

  def __init__(self,request=None):

    # A common set of attributes that define requests.
    self.requestAttributes = ['SubRequestID','TargetSE','Status','Operation','SourceSE','Catalogue','SpaceToken']
    # Possible keys to define the files in the request.
    self.fileAttributes = ['LFN','Size','PFN','GUID','Md5','Addler','Status','Attempt','FileID']
    # Possible keys to define the dataset in the request.
    self.datasetAttributes = ['Handle']

    Request.__init__(self,request)

###############################################################

  def setSubRequestStatus(self,ind,type,status):
    """ Set the operation to Done status
    """
    self.subrequests[type][ind]['Attributes']['Status'] = status

  def getSubRequestAttributes(self,ind,type):
    """ Get the sub-request attributes
    """
    return self.subrequests[type][ind]['Attributes']

  def setSubRequestAttributes(self,ind,type,attributeDict):
    """ Set the sub-request attributes
    """
    self.subrequests[type][ind]['Attributes'] = attributeDict

  def getSubRequestAttributeValue(self,ind,type,attribute):
    """ Get the attribute value associated to a sub-request
    """
    return self.subrequests[type][ind]['Attributes'][attribute]

  def setSubRequestAttributeValue(self,ind,type,attribute,value):
    """ Set the attribute value associated to a sub-request
    """
    if not self.subrequests[type][ind].has_key('Attributes'):
      self.subrequests[type][ind]['Attributes'] = {}
    self.subrequests[type][ind]['Attributes'][attribute] = value

###############################################################

  def getSubRequestNumFiles(self,ind,type):
    """ Get the number of files in the sub-request
    """
    return len(self.subrequests[type][ind]['Files'])

  def getSubRequestFiles(self,ind,type):
    """ Get the files associated to a sub-request
    """
    return self.subrequests[type][ind]['Files'].values()

  def setSubRequestFiles(self,ind,type,files):
    """ Set the files associated to a sub-request
    """

    i = 1
    for file in files:
      self.subrequests[type][ind]['Files']['File%d' % i] = file
      i += 1

  def setSubRequestFileAttributeValue(self,ind,type,lfn,attribute,value):
    """ Set the operation to Done status
    """
    numFiles = self.getSubRequestNumFiles(ind,type)
    for file in self.subrequests[type][ind]['Files'].keys():
      if self.subrequests[type][ind]['Files'][file]['LFN'] == lfn:
        self.subrequests[type][ind]['Files'][file][attribute] = value

  def getSubRequestFileAttributeValue(self,ind,type,lfn,attribute):
    """ Get the file attribute value associated to a LFN and sub-request
    """
    numFiles = self.getSubRequestNumFiles(ind,type)
    for file in self.subrequests[type][ind]['Files'].keys():
      if self.subrequests[type][ind]['Files'][file]['LFN'] == lfn:
        return self.subrequests[type][ind]['Files'][file][attribute]

  def getSubRequestFileAttributes(self,ind,type,lfn):
    """ Get the file attributes associated to a LFN and sub-request
    """
    numFiles = self.getSubRequestNumFiles(ind,type)
    for file in self.subrequests[type][ind]['Files'].keys():
      if self.subrequests[type][ind]['Files'][file]['LFN'] == lfn:
        return self.subrequests[type][ind]['Files'][file]

###############################################################

  def getSubRequestDatasets(self,ind,type):
    """ Get the datasets associated to a sub-request
    """
    return self.subrequests[type][ind]['Datasets'].values()

  def setSubRequestDatasets(self,ind,type,datasets):
    """ Set the datasets associated to a sub-request
    """
    if not self.subrequests[type][ind].has_key('Datasets'):
      self.subrequests[type][ind]['Datasets'] = {}

    i = len(self.subrequests[type][ind]['Datasets'])  + 1
    for dataset in datasets:
       self.subrequests[type][ind]['Datasets']['Dataset%d' % i] = dataset

###############################################################

  def isEmpty(self):
    """ Check if the request contains more operations to be performed
    """

    for stype,slist in self.subrequests.items():
      for tdic in slist:
        for file in tdic['Files'].values():
          if file['Status'] != "Done":
            return 0
    return 1

  def isRequestEmpty(self):
    """ Check whether all sub-requests are complete
    """
    for type in self.subrequests.keys():
      numSubRequests = self.getNumSubRequests(type)
      for ind in range(numSubRequests):
        status = self.getSubRequestAttributeValue(ind,type,'Status')['Value']
        if status != 'Done':
          return 0
    return 1

  def isRequestTypeEmpty(self,type):
    """ Check whether the requests of given type are complete
    """
    if type:
      numSubRequests = self.getNumSubRequests(type)
      for ind in range(numSubRequests):
        status = self.getSubRequestAttributeValue(ind,type,'Status')['Value']
        if status != 'Done':
          return 0
    return 1

  def isSubRequestEmpty(self,ind,type):
    """ Check if the request contains more operations to be performed
    """
    if type:
      for file in self.subrequests[type][ind]['Files'].values():
        if file['Status'] != "Done":
          return 0
    return 1

###############################################################

  def initiateSubRequest(self,type):
    """ Add dictionary to list of requests and return the list index
    """
    defaultDict = {'Attributes':{},'Files':{},'Datasets':{}}
    if not self.subrequests.has_key(type):
      self.subrequests[type] = []
    self.subrequests[type].append(defaultDict)
    length = len(self.subrequests[type])
    return (length-1)

  def addSubRequest(self,type,requestDict):
    """  Add a new sub-requests of specified type
    """
    # Initialise the sub-request
    ind = self.initiateSubRequest(type)

    # Stuff the sub-request with the attributes
    attributeDict = {}
    for key in self.requestAttributes:
      if requestDict['Attributes'].has_key(key):
        attributeDict[key] = requestDict['Attributes'][key]
      else:
        attributeDict[key] = ''
    if not attributeDict['Status']:
      attributeDict['Status'] = 'Waiting'
    if not attributeDict['SubRequestID']:
      attributeDict['SubRequestID'] = makeGuid()
    self.setSubRequestAttributes(ind,type,attributeDict)

    # Stuff the sub-request with the files
    fileDict = {}
    files = requestDict['Files']

    ifile = 1
    for file in files.values():
      for key in self.fileAttributes:
        if not file.has_key(key):
          file[key] = ''
      if not file['Status']:
        file['Status'] = 'Waiting'
      if not file['GUID']:
        file['GUID'] = makeGuid()
      if not file['Attempt']:
        file['Attempt'] = 1
      fileDict['File%d' % ifile] = file
      ifile += 1
    res = self.setSubRequestFiles(ind,type,files.values())

    # Stuff the sub-request with the datasets
    if requestDict.has_key('Datasets'):
      datasets = requestDict['Datasets']
    else:
      datasets = {}
    self.setSubRequestDatasets(ind,type,datasets.values())


###############################################################

#  def dump(self):
#    """ Sent to the logger all the sub-requests in this DM request.
#    """
#    for type in ['Transfer','Register','Removal','Stage']:
#      if type == 'Transfer':
#        reqs = self.transfers
#      if type == 'Register':
#        reqs = self.registers
#      if type == 'Removal':
#        reqs = self.removals
#      if type == 'Stage':
#        reqs = self.stages
#
#      ind = 1
#      for rdic in reqs:
#        gLogger.info( '\n======',type,'=======',ind,'====================' )
#        for key in rdic.keys():
#          if key == 'Attributes':
#            for att,attValue in rdic[key].items():
#              gLogger.info( (att+':').ljust(15),attValue)
#          elif key == 'Files':
#            gLogger.info('Files:'.ljust(15))
#            for file in rdic[key]:
#              gLogger.info(file['LFN'].ljust(15))
#          elif key == 'Datasets':
#            gLogger.info('Datasets:'.ljust(15))
#            datasets = rdic[key]
#            for dataset in datasets:
#              gLogger.info(dataset.ljust(15))
#        ind = ind+1

###############################################################

  def dumpShortToString(self):
    """ Generate summary string for all the sub-requests in this request.
    """
    out = ''
    for rdic in self.subrequests['transfer']:
      out = out + '\nTransfer: %s %s LFNs, % Datasets from %s to %s:\n' % (rdic['Attributes']['Operation'],len(rdic['Files']),len(rdic['Datasets']),rdic['SourceSE'],rdic['Attributes']['TargetSE'])
      statusDict = {}
      for file in rdic['Files'].values():
        status = file['Status']
        if not statusDict.has_key(status):
          statusDict[status]= 0
        statusDict[status] += 1
      for status in statusDict.keys():
        out = out + status +':'+str(statusDict[status])+'\t'

    for rdic in self.subrequests['register']:
      out = out + '\nRegister: %s %s LFNs, % Datasets:\n' % (rdic['Attributes']['Operation'],len(rdic['Files']),len(rdic['Datasets']))
      statusDict = {}
      for file in rdic['Files'].values():
        status = file['Status']
        if not statusDict.has_key(status):
          statusDict[status]= 0
        statusDict[status] += 1
      for status in statusDict.keys():
        out = out + status +':'+str(statusDict[status])+'\t'

    for rdic in self.subrequests['removal']:
      out = out + '\nRemoval: %s %s LFNs, % Datasets from %s:\n' % (rdic['Attributes']['Operation'],len(rdic['Files']),len(rdic['Datasets']),rdic['Attributes']['TargetSE'])
      statusDict = {}
      for file in rdic['Files'].values():
        status = file['Status']
        if not statusDict.has_key(status):
          statusDict[status]= 0
        statusDict[status] += 1
      for status in statusDict.keys():
        out = out + status +':'+str(statusDict[status])+'\t'

    for rdic in self.subrequests['stage']:
      out = out + '\nStage: %s %s LFNs, % Datasets at %s:\n' % (rdic['Attributes']['Operation'],len(rdic['Files']),len(rdic['Datasets']),rdic['Attributes']['TargetSE'])
      statusDict = {}
      for file in rdic['Files'].values():
        status = file['Status']
        if not statusDict.has_key(status):
          statusDict[status]= 0
        statusDict[status] += 1
      for status in statusDict.keys():
        out = out + status +':'+str(statusDict[status])+'\t'

    return S_OK(out)


