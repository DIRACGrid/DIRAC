"""
The Data Management Request contains all the necessary information for
a data management operation
"""

import xml.dom.minidom, time
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.RequestManagementSystem.Client.Request import Request

class DataManagementRequest(Request):

  def __init__(self,request=None,init=True):

    # A common set of attributes that define requests.
    self.requestAttributes = ['SubRequestID','TargetSE','Status','Operation','SourceSE','Catalogue','SpaceToken']
    # Possible keys to define the files in the request.
    self.fileAttributes = ['LFN','Size','PFN','GUID','Md5','Addler','Status','Attempt','FileID']
    # Possible keys to define the dataset in the request.
    self.datasetAttributes = ['Handle']

    Request.__init__(self,request,init)

###############################################################

  def getSubRequestNumFiles(self,ind,type):
    """ Get the number of files in the sub-request
    """
    numFiles = len(self.subrequests[type][ind]['Files'])
    return S_OK(numFiles)

  def getSubRequestFiles(self,ind,type):
    """ Get the files associated to a sub-request
    """
    files = self.subrequests[type][ind]['Files'].values()
    return S_OK(files)

  def setSubRequestFiles(self,ind,type,files):
    """ Set the files associated to a sub-request
    """
    i = 1
    for file in files:
      self.subrequests[type][ind]['Files']['File%d' % i] = file
      i += 1
    return S_OK()

  def setSubRequestFileAttributeValue(self,ind,type,lfn,attribute,value):
    """ Set the operation to Done status
    """
    numFiles = self.getSubRequestNumFiles(ind,type)
    for file in self.subrequests[type][ind]['Files'].keys():
      if self.subrequests[type][ind]['Files'][file]['LFN'] == lfn:
        self.subrequests[type][ind]['Files'][file][attribute] = value
    return S_OK()

  def getSubRequestFileAttributeValue(self,ind,type,lfn,attribute):
    """ Get the file attribute value associated to a LFN and sub-request
    """
    numFiles = self.getSubRequestNumFiles(ind,type)
    for file in self.subrequests[type][ind]['Files'].keys():
      if self.subrequests[type][ind]['Files'][file]['LFN'] == lfn:
        value = self.subrequests[type][ind]['Files'][file][attribute]
        return S_OK(value)
    return S_OK()

  def getSubRequestFileAttributes(self,ind,type,lfn):
    """ Get the file attributes associated to a LFN and sub-request
    """
    numFiles = self.getSubRequestNumFiles(ind,type)
    for file in self.subrequests[type][ind]['Files'].keys():
      if self.subrequests[type][ind]['Files'][file]['LFN'] == lfn:
        attributes = self.subrequests[type][ind]['Files'][file]
        return S_OK(attributes)
    return S_OK()

###############################################################

  def getSubRequestDatasets(self,ind,type):
    """ Get the datasets associated to a sub-request
    """
    datasets = self.subrequests[type][ind]['Datasets'].values()
    return S_OK(datasets)

  def setSubRequestDatasets(self,ind,type,datasets):
    """ Set the datasets associated to a sub-request
    """
    if not self.subrequests[type][ind].has_key('Datasets'):
      self.subrequests[type][ind]['Datasets'] = {}

    i = len(self.subrequests[type][ind]['Datasets'])  + 1
    for dataset in datasets:
      self.subrequests[type][ind]['Datasets']['Dataset%d' % i] = dataset
    return S_OK()

###############################################################
#
#  Request readiness checks specific to the DataManagement request
#  overrides the methods in the Request base class

  def isEmpty(self):
    """ Check if the request contains more operations to be performed
    """

    for stype,slist in self.subrequests.items():
      for tdic in slist:
        for file in tdic['Files'].values():
          if file['Status'] != "Done":
            return S_OK(0)
    return S_OK(1)

  def isSubRequestEmpty(self,ind,type):
    """ Check if the request contains more operations to be performed
    """
    if type:
      for file in self.subrequests[type][ind]['Files'].values():
        if file['Status'] != "Done":
          return S_OK(0)
    return S_OK(1)

###############################################################

  def initiateSubRequest(self,type):
    """ Add dictionary to list of requests and return the list index
    """
    defaultDict = {'Attributes':{},'Files':{},'Datasets':{}}
    if not self.subrequests.has_key(type):
      self.subrequests[type] = []
    self.subrequests[type].append(defaultDict)
    length = len(self.subrequests[type])
    return S_OK(length-1)

  def addSubRequest(self,type,requestDict):
    """  Add a new sub-requests of specified type. Overrides the corresponding
         method of the base class
    """
    # Initialise the sub-request
    ind = self.initiateSubRequest(type)['Value']

    # Stuff the sub-request with the attributes
    attributeDict = {}
    for key in self.requestAttributes:
      if requestDict['Attributes'].has_key(key):
        attributeDict[key] = requestDict['Attributes'][key]
      else:
        attributeDict[key] = ''

    if not attributeDict['Type']:
      attributeDict['Type'] = type
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
