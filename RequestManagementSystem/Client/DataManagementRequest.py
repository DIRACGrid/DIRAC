"""
The Data Management Request contains all the necessary information for
a data management operation
"""

import xml.dom.minidom, time
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC import gLogger, S_OK, S_ERROR

class DataManagementRequest:

  def __init__(self,request=None):

    # A common set of attributes that define requests.
    self.requestAttributes = ['SubRequestID','TargetSE','Status','Operation','SourceSE','Catalogue','SpaceToken']
    # Possible keys to define the files in the request.
    self.fileAttributes = ['LFN','Size','PFN','GUID','Md5','Addler','Status','Attempt','FileID']
    # Possible keys to define the dataset in the request.
    self.datasetAttributes = ['Handle']

    # These contain lists of all the type of sub requests in the current request.
    self.transfers = []
    self.registers = []
    self.removals = []
    self.stages = []

    self.jobid = None
    self.ownerDN = ''
    self.date = time.strftime('%Y-%m-%d %H:%M:%S')
    self.mode = ''
    self.dirac_instance = ''
    self.requestid = None
    self.requestname = None

    if request:
      dom = xml.dom.minidom.parseString(request)
      header = dom.getElementsByTagName('Header')[0]
      self.jobid = header.getAttribute('JobID')
      self.ownerDN = header.getAttribute('OwnerDN')
      self.date = header.getAttribute('Date')
      self.mode = header.getAttribute('Mode')
      self.dirac_instance = header.getAttribute('DiracInstance')
      self.requestid = header.getAttribute('RequestID')
      self.requestname = header.getAttribute('RequestName')

      if dom.getElementsByTagName('TRANSFER_REQUEST'):
        for subRequest in dom.getElementsByTagName('TRANSFER_REQUEST'):
          result = self.__parseSubRequest(subRequest)
          if result['OK']:
            self.addSubRequest(result['Value'],'transfer')

      if dom.getElementsByTagName('REGISTER_REQUEST'):
        for subRequest in dom.getElementsByTagName('REGISTER_REQUEST'):
          result = self.__parseSubRequest(subRequest)
          if result['OK']:
            self.addSubRequest(result['Value'],'register')

      if dom.getElementsByTagName('REMOVAL_REQUEST'):
        for subRequest in dom.getElementsByTagName('REMOVAL_REQUEST'):
          result = self.__parseSubRequest(subRequest)
          if result['OK']:
            self.addSubRequest(result['Value'],'removal')

      if dom.getElementsByTagName('STAGE_REQUEST'):
        for subRequest in dom.getElementsByTagName('STAGE_REQUEST'):
          result = self.__parseSubRequest(subRequest)
          if result['OK']:
            self.addSubRequest(result['Value'],'stage')

###############################################################

  def setCurrentDate(self):
    """ Set the request date to the current date and time
    """
    self.date = time.strftime('%Y-%m-%d %H:%M:%S')

  def getCurrentDate(self):
    """ Get the date the request was set
    """
    return self.date

  def setRequestName(self,requestName):
    """ Set the request name
    """
    self.requestname = requestName

  def getRequestName(self):
    """ Get the request name
    """
    return self.requestname

  def setRequestID(self,requestID):
    """ Set the request ID
    """
    self.requestid = requestID

  def getRequestID(self):
    """ Get the request ID
    """
    return self.requestid

  def setJobID(self,jobid):
    """ Set the associated Job ID
    """
    self.jobid = jobid

  def getJobID(self):
    """ Get the request job ID
    """
    return self.jobid

  def setOwnerDN(self,ownerDN):
    """ Set the request owner DN
    """
    self.ownerDN = ownerDN

  def getOwnerDN(self):
    """ get the request owner DN
    """
    return self.ownerDN

  def setMode(self,mode):
    """ Set the DIRAC WMS mode ( instance )
    """
    self.mode = mode

  def getMode(self):
    """ Get the DIRAC WMS mode ( instance )
    """
    return self.mode

  def setDiracInstance(self,instance):
    """ Set the DIRAC WMS mode ( instance )
    """
    self.dirac_instance = instance

  def getDiracInstance(self):
    """ Get the DIRAC WMS mode ( instance )
    """
    return self.dirac_instance

  def setCreationTime(self,dateTime):
    """ Set the creation time of the request
    """
    self.date = dateTime

###############################################################

  def getSubRequest(self,ind,type):
    """ Get the sub-request as specified by its index
    """
    if type == 'transfer':
      return S_OK(self.transfers[ind])
    elif type == 'register':
      return S_OK(self.registers[ind])
    elif type == 'removal':
      return S_OK(self.removals[ind])
    elif type == 'stage':
      return S_OK(self.stages[ind])
    else:
      return S_ERROR()

###############################################################

  def getNumSubRequests(self,type):
    """ Get the number of sub-requests for a given request type
    """
    if type == 'transfer':
      return S_OK(len(self.transfers))
    elif type == 'register':
      return S_OK(len(self.registers))
    elif type == 'removal':
      return S_OK(len(self.removals))
    elif type == 'stage':
      return S_OK(len(self.stages))
    else:
      return S_ERROR()

###############################################################

  def setSubRequestStatus(self,ind,type,status):
    """ Set the operation to Done status
    """
    if type == 'transfer':
      self.transfers[ind]['Attributes']['Status'] = status
    if type == 'register':
      self.registers[ind]['Attributes']['Status'] = status
    if type == 'removal':
      self.removals[ind]['Attributes']['Status'] = status
    if type == 'stage':
      self.stages[ind]['Attributes']['Status'] = status

  def getSubRequestAttributes(self,ind,type):
    """ Get the sub-request attributes
    """
    if type == 'transfer':
      subRequestAttributes = self.transfers[ind]['Attributes']
    if type == 'register':
      subRequestAttributes = self.registers[ind]['Attributes']
    if type == 'removal':
      subRequestAttributes = self.removals[ind]['Attributes']
    if type == 'stage':
      subRequestAttributes = self.stages[ind]['Attributes']
    return S_OK(subRequestAttributes)

  def setSubRequestAttributes(self,ind,type,attributeDict):
    """ Set the sub-request attributes
    """
    if type == 'transfer':
      self.transfers[ind]['Attributes'] = attributeDict
    if type == 'register':
      self.registers[ind]['Attributes'] = attributeDict
    if type == 'removal':
      self.removals[ind]['Attributes'] = attributeDict
    if type == 'stage':
      self.stages[ind]['Attributes'] = attributeDict
    return S_OK()

  def getSubRequestAttributeValue(self,ind,type,attribute):
    """ Get the attribute value associated to a sub-request
    """
    if type == 'transfer':
      value = self.transfers[ind]['Attributes'][attribute]
    if type == 'register':
      value = self.registers[ind]['Attributes'][attribute]
    if type == 'removal':
      value = self.removals[ind]['Attributes'][attribute]
    if type == 'stage':
      value = self.stages[ind]['Attributes'][attribute]
    return S_OK(value)

  def setSubRequestAttributeValue(self,ind,type,attribute,value):
    """ Set the attribute value associated to a sub-request
    """
    if type == 'transfer':
      if not self.transfers[ind].has_key('Attributes'):
        self.transfers[ind]['Attributes'] = {}
      self.transfers[ind]['Attributes'][attribute] = value
    if type == 'register':
      if not self.registers[ind].has_key('Attributes'):
        self.registers[ind]['Attributes'] = {}
      self.registers[ind]['Attributes'][attribute] = value
    if type == 'removal':
      if not self.removals[ind].has_key('Attributes'):
        self.removals[ind]['Attributes'] = {}
      self.removals[ind]['Attributes'][attribute] = value
    if type == 'stage':
      if not self.stages[ind].has_key('Attributes'):
        self.stages[ind]['Attributes'] = {}
      self.stages[ind]['Attributes'][attribute] = value
    return S_OK()

###############################################################

  def getSubRequestNumFiles(self,ind,type):
    """ Get the number of files in the sub-request
    """
    if type == 'transfer':
      numFiles = len(self.transfers[ind]['Files'])
    if type == 'register':
      numFiles = len(self.registers[ind]['Files'])
    if type == 'removal':
      numFiles = len(self.removals[ind]['Files'])
    if type == 'stage':
      numFiles = len(self.stages[ind]['Files'])
    return S_OK(numFiles)

  def getSubRequestFiles(self,ind,type):
    """ Get the files associated to a sub-request
    """
    if type == 'transfer':
      files = self.transfers[ind]['Files']
    if type == 'register':
      files = self.registers[ind]['Files']
    if type == 'removal':
      files = self.removals[ind]['Files']
    if type == 'stage':
      files = self.stages[ind]['Files']
    return S_OK(files)

  def setSubRequestFiles(self,ind,type,files):
    """ Set the files associated to a sub-request
    """
    if type == 'transfer':
      self.transfers[ind]['Files'] = files
    if type == 'register':
      self.registers[ind]['Files'] = files
    if type == 'removal':
      self.removals[ind]['Files'] = files
    if type == 'stage':
      self.stages[ind]['Files'] = files
    return S_OK()

  def setSubRequestFileAttributeValue(self,ind,type,lfn,attribute,value):
    """ Set the operation to Done status
    """
    if type == 'transfer':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.transfers[ind]['Files'][file]['LFN'] == lfn:
          self.transfers[ind]['Files'][file][attribute] = value
    if type == 'register':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.registers[ind]['Files'][file]['LFN'] == lfn:
          self.registers[ind]['Files'][file][attribute] = value
    if type == 'removal':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.removals[ind]['Files'][file]['LFN'] == lfn:
          self.removals[ind]['Files'][file][attribute] = value
    if type == 'stage':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.stages[ind]['Files'][file]['LFN'] == lfn:
          self.stages[ind]['Files'][file][attribute] = value

  def getSubRequestFileAttributeValue(self,ind,type,lfn,attribute):
    """ Get the file attribute value associated to a LFN and sub-request
    """
    if type == 'transfer':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.transfers[ind]['Files'][file]['LFN'] == lfn:
          value = self.transfers[ind]['Files'][file][attribute]
    if type == 'register':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.registers[ind]['Files'][file]['LFN'] == lfn:
          value = self.registers[ind]['Files'][file][attribute]
    if type == 'removal':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.removals[ind]['Files'][file]['LFN'] == lfn:
          value = self.removals[ind]['Files'][file][attribute]
    if type == 'stage':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.stages[ind]['Files'][file]['LFN'] == lfn:
          value = self.stages[ind]['Files'][file][attribute]
    return S_OK(value)

  def getSubRequestFileAttributes(self,ind,type,lfn):
    """ Get the file attributes associated to a LFN and sub-request
    """
    if type == 'transfer':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.transfers[ind]['Files'][file]['LFN'] == lfn:
          attributes = self.transfers[ind]['Files'][file]
    if type == 'register':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.registers[ind]['Files'][file]['LFN'] == lfn:
          attributes = self.registers[ind]['Files'][file]
    if type == 'removal':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.removals[ind]['Files'][file]['LFN'] == lfn:
          attributes = self.removals[ind]['Files'][file]
    if type == 'stage':
      res = self.getSubRequestNumFiles(ind,type)
      numFiles = res['Value']
      for file in range (numFiles):
        if self.stages[ind]['Files'][file]['LFN'] == lfn:
          attributes = self.stages[ind]['Files'][file]
    return S_OK(attributes)

###############################################################

  def getSubRequestDatasets(self,ind,type):
    """ Get the datasets associated to a sub-request
    """
    if type == 'transfer':
      datasets = self.transfers[ind]['Datasets']
    if type == 'register':
      datasets = self.registers[ind]['Datasets']
    if type == 'removal':
      datasets = self.removals[ind]['Datasets']
    if type == 'stage':
      datasets = self.stages[ind]['Datasets']
    return S_OK(datasets)

  def setSubRequestDatasets(self,ind,type,datasets):
    """ Set the datasets associated to a sub-request
    """
    if type == 'transfer':
      if not self.transfers[ind].has_key('Datasets'):
        self.transfers[ind]['Datasets'] = []
      self.transfers[ind]['Datasets'].extend(datasets)
    if type == 'register':
      if not self.registers[ind].has_key('Datasets'):
        self.registers[ind]['Datasets'] = []
      self.registers[ind]['Datasets'].extend(datasets)
    if type == 'removal':
      if not self.removals[ind].has_key('Datasets'):
        self.removals[ind]['Datasets'] = []
      self.removals[ind]['Datasets'].extend(datasets)
    if type == 'stage':
      if not self.stages[ind].has_key('Datasets'):
        self.stages[ind]['Datasets'] = []
      self.stages[ind]['Datasets'].extend(datasets)
    return S_OK()

###############################################################

  def isEmpty(self):
    """ Check if the request contains more operations to be performed
    """
    for tdic in self.transfers:
      for file in tdic['Files']:
        if file['Status'] != "Done":
          return S_OK(0)
    for tdic in self.registers:
      for file in tdic['Files']:
        if file['Status'] != "Done":
          return S_OK(0)
    for tdic in self.removals:
      for file in tdic['Files']:
        if file['Status'] != "Done":
          return S_OK(0)
    for tdic in self.stages:
      for file in tdic['Files']:
        if file['Status'] != "Done":
          return S_OK(0)
    return S_OK(1)

###############################################################
  def initiateSubRequest(self,type):
    """ Add dictionary to list of requests and return the list index
    """
    defaultDict = {'Attributes':{},'Files':[],'Datasets':[]}
    if type == 'transfer':
      self.transfers.append(defaultDict)
      length = len(self.transfers)
    if type == 'register':
      self.registers.append(defaultDict)
      length = len(self.registers)
    if type == 'removal':
      self.removals.append(defaultDict)
      length = len(self.removals)
    if type == 'stage':
      self.stages.append(defaultDict)
      length = len(self.stages)
    ind = length-1
    return S_OK(ind)

  def addSubRequest(self,requestDict,type):
    """  Add a new sub-requests of specified type
    """
    # Initialise the sub-request
    res = self.initiateSubRequest(type)
    ind = res['Value']

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
    res = self.setSubRequestAttributes(ind,type,attributeDict)

    # Stuff the sub-request with the files
    fileList = []
    files = requestDict['Files']
    for file in files:
      for key in self.fileAttributes:
        if not file.has_key(key):
          file[key] = ''
      if not file['Status']:
        file['Status'] = 'Waiting'
      if not file['GUID']:
        file['GUID'] = makeGuid()
      if not file['Attempt']:
        file['Attempt'] = 1
      fileList.append(file)
    res = self.setSubRequestFiles(ind,type,files)

    # Stuff the sub-request with the datasets
    if requestDict.has_key('Datasets'):
      datasets = requestDict['Datasets']
    else:
      datasets = []
    res = self.setSubRequestDatasets(ind,type,datasets)

###############################################################

  def dump(self):
    """ Sent to the logger all the sub-requests in this DM request.
    """
    for type in ['Transfer','Register','Removal','Stage']:
      if type == 'Transfer':
        reqs = self.transfers
      if type == 'Register':
        reqs = self.registers
      if type == 'Removal':
        reqs = self.removals
      if type == 'Stage':
        reqs = self.stages

      ind = 1
      for rdic in reqs:
        gLogger.info( '\n======',type,'=======',ind,'====================' )
        for key in rdic.keys():
          if key == 'Attributes':
            for att,attValue in rdic[key].items():
              gLogger.info( (att+':').ljust(15),attValue)
          elif key == 'Files':
            gLogger.info('Files:'.ljust(15))
            for file in rdic[key]:
              gLogger.info(file['LFN'].ljust(15))
          elif key == 'Datasets':
            gLogger.info('Datasets:'.ljust(15))
            datasets = rdic[key]
            for dataset in datasets:
              gLogger.info(dataset.ljust(15))
        ind = ind+1

###############################################################

  def dumpShortToString(self):
    """ Generate summary string for all the sub-requests in this request.
    """
    out = ''
    for rdic in self.transfers:
      out = out + '\nTransfer: %s %s LFNs, % Datasets from %s to %s:\n' % (rdic['Attributes']['Operation'],len(rdic['Files']),len(rdic['Datasets']),rdic['SourceSE'],rdic['Attributes']['TargetSE'])
      statusDict = {}
      for file in rdic['Files']:
        status = file['Status']
        if not statusDict.has_key(status):
          statusDict[status]= 0
        statusDict[status] += 1
      for status in statusDict.keys():
        out = out + status +':'+str(statusDict[status])+'\t'

    for rdic in self.registers:
      out = out + '\nRegister: %s %s LFNs, % Datasets:\n' % (rdic['Attributes']['Operation'],len(rdic['Files']),len(rdic['Datasets']))
      statusDict = {}
      for file in rdic['Files']:
        status = file['Status']
        if not statusDict.has_key(status):
          statusDict[status]= 0
        statusDict[status] += 1
      for status in statusDict.keys():
        out = out + status +':'+str(statusDict[status])+'\t'

    for rdic in self.removals:
      out = out + '\nRemoval: %s %s LFNs, % Datasets from %s:\n' % (rdic['Attributes']['Operation'],len(rdic['Files']),len(rdic['Datasets']),rdic['Attributes']['TargetSE'])
      statusDict = {}
      for file in rdic['Files']:
        status = file['Status']
        if not statusDict.has_key(status):
          statusDict[status]= 0
        statusDict[status] += 1
      for status in statusDict.keys():
        out = out + status +':'+str(statusDict[status])+'\t'

    for rdic in self.stages:
      out = out + '\nStage: %s %s LFNs, % Datasets at %s:\n' % (rdic['Attributes']['Operation'],len(rdic['Files']),len(rdic['Datasets']),rdic['Attributes']['TargetSE'])
      statusDict = {}
      for file in rdic['Files']:
        status = file['Status']
        if not statusDict.has_key(status):
          statusDict[status]= 0
        statusDict[status] += 1
      for status in statusDict.keys():
        out = out + status +':'+str(statusDict[status])+'\t'

    return S_OK(out)

###############################################################

  def toXML(self):
    """ Output the request (including all sub-requests) to XML.
    """

    out =  '<?xml version="1.0" encoding="UTF-8" ?>\n\n'
    out = out + '<DATA_MANAGEMENT_REQUEST>\n\n'

    attributes = ''
    if self.jobid:
      attributes = attributes + ' JobID="'+self.jobid+'" '
    else:
      attributes = attributes + ' JobID="0" '
    if self.requestid:
      attributes = attributes + ' RequestID="'+str(self.requestid)+'" '
    if self.requestname:
      attributes = attributes + ' RequestName="'+self.requestname+'" '
    if self.ownerDN:
      attributes = attributes + ' OwnerDN="'+self.ownerDN+'" '
    if self.date:
      attributes = attributes + ' Date="'+str(self.date)+'" '
    if self.mode:
      attributes = attributes + ' Mode="'+self.mode+'" '
    if self.dirac_instance:
      attributes = attributes + ' DiracInstance="'+self.dirac_instance+'" '

    out = out + '<Header '+attributes+' />\n\n'

    for type in ['transfer','register','removal','stage']:
      res = self.getNumSubRequests(type)
      if not res['OK']:
        return res
      for ind in range(res['Value']):
        res = self.__createSubRequestXML(ind,type)
        if not res['OK']:
          return res
        outStr = res['Value']
        out = '%s%s\n\n' % (out,outStr)

    out = '%s</DATA_MANAGEMENT_REQUEST>\n' % out
    return S_OK(out)

###############################################################

  def toFile(self,fname):
    res = self.toXML()
    if not res['OK']:
      return res
    reqfile = open(fname,'w')
    reqfile.write(res['Value'])
    reqfile.close()
    return S_OK()

###############################################################

  def __parseSubRequest(self,subRequest):
    try:
      requestDict = {}
      """ Get all the attributes assigned to the sub-request.
          These define the operations that will be performed.
      """
      requestDict['Attributes'] = {}
      for attribute in subRequest.attributes.keys():
        requestDict['Attributes'][attribute] = subRequest.getAttribute(attribute)

      """ Obtain all the files which are detailed in the sub-request. """
      files = subRequest.getElementsByTagName('File')
      requestDict['Files'] = []
      for file in files:
        """ Each file tag contains attributes specific to each file. """
        fileAttributes = file.attributes.keys()
        attributesDict = {}
        for fileAttribute in fileAttributes:
          attributesDict[fileAttribute] = file.getAttribute(fileAttribute)
        requestDict['Files'].append(attributesDict)

      """ Obtain all the datasets that are detailed in the sub-request """
      datasets = subRequest.getElementsByTagName('Dataset')
      requestDict['Datasets'] = []
      for dataset in datasets:
        """ Each dataset tag must contain a handle tag. """
        datasetAttributes = dataset.attributes.keys()
        if 'Handle' in datasetAttributes:
          requestDict['Datasets'].append(dataset.getAttribute('Handle'))
        else:
          return S_ERROR('No Handle supplied for dataset in sub-request')

      result = S_OK()
      result['Value'] = requestDict
      return result
    except Exception, x:
      errorStr = 'Failed while parsing sub-request: %s' % x
      result = S_ERROR(errorStr)
      return result

  def __createSubRequestXML(self,ind,type):

    requestTypeStr = '%s_REQUEST' % type.upper()
    out = '  <%s\n' % requestTypeStr
    res = self.getSubRequestAttributes(ind,type)
    if not res['OK']:
      return res
    attributes = res['Value']
    for attribute in attributes:
      res = self.getSubRequestAttributeValue(ind,type,attribute)
      if not res['OK']:
        return res
      atttributeValue = res['Value']
      out = '%s    %s="%s"\n' % (out,attribute,atttributeValue)
    out = '%s    >\n' % out

    res = self.getSubRequestFiles(ind,type)
    if not res['OK']:
      return res
    files = res['Value']
    for file in files:
      out = '%s    <File\n' % out
      for attribute,atttributeValue in file.items():
        out = '%s      %s="%s"\n' % (out,attribute,atttributeValue)
      out = '%s    />\n' % out

    res = self.getSubRequestDatasets(ind,type)
    if not res['OK']:
      return res
    datasets = res['Value']
    for dataset in datasets:
      out = '%s    <Dataset\n      Handle="%s"\n    />\n' % (out,dataset)
    out = '%s  </%s>\n' % (out,requestTypeStr)
    return S_OK(out)