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
    self.requestAttributes = ['SubRequestID','TargetSE','Status','Operation','SourceSE','Catalogue']
    # Possible keys to define the files in the request.
    self.fileAttributes = ['LFN','Size','PFN','GUID','Md5','Addler','Status','Attempt']
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
    """ Set the DIRAC WMS mode ( instance )
    """
    return self.mode

  def setDiracInstance(self,instance):
    """ Set the DIRAC WMS mode ( instance )
    """
    self.dirac_instance = instance

  def getDiracInstance(self):
    """ Set the DIRAC WMS mode ( instance )
    """
    return self.dirac_instance

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
      self.transfers[ind]['Status'] = status
    if type == 'register':
      self.registers[ind]['Status'] = status
    if type == 'removal':
      self.removals[ind]['Status'] = status
    if type == 'stage':
      self.stages[ind]['Status'] = status

###############################################################

  def setSubRequestFileStatus(self,ind,type,lfn,status):
    """ Set the operation to Done status
    """
    if type == 'transfer':
      self.transfers[ind]['Files'][lfn]['Status'] = status
    if type == 'register':
      self.registers[ind]['Files'][lfn]['Status'] = status
    if type == 'removal':
      self.removals[ind]['Files'][lfn]['Status'] = status
    if type == 'stage':
      self.stages[ind]['Files'][lfn]['Status'] = status

###############################################################

  def getSubRequestAttributes(self,ind,type):
    """ Get the sub-request attributes
    """
    if type == 'transfer':
      keys = self.transfers[ind].keys()
    if type == 'register':
      keys = self.registers[ind].keys()
    if type == 'removal':
      keys = self.removals[ind].keys()
    if type == 'stage':
      keys = self.stages[ind].keys()
    subRequestAttributes = []
    for attribute in keys:
      if attribute in self.requestAttributes:
        subRequestAttributes.append(attribute)
    return S_OK(subRequestAttributes)

###############################################################

  def getSubRequestAttributeValue(self,ind,type,attribute):
    """ Get the file attributes associated to a LFN and sub-request
    """
    if type == 'transfer':
      value = self.transfers[ind][attribute]
    if type == 'register':
      value = self.registers[ind][attribute]
    if type == 'removal':
      value = self.removals[ind][attribute]
    if type == 'stage':
      value = self.stages[ind][attribute]
    return S_OK(value)

###############################################################

  def getSubRequestFiles(self,ind,type):
    """ Get the file attributes associated to a LFN and sub-request
    """
    if type == 'transfer':
      lfns = self.transfers[ind]['Files'].keys()
    if type == 'register':
      lfns = self.registers[ind]['Files'].keys()
    if type == 'removal':
      lfns = self.removals[ind]['Files'].keys()
    if type == 'stage':
      lfns = self.stages[ind]['Files'].keys()
    return S_OK(lfns)

  ###############################################################

  def getSubRequestFileAttributes(self,ind,type,lfn):
    """ Get the file attributes associated to a LFN and sub-request
    """
    if type == 'transfer':
      keys = self.transfers[ind]['Files'][lfn].keys()
    if type == 'register':
      keys = self.registers[ind]['Files'][lfn].keys()
    if type == 'removal':
      keys = self.removals[ind]['Files'][lfn].keys()
    if type == 'stage':
      keys = self.stages[ind]['Files'][lfn].keys()
    subRequestFileAttributes = []
    for attribute in keys:
      if attribute in self.fileAttributes:
        subRequestFileAttributes.append(attribute)
    return S_OK(subRequestFileAttributes)

###############################################################

  def getSubRequestFileAttributeValue(self,ind,type,lfn,attribute):
    """ Get the file attributes associated to a LFN and sub-request
    """
    if type == 'transfer':
      value = self.transfers[ind]['Files'][lfn][attribute]
    if type == 'register':
      value = self.registers[ind]['Files'][lfn][attribute]
    if type == 'removal':
      value = self.removals[ind]['Files'][lfn][attribute]
    if type == 'stage':
      value = self.stages[ind]['Files'][lfn][attribute]
    return S_OK(value)

###############################################################

  def getSubRequestDatasets(self,ind,type):
    """ Get the file attributes associated to a LFN and sub-request
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

###############################################################

  def isEmpty(self):
    """ Check if the request contains more operations to be performed
    """
    for tdic in self.transfers:
      for lfn in tdic['Files'].keys():
        if tdic['Files'][lfn]['Status'] != "Done":
          return S_OK(0)
    for tdic in self.registers:
      for lfn in tdic['Files'].keys():
        if tdic['Files'][lfn]['Status'] != "Done":
          return S_OK(0)
    for tdic in self.removals:
      for lfn in tdic['Files'].keys():
        if tdic['Files'][lfn]['Status'] != "Done":
          return S_OK(0)
    for tdic in self.stages:
      for lfn in tdic['Files'].keys():
        if tdic['Files'][lfn]['Status'] != "Done":
          return S_OK(0)
    return S_OK(1)

###############################################################

  def addSubRequest(self,requestDict,type,catalogue=None):
    """  Add a new sub-requests of specified type
    """
    reqDict = {'Files':{}}
    for key in self.requestAttributes:
      if requestDict.has_key(key):
        reqDict[key] = requestDict[key]
      else:
        reqDict[key] = ''

    lfns = requestDict['Files'].keys()
    for lfn in lfns:
      reqDict['Files'][lfn] = {}
      for key in self.fileAttributes:
        if key != 'LFN':
          if requestDict['Files'][lfn].has_key(key):
            reqDict['Files'][lfn][key] = requestDict['Files'][lfn][key]
          else:
            reqDict['Files'][lfn][key] = ''

    if requestDict.has_key('Datasets'):
      reqDict['Datasets'] = requestDict['Datasets']
    else:
      reqDict['Datasets'] = []


    if not reqDict['Status']:
      reqDict['Status'] = 'Waiting'
    if not reqDict['SubRequestID']:
      reqDict['SubRequestID'] = makeGuid()
    if catalogue:
      reqDict['Catalogue'] = catalogue

    for lfn in reqDict['Files'].keys():
      if not reqDict['Files'][lfn]['Status']:
        reqDict['Files'][lfn]['Status'] = 'Waiting'
      if not reqDict['Files'][lfn]['GUID']:
        reqDict['Files'][lfn]['GUID'] = makeGuid()
      if not reqDict['Files'][lfn]['Attempt']:
        reqDict['Files'][lfn]['Attempt'] = 1

    if type == 'transfer':
      self.transfers.append(reqDict)
    if type == 'register':
      self.registers.append(reqDict)
    if type == 'removal':
      self.removals.append(reqDict)
    if type == 'stage':
      self.stages.append(reqDict)

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
          if key == 'Files':
            gLogger.info('Files:'.ljust(15))
            lfns = rdic['Files'].keys()
            for lfn in lfns:
              gLogger.info(lfn.ljust(15))
          elif key == 'Datasets':
            gLogger.info('Datasets:'.ljust(15))
            datasets = rdic['Datasets']
            for dataset in datasets:
              gLogger.info(dataset.ljust(15))
          else:
            gLogger.info( (key+':').ljust(15),rdic[key] )
        ind = ind+1

###############################################################

  def dumpShortToString(self):
    """ Generate summary string for all the sub-requests in this request.
    """
    out = ''
    for rdic in self.transfers:
      out = out + '\nTransfer: %s %s LFNs, % Datasets from %s to %s:\n' % (rdic['Operation'],len(rdic['Files'].keys()),len(rdic['Datasets']),rdic['SourceSE'],rdic['TargetSE'])
      lfns = rdic['Files'].keys()
      statusDict = {}
      for lfn in lfns:
        status = rdic['Files'][lfn]['Status']
        if not statusDict.has_key(status):
          statusDict[status]= 0
        statusDict[status] += 1
      for status in statusDict.keys():
        out = out + status +':'+str(statusDict[status])+'\t'

    for rdic in self.registers:
      out = out + '\nRegister: %s %s LFNs, % Datasets:\n' % (rdic['Operation'],len(rdic['Files'].keys()),len(rdic['Datasets']))
      lfns = rdic['Files'].keys()
      statusDict = {}
      for lfn in lfns:
        status = rdic['Files'][lfn]['Status']
        if not statusDict.has_key(status):
          statusDict[status]= 0
        statusDict[status] += 1
      for status in statusDict.keys():
        out = out + status +':'+str(statusDict[status])+'\t'

    for rdic in self.removals:
      out = out + '\nRemoval: %s %s LFNs, % Datasets from %s:\n' % (rdic['Operation'],len(rdic['Files'].keys()),len(rdic['Datasets']),rdic['TargetSE'])
      lfns = rdic['Files'].keys()
      statusDict = {}
      for lfn in lfns:
        status = rdic['Files'][lfn]['Status']
        if not statusDict.has_key(status):
          statusDict[status]= 0
        statusDict[status] += 1
      for status in statusDict.keys():
        out = out + status +':'+str(statusDict[status])+'\t'

    for rdic in self.stages:
      out = out + '\nStage: %s %s LFNs, % Datasets at %s:\n' % (rdic['Operation'],len(rdic['Files'].keys()),len(rdic['Datasets']),rdic['TargetSE'])
      lfns = rdic['Files'].keys()
      statusDict = {}
      for lfn in lfns:
        status = rdic['Files'][lfn]['Status']
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
      attributes = attributes + ' Date="'+self.date+'" '
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
      for attribute in subRequest.attributes.keys():
        requestDict[attribute] = subRequest.getAttribute(attribute)

      """ Obtain all the files which are detailed in the sub-request. """
      files = subRequest.getElementsByTagName('File')
      requestDict['Files'] = {}
      for file in files:
        """ Each file tag contains attributes specific to each file. """
        fileAttributes = file.attributes.keys()
        """ LFN is primary to all DM requests and must be supplied. """
        if 'LFN' in fileAttributes:
          lfn = file.getAttribute('LFN')
          requestDict['Files'][lfn] = {}
          fileAttributes.remove('LFN')
          for fileAttribute in fileAttributes:
            requestDict['Files'][lfn][fileAttribute] = file.getAttribute(fileAttribute)
        else:
          return S_ERROR('No LFN supplied with sub-request')

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
    lfns = res['Value']
    for lfn in lfns:
      out = '%s    <File\n      LFN="%s"\n' % (out,lfn)
      res = self.getSubRequestFileAttributes(ind,type,lfn)
      if not res['OK']:
        return res
      attributes = res['Value']
      for attribute in attributes:
        res = self.getSubRequestFileAttributeValue(ind,type,lfn,attribute)
        if not res['OK']:
          return res
        atttributeValue = res['Value']
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