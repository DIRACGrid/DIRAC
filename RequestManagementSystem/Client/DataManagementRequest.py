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
    self.requestAttributes = ['RequestID','Destination','Status','Operation','Source','Catalogue']
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

    if request:
      dom = xml.dom.minidom.parseString(request)
      header = dom.getElementsByTagName('Header')[0]
      self.jobid = header.getAttribute('JobID')
      self.ownerDN = header.getAttribute('OwnerDN')
      self.date = header.getAttribute('Date')
      self.mode = header.getAttribute('Mode')

      if dom.getElementsByTagName('TRANSFER_REQUEST'):
        for subRequest in dom.getElementsByTagName('TRANSFER_REQUEST'):
          result = self.__parseSubRequest(subRequest)
          if result['OK']:
            self.addTransfer(result['Value'])

      if dom.getElementsByTagName('REGISTER_REQUEST'):
        for subRequest in dom.getElementsByTagName('REGISTER_REQUEST'):
          result = self.__parseSubRequest(subRequest)
          if result['OK']:
            self.addRegister(result['Value'])

      if dom.getElementsByTagName('REMOVAL_REQUEST'):
        for subRequest in dom.getElementsByTagName('REMOVAL_REQUEST'):
          result = self.__parseSubRequest(subRequest)
          if result['OK']:
            self.addRemoval(result['Value'])

      if dom.getElementsByTagName('STAGE_REQUEST'):
        for subRequest in dom.getElementsByTagName('STAGE_REQUEST'):
          result = self.__parseSubRequest(subRequest)
          if result['OK']:
            self.addStage(result['Value'])

###############################################################

  def setCurrentDate(self):
    """ Set the request date to the current date and time
    """
    self.date = time.strftime('%Y-%m-%d %H:%M:%S')

  def getCurrentDate(self):
    """ Get the date the request was set
    """
    return self.date

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
      return self.transfers[ind]
    elif type == 'register':
      return self.registers[ind]
    elif type == 'removal':
      return self.removals[ind]
    elif type == 'stage':
      return self.stages[ind]
    else:
      return 0

###############################################################

  def getNumSubRequests(self,type):
    """ Get the number of sub-requests for a given request type
    """
    if type == 'transfer':
      return len(self.transfers)
    elif type == 'register':
      return len(self.registers)
    elif type == 'removal':
      return len(self.removals)
    elif type == 'stage':
      return len(self.stages)
    else:
      return 0

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

  def isEmpty(self):
    """ Check if the request contains more operations to be performed
    """
    for tdic in self.transfers:
      for lfn in tdic['Files'].keys():
        if tdic['Files'][lfn]['Status'] != "Done":
          return 0
    for tdic in self.registers:
      for lfn in tdic['Files'].keys():
        if tdic['Files'][lfn]['Status'] != "Done":
          return 0
    for tdic in self.removals:
      for lfn in tdic['Files'].keys():
        if tdic['Files'][lfn]['Status'] != "Done":
          return 0
    for tdic in self.stages:
      for lfn in tdic['Files'].keys():
        if tdic['Files'][lfn]['Status'] != "Done":
          return 0
    return 1

###############################################################

  def addSubRequest(self,requestDict,type,catalogue=None):
    """  Add a new sub-requests of specified type
    """
    reqDict = {'Files':{}}
    self.datasetAttributes
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

    if not reqDict['Status']:
      reqDict['Status'] = 'Waiting'
    if not reqDict['RequestID']:
      reqDict['RequestID'] = makeGuid()
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

    return out

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
    if self.ownerDN:
      attributes = attributes + ' OwnerDN="'+self.ownerDN+'" '
    if self.date:
      attributes = attributes + ' Date="'+self.date+'" '
    if self.mode:
      attributes = attributes + ' Mode="'+self.mode+'" '
    if self.dirac_instance:
      attributes = attributes + ' DiracInstance="'+self.dirac_instance+'" '

    out = out + '<Header '+attributes+' />\n\n'

    for subRequest in self.transfers:
      out = out+'  <TRANSFER_REQUEST>\n'+self.__createSubRequestXML(subRequest)+'  </TRANSFER_REQUEST>\n\n'

    for subRequest in self.registers:
      out = out+'  <REGISTER_REQUEST>\n'+self.__createSubRequestXML(subRequest)+'  </REGISTER_REQUEST>\n\n'

    for subRequest in self.removals:
      out = out+'  <REMOVAL_REQUEST>\n'+self.__createSubRequestXML(subRequest)+'  </REMOVAL_REQUEST>\n\n'

    for subRequest in self.stages:
      out = out+'  <STAGE_REQUEST>\n'+self.__createSubRequestXML(subRequest)+'  </STAGE_REQUEST>\n\n'

    out = out + '</DATA_MANAGEMENT_REQUEST>\n'
    return out

###############################################################

  def toFile(self,fname):
    reqfile = open(fname,'w')
    xmlstr = self.toXML()
    reqfile.write(xmlstr)
    reqfile.close()

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
        if datasetAttributes.contains('Handle'):
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

  def __createSubRequestXML(self,subRequest):
    out = ''
    for key,value in subRequest.items():
      if key == 'Files':
        lfns = value.keys()
        for lfn in lfns:
          out = out+'    <File\n      LFN="%s"\n' % lfn
          attributes = value[lfn]
          for attribute,attValue in attributes.items():
            out = out + '      '+attribute+'="'+str(attValue)+'"\n'
          out = out+'    />\n'
      elif key == 'Datasets':
        for dataset in value:
          out = out+'    <Dataset\n      Handle="'+dataset+'"\n    />\n'
      else:
        out = out + '    '+key+'="'+str(value)+'"\n'
    return out