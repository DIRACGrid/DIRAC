# $HeadURL$
__RCSID__ = "$Id$"

"""
The SubRequestContainer is a container for DIRAC sub-requests
"""

import commands, os, xml.dom.minidom, types, time, copy, datetime
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC import gConfig,gLogger, S_OK, S_ERROR, Time
from DIRAC.Core.Utilities import DEncode

class SubRequestContainer:

  def __init__(self):
    # These are the subrequest attributes
    self.RequestType = ''
    self.Status = 'Waiting'
    self.SubRequestID = 0
    self.Operation = ''
    self.SourceSE = ''
    self.TargetSE = ''
    self.CreationTime = str(Time.dateTime())
    self.SubmissionTime = str(Time.dateTime())
    self.LastUpdate = str(Time.dateTime())
    self.Error = ''
    self.Catalog = ''
    self.Arguments = ''
    self.Files = []
    self.Datasets = []

  def setCreationTime(self,time=''):
    """ Set the creation time to the current data and time
    """
    if not time:
      time = str(Time.dateTime())
    self.CreationTime = time
    return S_OK()

  def setLastUpdate(self,time=''):
    """ Set the last update to the current data and time
    """
    if not time:
      time = str(Time.dateTime())
    self.LastUpdate = time  
    return S_OK()

  def getAttributes(self):
    """ Get the sub-request attributes in a dictionary
    """
    attrsDict = { 'RequestType'  :  self.RequestType,
                  'Status'       :  self.Status,
                  'Operation'    :  self.Operation,
                  'SubRequestID' :  self.SubRequestID,
                  'CreationTime' :  self.CreationTime,
                  'LastUpdate'   :  self.LastUpdate,
                  'Error'        :  self.Error,
                  'TargetSE'     :  self.TargetSE,
                  'Catqlog'      :  self.Catalog}
    return S_OK(attrsDict)

  def getNumFiles(self,ind,type):
    """ Get the number of files present in the sub-request
    """
    return S_OK(len(self.Files))

  def isEmpty(self):
    """ Determine if all the sub-request operations have been completed
    """
    for oFile in self.Files:
      if not oFile.Status in ['Done','Failed']:
        return S_OK(0)
    for oDataset in self.Datasets:
      if not oDataset.Status in ['Done','Failed']:
         return S_OK(0)
    if not self.Status == 'Done':
      self.Status = 'Done'
    return S_OK(1)

  def digest(self):
    """ Create a digest string of the current status of the sub-request
    """
    digestList = []
    digestList.append(self.RequestType)
    digestList.append(self.Operation)
    digestList.append(self.Status)
    digestList.append(self.TargetSE)
    digestList.append(self.Catalog)
    if len(self.Files) > 0:
      digestList.append('<%s files>' % len(self.Files)) 
    if len(self.Datasets) > 0:
      digestList.append('<%s datasets>' % len(self.Datasets))
    return S_OK(":".join(digestList))

class Ignore:
  def update(self,request):
    """ Add subrequests from another request
    """
    requestTypes = request.getSubRequestTypes()['Value']
    for requestType in requestTypes:
      subRequests = request.getSubRequests(requestType)['Value']
      self.setSubRequests(requestType,subRequests)
    return S_OK()

  def initiateSubRequest(self,type):
    """ Add dictionary to list of requests and return the list index
    """
    defaultDict = {'Attributes':{},'Files':[],'Datasets':[]}
    if not self.subRequests.has_key(type):
      self.subRequests[type] = []
    self.subRequests[type].append(defaultDict)
    length = len(self.subRequests[type])
    return S_OK(length-1)

  def addSubRequest(self,requestDict,type):
    """  Add a new sub-requests of specified type
    """
    # Initialise the sub-request
    index = self.initiateSubRequest(type)['Value']
    # Stuff the sub-request with the attributes
    attributeDict = {'Status':'Waiting','SubRequestID':makeGuid(),
                     'CreationTime': str(datetime.datetime.utcnow()),
                     'ExecutionOrder':0}
    for attr,value in requestDict['Attributes'].items():
      attributeDict[attr] = value
    self.setSubRequestAttributes(index,type,attributeDict)

    if requestDict.has_key('Files'):
      files = []
      for file in requestDict['Files']:
        fileDict = {'Status':'Waiting','FileID':makeGuid(),'Attempt':1}
        for attr,value in file.items():
          fileDict[attr] = value
        files.append(fileDict)
      self.setSubRequestFiles(index,type,files)

    if requestDict.has_key('Datasets'):
      datasets = []
      for dataset in requestDict['Datasets']:
        datasetDict = {'Status':'Waiting'}
        for attr,value in file.items():
          fileDict[attr] = value
        datasets.append(datasetDict)
      self.setSubRequestDatasets(index,type,datasets)
    return S_OK(index)

  ##############################################

  def toXML(self):
    """ Output the sub-request to XML
    """
    name = self.RequestType.upper()+'_SUBREQUEST'
    out = self.__dictionaryToXML(name,self.subRequests)
    return S_OK(out)

  def __dictionaryToXML(self,name,dict,indent = 0,attributes={}):
    """ Utility to convert a dictionary to XML
    """
    xml_attributes = ''
    xml_elements = []
    for attr,value in dict.items():
      if type(value) is types.DictType:
        xml_elements.append(self.__dictionaryToXML(attr,value,indent+1))
      elif type(value) is types.ListType:
        xml_elements.append(self.__listToXML(attr,value,indent+1))
      else:
        xml_attributes += ' '*(indent+1)*8+'<%s element_type="leaf"><![CDATA[%s]]></%s>\n' % (attr,str(value),attr)

    for attr,value in attributes.items():
      xml_attributes += ' '*(indent+1)*8+'<%s element_type="leaf">![CDATA[%s]]</%s>\n' % (attr,str(value),attr)

    out = ' '*indent*8+'<%s element_type="dictionary">\n%s\n' % (name,xml_attributes[:-1])
    for el in xml_elements:
      out += ' '*indent*8+el
    out += ' '*indent*8+'</%s>\n' % name
    return out

  def __listToXML(self,name,list,indent = 0):
    """ Utility to convert a list to XML
    """
    out = ''
    if list:
      den = DEncode.encode(list)
      out += ' '*indent*8+'<%s element_type="list">\n' % (name)
      out += ' '*(indent+1)*8+'<EncodedString element_type="leaf"><![CDATA[%s]]></EncodedString>\n' % (den)
      out += ' '*indent*8+'</%s>\n' % name
    return out

  def parseRequest(self,request):
    """ Create request from the XML string or file
    """
    if os.path.exists(request):
      dom = xml.dom.minidom.parse(request)
    else:
      dom = xml.dom.minidom.parseString(request)

    header = dom.getElementsByTagName('Header')[0]
    for name in self.attributeNames:
      self.attributes[name] = header.getAttribute(name)

    request = dom.getElementsByTagName('DIRAC_REQUEST')[0]
    dom_subrequests = request.childNodes
    for dom_subrequest in dom_subrequests:
      if dom_subrequest.nodeName.find('_SUBREQUEST') != -1:
        startTime = time.time()
        subrequest = self.parseSubRequest(dom_subrequest)
        middleTime = time.time()
        requestType = dom_subrequest.nodeName.split('_')[0].lower()
        self.addSubRequest(subrequest,requestType)

  def parseSubRequest(self,dom):
    """ A simple subrequest parser from the dom object. This is to be overloaded
        in more complex request types
    """
    subDict = self.__dictionaryFromXML(dom)
    return subDict

  def __dictionaryFromXML(self,dom):
    """ Utility to get a dictionary from the XML element
    """
    resultDict = {}
    for child in dom.childNodes:
      if child.nodeType == child.ELEMENT_NODE:
        dname = child.nodeName
        dom_dict = dom.getElementsByTagName(dname)[0]
        if dom_dict.getAttribute('element_type') == 'dictionary':
          ddict = self.__dictionaryFromXML(child)
          resultDict[dname] = ddict
        elif dom_dict.getAttribute('element_type') == 'list':
          resultDict[dname] = self.__listFromXML(child)
        elif dom_dict.getAttribute('element_type') == 'leaf':
          value = self.__getCharacterData(child)
          resultDict[dname] = value
    return resultDict

  def __listFromXML(self,dom):
    resultList = []
    """
    for child in dom.childNodes:
      if child.nodeType == child.ELEMENT_NODE:
        dname = child.nodeName
        dom_dict = dom.getElementsByTagName(dname)[0]
        if dom_dict.getAttribute('element_type') == 'dictionary':
          ddict = self.__dictionaryFromXML(child)
          resultList.append(ddict)
        elif dom_dict.getAttribute('element_type') == 'list':
          resultList = self.__listFromXML(child)
        elif dom_dict.getAttribute('element_type') == 'leaf':
          value = self.__getCharacterData(child)
          resultList.append(value)
    """
    for child in dom.childNodes:
      if child.nodeType == child.ELEMENT_NODE:
        dname = child.nodeName
        dom_dict = dom.getElementsByTagName(dname)[0]
        if dom_dict.getAttribute('element_type') == 'leaf':
          value = self.__getCharacterData(child)
          resultList,ignored = DEncode.decode(value)
    return resultList

  def __getCharacterData(self,node):
    out = ''
    for child in node.childNodes:
      if child.nodeType == child.TEXT_NODE or \
         child.nodeType == child.CDATA_SECTION_NODE:
        out = out + child.data
    return str(out.strip())
