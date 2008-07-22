# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/Request.py,v 1.20 2008/07/22 14:25:42 acasajus Exp $

""" Request base class. Defines the common general parameters that should be present in any
    request
"""

__RCSID__ = "$Id: Request.py,v 1.20 2008/07/22 14:25:42 acasajus Exp $"

import commands, os, xml.dom.minidom, types, time, copy, datetime
from DIRAC import gConfig

from DIRAC.Core.Security.Misc import getProxyInfo

def getCharacterData(node):

  out = ''
  for child in node.childNodes:
    if child.nodeType == child.TEXT_NODE or \
       child.nodeType == child.CDATA_SECTION_NODE:
      out = out + child.data

  return out.strip()


class Request:

  def __init__(self,request=None,init=True):

    # This is a list of attributes - mandatory parameters
    self.attributeNames = ['RequestName','RequestID','DIRACSetup','OwnerDN',
                           'OwnerGroup','SourceComponent','CreationTime','ExecutionTime','JobID',
                           'Status']

    self.attributes = {}

    # Subrequests are dictionaries of arbitrary number of levels
    # The dictionary named Attributes must be present and must have
    # the following mandatory names

    self.subAttributeNames = ['Status','SubRequestID','Method','RequestType','CreationTime','ExecutionTime']
    self.subrequests = {}

    if init:
      self.initialize(request)

  def initialize(self,request):
    """ Set default values to attributes,parameters
    """

    if type(request) == types.NoneType:
      # Set some defaults
      for name in self.attributeNames:
        self.attributes[name] = 'Unknown'
      status,self.attributes['RequestID'] = commands.getstatusoutput('uuidgen')
      self.attributes['CreationTime'] = str(datetime.datetime.utcnow())
      self.attributes['Status'] = "New"
      result = getProxyInfo()
      if result['OK']:
        proxyDict = result[ 'Value' ]
        self.attributes['OwnerDN'] = proxyDict[ 'identity' ]
        if 'group' in proxyDict:
          self.attributes['OwnerGroup'] = proxyDict[ 'group' ]
      self.attributes['DIRACSetup'] = gConfig.getValue('/DIRAC/Setup','LHCb-Development')

    elif type(request) == types.InstanceType:
      for attr in self.attributeNames:
        self.attributes[attr] = script.attributes[attr]

    # initialize request from an XML string
    if type(request) in types.StringTypes:
      for name in self.attributeNames:
        self.attributes[name] = 'Unknown'
      self.parseRequest(request)

    # Initialize request from another request
    elif type(request) == types.InstanceType:
      self.subrequests = copy.deepcopy(request.subrequests)

#####################################################################
  def __getattr__(self,name):
    """ Generic method to access request attributes or parameters
    """

    if name.find('getSubrequest') ==0:
      item = name[13:]
      self.item_called = item
      if item in self.subAttributeNames:
        return self.__get_subattribute
      else:
        raise AttributeError, name
    if name.find('setSubrequest') ==0:
      item = name[13:]
      self.item_called = item
      if item in self.subAttributeNames:
        return self.__set_subattribute
      else:
        raise AttributeError, name
    if name.find('get') ==0:
      item = name[3:]
      self.item_called = item
      if item in self.attributeNames:
        return self.__get_attribute
      else:
        raise AttributeError, name
    elif name.find('set') == 0:
      item = name[3:]
      self.item_called = item
      if item in self.attributeNames:
        return self.__set_attribute
      else:
        raise AttributeError, name
    else:
      raise AttributeError, name

#####################################################################
  def getSubRequestTypes(self):
    """ Get the list of subrequest types
    """
    subRequestTypes = self.subrequests.keys()
    return S_OK(subRequestTypes)

#####################################################################
#
#  Attribute access methods

  def getRequestAttributes(self):
    """ Get the dictionary of the request attributes
    """
    return S_OK(self.attributes)

  def setRequestAttributes(self,attributeDict):
    """ Set the attributes associated to this request
    """
    self.attributes.update(attributeDict)
    return S_OK()

  def getAttribute(self,aname):
    """ Get the attribute specified by its name aname
    """
    attributeValue = self.attributes[aname]
    return S_OK(attributeValue)

  def setAttribute(self,aname,value):
    """ Set the attribute specified by its name aname
    """
    self.attributes[aname] =  value
    return S_OK()

  def __get_attribute(self):
     """ Generic method to get attributes
     """
     return self.attributes[self.item_called]

  def __set_attribute(self,value):
     """ Generic method to set attribute value
     """
     self.attributes[self.item_called] = value

  def __get_subattribute(self,ind):
     """ Generic method to get attributes
     """
     return self.subrequests[ind]['Attributes'][self.item_called]

  def __set_subattribute(self,ind,value):
     """ Generic method to set attribute value
     """
     self.subrequests[ind]['Attributes'][self.item_called] = value

#####################################################################
  def setCreationTime(self,time='now'):
    """ Set the creation time to the current data and time
    """

    if time.lower() == "now":
      self.attributes['CreationTime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    else:
      self.attributes['CreationTime'] = time
    return S_OK()

#####################################################################
  def setExecutionTime(self,time='now'):
    """ Set the execution time to the current data and time
    """

    if time.lower() == "now":
      self.attributes['ExecutionTime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    else:
      self.attributes['ExecutionTime'] = time
    return S_OK()

#####################################################################
  def getSubRequests(self,type):
    """ Get the the sub-requests of a particular type
    """

    if type in self.subrequests.keys():
      subRequests = self.subrequests[type]
    else:
      subRequests = []
    return S_OK(subRequests)

#####################################################################
  def setSubRequests(self,stype,subRequests):
    """ Set the sub-requests of a particular type associated to this request
    """
    if stype not in self.subrequests.keys():
      self.subrequests[stype] = []
    for sub in subRequests:
      self.addSubRequest(stype,sub)
    return S_OK()

#####################################################################
  def update(self,request):
    """ Add subrequests from another request
    """

    subTypes = request.getSubRequestTypes()
    for stype in subTypes:
      subRequests = request.getSubRequests(stype)
      self.setSubRequests(stype,subRequests)

#####################################################################
  def addSubRequest(self,stype,subRequest):
    """ Set the sub-requests of a particular type associated to this request
    """

    if stype not in self.subrequests.keys():
      self.subrequests[stype] = []
    if type(subRequest) == types.DictType:
      new_subrequest = copy.deepcopy(subRequest)
    elif type(subRequest) == types.InstanceType:
      new_subrequest = copy.deepcopy(subRequest.getDictionary())
    new_subrequest['Attributes']['RequestType'] = stype
    status,new_subrequest['Attributes']['SubRequestID'] = commands.getstatusoutput('uuidgen')
    self.subrequests[stype].append(new_subrequest)

###############################################################

  def getSubRequest(self,ind,type):
    """ Get the sub-request as specified by its index
    """
    try:
      subrequest = self.subrequests[type][ind]
      return S_OK(subrequest)
    except:
      return S_ERROR(subrequest)

###############################################################

  def getNumSubRequests(self,type):
    """ Get the number of sub-requests for a given request type
    """
    if type in self.subrequests.keys():
      numSubRequests =  len(self.subrequests[type])
      return S_OK(numSubRequests)
    else:
      return S_ERROR()

###############################################################

  def setSubRequestStatus(self,ind,type,status):
    """ Set the operation to Done status
    """
    self.subrequests[type][ind]['Attributes']['Status'] = status
    return S_OK()

  def getSubRequestAttributes(self,ind,type):
    """ Get the sub-request attributes
    """
    attributes = self.subrequests[type][ind]['Attributes']
    return S_OK(attributes)

  def setSubRequestAttributes(self,ind,type,attributeDict):
    """ Set the sub-request attributes
    """
    self.subrequests[type][ind]['Attributes'].update(attributeDict)
    return S_OK()

  def getSubRequestAttributeValue(self,ind,type,attribute):
    """ Get the attribute value associated to a sub-request
    """
    requestAttrValue = self.subrequests[type][ind]['Attributes'][attribute]
    return S_OK(requestAttrValue)

  def setSubRequestAttributeValue(self,ind,type,attribute,value):
    """ Set the attribute value associated to a sub-request
    """
    if not self.subrequests[type][ind].has_key('Attributes'):
      self.subrequests[type][ind]['Attributes'] = {}
    self.subrequests[type][ind]['Attributes'][attribute] = value
    return S_OK()

###############################################################
  def isEmpty(self,requestType=None):
    """ Check if the request has all the subrequests done
    """
    for stype,slist in self.subrequests.items():
      if requestType:
        if stype == requestType:
          for tdic in slist:
            if tdic['Attributes']['Status'] != "Done":
              return S_OK(0)
      else:
        for tdic in slist:
          if tdic['Attributes']['Status'] != "Done":
            return S_OK(0)
    return S_OK(1)

  def isSubRequestEmpty(self,ind,type):
    """ Check if the specified subrequest is done
    """
    if not self.subrequests.has_key(type):
      return S_OK(1)
    if ind < len(self.subrequests[type]):
      if self.subrequests[type][ind]['Attributes']['Status'] != "Done":
        return S_OK(0)
    return S_OK(1)

 #####################################################################
  def __dumpDictionary(self,name,dict,indent=0):
    """ Utility for pretty printing of dictionaries
    """

    if indent:
      print ' '*indent*8,name+':'
    else:
      print name+':'

    # print dictionaries in the alphabetic order
    names = dict.keys()
    names.sort()

    for name in names:
      value = dict[name]
      if type(value) is not types.DictType:
        print ' '*(indent+1)*8,(name+':').ljust(26),value
      else:
        self.__dumpDictionary(name,value,indent+1)

  def dump(self):
    """ Print out the request contents
    """

    print "=============================================================="
    for pname in self.attributeNames:
      print (pname+':').ljust(26),self.attributes[pname]
    print "=============================================================="

    for stype in self.subrequests.keys():
      for i in range(len(self.subrequests[stype])):
        sub = self.subrequests[stype][i]
        self.__dumpDictionary(stype+' subrequest',sub,0)
        print "--------------------------------------------------------"

    print "=============================================================="

  def dumpSubrequest(self,ind,stype):
    """ Print out the subrequest contents
    """

    sub = self.subrequests[stype][ind]
    self.__dumpDictionary(stype+' subrequest',sub,0)

###############################################################

  def toXML(self,requestType = ''):
    """ Output the request (including all sub-requests) to XML.
    """
    out =  '<?xml version="1.0" encoding="UTF-8" ?>\n\n'
    out += '<DIRAC_REQUEST>\n\n'

    xml_attributes = ''
    for attr,value in self.attributes.items():
      xml_attributes += '             %s="%s"\n' % (attr,str(value))

    out += '<Header \n%s/>\n\n' % xml_attributes
    for rtype in self.subrequests.keys():
      if requestType:
        if rtype == requestType:
          nReq = self.getNumSubRequests(rtype)['Value']
          for i in range(nReq):
            out += self.createSubRequestXML(i,rtype)['Value']
      else:
        nReq = self.getNumSubRequests(rtype)['Value']
        for i in range(nReq):
          out += self.createSubRequestXML(i,rtype)['Value']
    out += '</DIRAC_REQUEST>\n'
    return S_OK(str(out))

  def createSubRequestXML(self,ind,rtype):
    """ A simple subrequest representation assuming the subrequest is just
        a dictionary of subrequest attributes
    """
    rname = rtype.upper()+'_SUBREQUEST'
    out = self.__dictionaryToXML(rname,self.subrequests[rtype][ind])
    return S_OK(out)

  def __dictionaryToXML(self,name,dict,indent = 0,attributes={}):
    """ Utility to convert a dictionary to XML
    """

    xml_attributes = ''
    xml_elements = []
    for attr,value in dict.items():
      if type(value) is not types.DictType:
        xml_attributes += ' '*(indent+1)*8+'<%s element_type="leaf"><![CDATA[%s]]></%s>\n' % (attr,str(value),attr)
      else:
        xml_elements.append(self.__dictionaryToXML(attr,value,indent+1))

    for attr,value in attributes.items():
      xml_attributes += ' '*(indent+1)*8+'<%s element_type="leaf">![CDATA[%s]]</%s>\n' % (attr,str(value),attr)

    out = ' '*indent*8+'<%s element_type="dictionary">\n%s\n' % (name,xml_attributes[:-1])
    for el in xml_elements:
      out += ' '*indent*8+el
    out += ' '*indent*8+'</%s>\n' % name
    return out

###############################################################
  def toFile(self,fname):
    """ Output the XML representation of the request to a file
    """
    xmlString = self.toXML()
    reqfile = open(fname,'w')
    reqfile.write(xmlString)
    reqfile.close()

###############################################################
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
        stype,subrequest = self.parseSubRequest(dom_subrequest)
        self.addSubRequest(stype,subrequest)

###############################################################
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
        elif dom_dict.getAttribute('element_type') == 'leaf':
          value = getCharacterData(child)
          resultDict[dname] = value

    return resultDict

  def parseSubRequest(self,dom):
    """ A simple subrequest parser from the dom object. This is to be overloaded
        in more complex request types
    """
    subDict = self.__dictionaryFromXML(dom)
    subType = subDict['Attributes']['RequestType']
    return subType,subDict

###############################################################
#
#  Short string representation of the request to store as a job parameter
#  All the possible subrequest types should be present here

  def dumpShortToString(self):
    """ Generate summary string for all the sub-requests in this request.
    """
    out = ''
    requestTypes = self.subrequests.keys()

    for rtype in requestTypes:
      if rtype == 'transfer' and self.subrequests.has_key(rtype):
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

      if rtype == 'register' and self.subrequests.has_key(rtype):
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

      if rtype == 'removal' and self.subrequests.has_key(rtype):
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

      if rtype == 'stage' and self.subrequests.has_key(rtype):
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

      if rtype == 'jobstate' and self.subrequests.has_key(rtype):
        pass
      if rtype == 'bookkeeping' and self.subrequests.has_key(rtype):
        pass


    return S_OK(out)
