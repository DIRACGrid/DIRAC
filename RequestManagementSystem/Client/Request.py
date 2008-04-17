# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/Request.py,v 1.11 2008/04/17 08:07:30 atsareg Exp $

""" Request base class. Defines the common general parameters that should be present in any
    request
"""

__RCSID__ = "$Id: Request.py,v 1.11 2008/04/17 08:07:30 atsareg Exp $"

import commands, os, xml.dom.minidom, types, time, copy
import DIRAC.Core.Utilities.Time as Time

def getCharacterData(node):

  out = ''
  for child in node.childNodes:
    if child.nodeType == child.TEXT_NODE or \
       child.nodeType == child.CDATA_SECTION_NODE:
      out = out + child.data

  return out.strip()


class Request:

  def __init__(self,script=None):

    # This is a list of attributes - mandatory parameters
    self.attributeNames = ['RequestName','RequestType','RequestID','DIRACSetup','OwnerDN',
                           'OwnerGroup','SourceComponent','CreationTime','ExecutionTime','JobID',
                           'Status']

    self.attributes = {}

    # Subrequests are dictionaries of arbitrary number of levels
    # The dictionary named Attributes must be present and must have
    # the following mandatory names

    self.subAttributeNames = ['Status','SubrequestID','Method','Type','CreationTime','ExecutionTime']
    self.subrequests = {}

    self.initialize(script)

  def initialize(self,script):
    """ Set default values to attributes,parameters
    """

    if type(script) in types.StringTypes or type(script) == types.NoneType:
      for name in self.attributeNames:
        self.attributes[name] = 'Unknown'

      # Set some defaults
      self.attributes['DIRACSetup'] = "LHCb-Development"
      status,self.attributes['RequestID'] = commands.getstatusoutput('uuidgen')
      self.attributes['CreationTime'] = Time.toString(Time.dateTime())
      self.attributes['Status'] = "New"
    elif type(script) == types.InstanceType:
      for attr in self.attributeNames:
        self.attributes[attr] = script.attributes[attr]

    # initialize request from an XML string
    if type(script) in types.StringTypes:
      self.parseRequest(script)

    # Initialize request from another request
    elif type(script) == types.InstanceType:
      self.subrequests = copy.deepcopy(script.subrequests)

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

    return self.subrequests.keys()

#####################################################################
#
#  Attribute access methods

  def getRequestAttributes(self):
    """ Get the dictionary of the request attributes
    """
    return self.attributes

  def setRequestAttributes(self,attributeDict):
    """ Set the attributes associated to this request
    """
    self.attributes.update(attributeDict)

  def getAttribute(self,aname):
    """ Get the attribute specified by its name aname
    """
    return self.attributes[aname]

  def setAttribute(self,aname,value):
    """ Set the attribute specified by its name aname
    """
    self.attributes[aname] =  value

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
  def setCreationTime(self,time):
    """ Set the creation time to the current data and time
    """

    if time.lower() == "now":
      self.attributes['CreationTime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    else:
      self.attributes['CreationTime'] = time

#####################################################################
  def setExecutionTime(self,time):
    """ Set the execution time to the current data and time
    """

    if time.lower() == "now":
      self.attributes['ExecutionTime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    else:
      self.attributes['ExecutionTime'] = time

#####################################################################
  def getSubRequests(self,type):
    """ Get the the sub-requests of a particular type
    """

    if type in self.subrequests.keys():
      return self.subrequests[type]
    else:
      return []

#####################################################################
  def setSubRequests(self,stype,subRequests):
    """ Set the sub-requests of a particular type associated to this request
    """
    if stype not in self.subrequests.keys():
      self.subrequests[stype] = []
    for sub in subRequests:
      self.addSubRequest(stype,sub)

 #####################################################################
  def addSubRequest(self,stype,subRequest):
    """ Set the sub-requests of a particular type associated to this request
    """

    if stype not in self.subrequests.keys():
      self.subrequests[stype] = []
    new_subrequest = copy.deepcopy(subRequest)
    new_subrequest['Attributes']['Type'] = stype
    status,new_subrequest['Attributes']['SubrequestID'] = commands.getstatusoutput('uuidgen')
    self.subrequests[stype].append(new_subrequest)

###############################################################

  def getSubRequest(self,ind,type):
    """ Get the sub-request as specified by its index
    """

    try:
      subrequest = self.subrequests[type][ind]
      return subrequest
    except:
      return None

###############################################################

  def getNumSubRequests(self,type):
    """ Get the number of sub-requests for a given request type
    """

    if type in self.subrequests.keys():
      return len(self.subrequests[type])
    else:
      return 0

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
  def isEmpty(self):
    """ Check if the request contains more operations to be performed
    """

    for stype,slist in self.subrequests.items():
      for tdic in slist:
        if tdic['Attributes']['Status'] != "Done":
          return 0
    return 1

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
        if i != len(self.subrequests[stype])-1:
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

    reqType = self.attributes['RequestType']

    out =  '<?xml version="1.0" encoding="UTF-8" ?>\n\n'
    out += '<DIRAC_REQUEST>\n\n'

    xml_attributes = ''
    for attr in self.attributeNames:
      xml_attributes += '             %s="%s"\n' % (attr,str(self.attributes[attr]))

    out += '<Header \n%s/>\n\n' % xml_attributes

    for rtype in self.subrequests.keys():
      nReq = self.getNumSubRequests(rtype)
      for i in range(nReq):
        out += self.createSubRequestXML(i,rtype)

    out += '</DIRAC_REQUEST>\n'
    return out

  def createSubRequestXML(self,ind,rtype):
    """ A simple subrequest representation assuming the subrequest is just
        a dictionary of subrequest attributes
    """
    rname = rtype.upper()+'_SUBREQUEST'
    out = self.__dictionaryToXML(rname,self.subrequests[rtype][ind])
    return out

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
    subType = subDict['Attributes']['Type']

    return subType,subDict

