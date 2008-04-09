# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/Request.py,v 1.10 2008/04/09 20:55:48 atsareg Exp $

""" Request base class. Defines the common general parameters that should be present in any
    request
"""

__RCSID__ = "$Id: Request.py,v 1.10 2008/04/09 20:55:48 atsareg Exp $"

import commands, os, xml.dom.minidom, types, time
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
    self.attributeNames = ['RequestName','RequestType','RequestMethod',
                           'RequestID','DIRACSetup','OwnerDN','OwnerGroup',
                           'SourceComponent','TargetComponent','CurrentDate',
                           'CreationTime','ExecutionTime','JobID','Status']

    self.attributes = {}

    # Subrequests are dictionaries of arbitrary number of levels
    # The upper level must have the following attributes:

    self.subAttributeNames = ['Status','ExecutionTime']
    self.subrequests = {}

    self.initialize(script)

  def initialize(self,script):
    """ Set default values to attributes,parameters
    """

    for name in self.attributeNames:
      self.attributes[name] = 'Unknown'

    # Set some defaults
    self.attributes['DIRACSetup'] = "LHCb-Development"
    status,self.attributes['RequestID'] = commands.getstatusoutput('uuidgen')
    self.attributes['CreationTime'] = Time.toString(Time.dateTime())
    self.attributes['Status'] = "New"

    if script:
      self.parseRequest(script)

#####################################################################
  def __getattr__(self,name):
    """ Generic method to access request attributes or parameters
    """

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

  def __get_attribute(self):
     """ Generic method to get attributes
     """
     return self.attributes[self.item_called]

  def __set_attribute(self,value):
     """ Generic method to set attribute value
     """
     self.attributes[self.item_called] = value

#####################################################################
  def setCreationTime(self,time):
    """ Set the creation time to the current data and time
    """

    if time.lower() == "now":
      self.attributes['CreationTime'] = time.strftime('%Y-%m-%d %H:%M:%S')
    else:
      self.attributes['CreationTime'] = time

  #####################################################################
  def setCurrentDate(self):
    """ Set the creation time to the current data and time
    """

    self.attributes['CurrentDate'] = time.strftime('%Y-%m-%d %H:%M:%S')

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
    self.subrequests[stype].extend(subRequests)

 #####################################################################
  def addSubRequest(self,stype,subRequest):
    """ Set the sub-requests of a particular type associated to this request
    """

    if stype not in self.subrequests.keys():
      self.subrequests[stype] = []
    self.subrequests[stype].append(subRequest)

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
  def isEmpty(self):
    """ Check if the request contains more operations to be performed
    """

    for stype,slist in self.subrequests.items():
      for tdic in slist:
        if tdic['Status'] != "Done":
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
    for name,value in dict.items():
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
      for sub in self.subrequests[stype]:
        self.__dumpDictionary(stype+' subrequest',sub,0)
        print "--------------------------------------------------------"

    print "=============================================================="

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
    out = self.__dictionaryToXML(rname,self.subrequests[rtype][ind],
                                 attributes={'SubrequestType':rtype})
    return out

  def __dictionaryToXML(self,name,dict,indent = 0,attributes={}):
    """ Utility to convert a dictionary to XML
    """

    xml_attributes = ''
    xml_elements = []
    for attr,value in dict.items():
      if type(value) is not types.DictType:
        xml_attributes += '             %s="%s"\n' % (attr,str(value))
      else:
        xml_elements.append(self.__dictionaryToXML(attr,value,indent+1))

    for attr,value in attributes.items():
      xml_attributes += '             %s="%s"\n' % (attr,str(value))

    out = ' '*indent*8+'<%s \n%s>\n' % (name,xml_attributes[:-1])
    for el in xml_elements:
      out += ' '*indent*8+el
    out += ' '*indent*8+'</%s>\n\n' % name
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
    for name,value in dom.attributes.items():
      resultDict[name] = value

    for child in dom.childNodes:
      if child.nodeType == child.ELEMENT_NODE:
        dname = child.nodeName
        ddict = self.__dictionaryFromXML(child)
        resultDict[dname] = ddict

    return resultDict

  def parseSubRequest(self,dom):
    """ A simple subrequest parser from the dom object. This is to be overloaded
        in more complex request types
    """

    subDict = self.__dictionaryFromXML(dom)
    subType = subDict['SubrequestType']

    return subType,subDict

