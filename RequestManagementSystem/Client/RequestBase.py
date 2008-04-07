# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/RequestBase.py,v 1.3 2008/04/07 07:44:20 atsareg Exp $

""" Request base class. Defines the common general parameters that should be present in any
    request
"""

__RCSID__ = "$Id: RequestBase.py,v 1.3 2008/04/07 07:44:20 atsareg Exp $"

import commands, os, xml.dom.minidom
import DIRAC.Core.Utilities.Time as Time

def getCharacterData(node):

  out = ''
  for child in node.childNodes:
    if child.nodeType == child.TEXT_NODE or \
       child.nodeType == child.CDATA_SECTION_NODE:
      out = out + child.data

  return out.strip()


class RequestBase:

  def __init__(self,script=None):

    # This is a list of attributes - mandatory parameters
    self.attributeNames = ['RequestName','RequestType','RequestTechnology',
                           'RequestID','DIRACSetup','OwnerDN','OwnerGroup',
                           'SourceComponent','TargetComponent',
                           'CreationTime','ExecutionTime','JobID','Status']

    self.attributes = {}

    self.parameters = {}
    self.parameterNames = []

    self.subrequests = {}
    self.subrequestTypes = []

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

    for name in self.parameterNames:
      self.parameters[name] = 'Unknown'

    for name in self.subrequestTypes:
      self.subrequests[name] = []

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
      elif item in self.parameterNames:
        return self.__get_parameter
      else:
        raise AttributeError
    elif name.find('set') == 0:
      item = name[3:]
      self.item_called = item
      if item in self.attributeNames:
        return self.__set_attribute
      elif item in self.parameterNames:
        return self.__set_parameter
      else:
        raise AttributeError
    else:
      raise AttributeError

#####################################################################
  def getSubRequestTypes(self):
    """ Get the list of subrequest types
    """

    return self.subrequestTypes

#####################################################################
#
#  Attribute access methods

  def getAttributes(self):
    """ Get the dictionary of the request attributes
    """
    return self.attributes

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
#
#  Parameter access methods

  def getParameters(self):
    """ Get the dictionary of the request parameters
    """
    return self.parameters

  def getParameter(self,name):
    """ Get the parameter specified by its name
    """
    if name in self.parameterNames:
      return self.parameters[aname]
    else:
      return ''

  def __get_parameter(self):
     """ Generic method to get attributes
     """
     return self.parameters[self.item_called]

  def __set_parameter(self,value):
     """ Generic method to set attribute value
     """
     self.parameters[self.item_called] = value

#####################################################################
  def setCreationTime(self,time):
    """ Set the creation time to the current data and time
    """

    if time.lower() == "now":
      self.attributes['CreationTime'] = Time.toString(Time.dateTime())
    else:
      self.attributes['CreationTime'] = time

  def getCreationTime(self):
    """ Get the date the request was created
    """
    return self.attributes['CreationTime']

#####################################################################
  def setExecutionTime(self,time):
    """ Set the execution time to the current data and time
    """

    if time.lower() == "now":
      self.attributes['ExecutionTime'] = Time.toString(Time.dateTime())
    else:
      self.attributes['ExecutionTime'] = time

  def getExecutionTime(self):
    """ Get the date the request was created
    """
    return self.attributes['ExecutionTime']

#####################################################################
  def getSubRequests(self,type):
    """ Get the the sub-requests of a particular type
    """

    if type in self.subrequestTypes():
      return self.subrequests[type]
    else:
      return []

#####################################################################
  def setSubRequests(self,stype,subRequests):
    """ Set the sub-requests of a particular type associated to this request
    """

    if stype in self.subrequestTypes:
      self.subrequests[stype].extend(subRequests)

 #####################################################################
  def addSubRequest(self,stype,subRequest):
    """ Set the sub-requests of a particular type associated to this request
    """

    if stype in self.subrequestTypes:
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

    if type in self.subrequestTypes:
      return len(self.subrequests)
    else:
      return 0

 #####################################################################
  def dump(self):
    """ Print out the request contents
    """

    print "=============================================================="
    for pname in self.attributeNames:
      print (pname+':').ljust(26),self.attributes[pname]

    if self.parameters:
      print "--------------------------------------------------------"
      for pname in self.parameterNames:
        print (pname+':').ljust(26),self.parameters[pname]
    print "=============================================================="

    for stype in self.subrequestTypes:
      for sub in self.subrequests[stype]:
        print stype,'subrequest:'
        for name,value in sub.items():
          print (name+':').ljust(26),value
        print "--------------------------------------------------------"

    print "=============================================================="

###############################################################

  def toXML(self,requestType = ''):
    """ Output the request (including all sub-requests) to XML.
    """

    reqType = self.attributes['RequestType']

    out =  '<?xml version="1.0" encoding="UTF-8" ?>\n\n'
    out += '<%s_REQUEST>\n\n' % reqType.upper()

    xml_attributes = ''
    for attr in self.attributeNames:
      xml_attributes += '             %s="%s"\n' % (attr,str(self.attributes[attr]))

    out += '<Header \n%s/>\n\n' % xml_attributes

    xml_parameters = ''
    for par in self.parameterNames:
      xml_parameters += '    <%s><![CDATA[%s]]></%s>\n' % (par,str(self.parameters[par]),par)

    if xml_parameters:
      out += '<Parameters>\n'
      out += '%s \n' % xml_parameters
      out += '</Parameters>\n\n'

    for rtype in self.subrequestTypes:
      nReq = self.getNumSubRequests(rtype)
      for i in range(nReq):
        out += self.createSubRequestXML(i,rtype)

    out += '</%s_REQUEST>\n' % reqType.upper()
    return out

  def createSubRequestXML(self,ind,rtype):
    """ A simple subrequest representation assuming the subrequest is just
        a dictionary of subrequest attributes
    """

    xml_attributes = ''
    for attr in self.subrequests[rtype][ind].keys():
      xml_attributes += '             %s="%s"\n' % (attr,str(self.subrequests[rtype][ind][attr]))

    out = '<%s_SUBREQUEST \n%s/>\n\n' % (rtype.upper(),xml_attributes)
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

    dom_parameters = dom.getElementsByTagName('Parameters')[0]
    for name in self.parameterNames:
      node = dom_parameters.getElementsByTagName(name)[0]

      self.parameters[name] = getCharacterData(node)

    for rtype in self.subrequestTypes:
      dom_subrequests = dom.getElementsByTagName('%s_SUBREQUEST' % rtype.upper())
      if dom_subrequests:
        for req in dom_subrequests:
          subrequest = self.parseSubRequestFromDom(req)
          self.addSubRequest(rtype,subrequest)

###############################################################
  def parseSubRequestFromDom(self,dom):
    """ A simple subrequest parser from the dom object. This is to be overloaded
        in more complex request types
    """

    subDict = {}
    nAttributes = dom.attributes.length
    for i in range(nAttributes):
      subDict[str(dom.attributes.item(i).name)] = str(dom.attributes.item(i).value)

    return subDict

