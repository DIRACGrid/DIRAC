# $Id: WorkflowReader.py,v 1.1 2007/06/20 11:06:03 gkuznets Exp $
"""
    This is a comment
"""
__RCSID__ = "$Revision: 1.1 $"

#try: # this part to inport as part of the DIRAC framework
from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
#  print "DIRAC.Core.Workflow.Parameter"
#except: # this part is to import code without DIRAC
#  from Parameter import *
#  from Module import *
#  from Step import *
#  from Workflow import *
#  print "Parameter"

import xml.sax
from xml.sax.handler import ContentHandler


class WorkflowXMLHandler(ContentHandler):

  def __init__(self):
    # this is an attribute for the object to be created from the XML document
    self.root=None
    self.stack=None
    self.strings=None

  def startDocument(self):
    #reset the process
    self.root=None
    self.stack=[]
    self.strings=[]
  def endDocument(self):
    pass

  def startElement(self, name, attrs):
    if name == "Workflow":
      self.root = Workflow()
      #self.current = self.root
      self.stack.append(self.current)
    elif name == "StepDefinition":
      #self.current = StepDefinition()
      self.root.addStep(self.current)
      self.stack.append(self.root)
    elif name == "StepInstance":
      pass
    elif name == "ModuleDefinition":
      pass
    elif name == "ModuleInstance":
      pass
    else:
      print "startElement", name, attrs
      print attrs.getLength(), attrs.getNames()
    #print attrs.getType(attrs.getNames()[0]), attrs.getValue(attrs.getNames()[0])

  def endElement(self, name):
    # attributes
    if name=="origin":
      self.stack[].setOrigin(self.getCharacters())
    elif name == "version":
      self.current.setVersion(self.getCharacters())
    elif name == "name":
      self.current.setName(self.getCharacters())
    elif name == "type":
      self.current.setType(self.getCharacters())
    elif name == "required":
      self.current.setRequired(self.getCharacters())
    elif name == "descr_short":
      self.current.setDescrShort(self.getCharacters())
    elif name == "name":
      self.current.setName(self.getCharacters())
    elif name == "type":
      self.current.setType(self.getCharacters())

    #objects
    elif name=="Workflow":
      pass
    elif name == "StepDefinition":
      pass
    elif name == "StepInstance":
      pass
    elif name == "ModuleDefinition":
      pass
    elif name == "ModuleInstance":
      pass
    else:
      print "endElement", name

  def getCharacters(self):
    # combine all strings and clear the list
    ret = ''.join(self.strings)
    self.clearCharacters()
    return ret

  def clearCharacters(self):
    del self.strings
    self.strings=[]

  def characters(self, content):
    print "characters", content
    self.strings.append(content)

def fromXMLString(xml_string):
  #parser = xml.sax.make_parser()
  handler = WorkflowXMLHandler()
  xml.sax.parseString(xml_string, handler)

  #print xml_string
  #print xml.dom.getDOMImplementation()
  return handler.root