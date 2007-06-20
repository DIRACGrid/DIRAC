# $Id: WorkflowReader.py,v 1.2 2007/06/20 15:23:03 gkuznets Exp $
"""
    This is a comment
"""
__RCSID__ = "$Revision: 1.2 $"

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
      self.stack.append(self.root)
    elif name == "StepDefinition":
      obj = StepDefinition()
      self.root.addStep(obj)
      self.stack.append(obj)
    elif name == "StepInstance":
      #obj = self.root.createStepInstance()
      #self.root.addStep(obj)
      #self.stack.append(obj)
      pass
    elif name == "ModuleDefinition":
      obj = ModuleDefinition()
      self.root.addModule(obj)
      self.stack.append(obj)
      pass
    elif name == "ModuleInstance":
      #self.root.addStep(obj)
      #self.stack.append(obj)
      pass
    else:
      print "startElement", name, attrs
      print attrs.getLength(), attrs.getNames()
    #print attrs.getType(attrs.getNames()[0]), attrs.getValue(attrs.getNames()[0])

  def endElement(self, name):
    # attributes
    if name=="origin":
      self.stack[len(self.stack)-1].setOrigin(self.getCharacters())
    elif name == "version":
      self.stack[len(self.stack)-1].setVersion(self.getCharacters())
    elif name == "name":
      self.stack[len(self.stack)-1].setName(self.getCharacters())
    elif name == "type":
      self.stack[len(self.stack)-1].setType(self.getCharacters())
    elif name == "required":
      self.stack[len(self.stack)-1].setRequired(self.getCharacters())
    elif name == "descr_short":
      self.stack[len(self.stack)-1].setDescrShort(self.getCharacters())
    elif name == "name":
      self.stack[len(self.stack)-1].setName(self.getCharacters())
    elif name == "type":
      self.stack[len(self.stack)-1].setType(self.getCharacters())

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