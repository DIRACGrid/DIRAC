# $Id: WorkflowReader.py,v 1.6 2007/10/23 15:41:56 gkuznets Exp $
"""
    This is a comment
"""
__RCSID__ = "$Revision: 1.6 $"

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
    self.root=None # the reference on the all document
    self.stack=None # to keep last object
    self.strings=None # to accumulate string object (list of strings) used to split long string

  def startDocument(self):
    #reset the process
    self.root=None
    self.stack=[]
    self.strings=[]
  def endDocument(self):
    pass

  def startElement(self, name, attrs):
    #print name ,"startElement", "attr=", attrs.getLength(), attrs.getNames()
    self.clearCharacters() # clear to remove empty or nonprintable characters

    if name == "Workflow":
      self.root = Workflow()
      self.stack.append(self.root)

    elif name == "StepDefinition":
      obj = StepDefinition()
      if self.root == None: # in case we are saving Step only
        self.root = obj
      else:
        self.root.addStep(obj)
      self.stack.append(obj)

    elif name == "StepInstance":
      obj = StepDefinition("")
      self.stack[len(self.stack)-1].step_instances.append(obj)
      self.stack.append(obj)

    elif name == "ModuleDefinition":
      obj = ModuleDefinition("TemporaryXMLReadingObject")
      if self.root == None: # in case we are saving Module only
        self.root = obj
      else:
        self.root.addModule(obj)
      self.stack.append(obj)

    elif name == "ModuleInstance":
      obj = ModuleInstance("")
      self.stack[len(self.stack)-1].module_instances.append(obj)
      self.stack.append(obj)

    elif name == "Parameter":
      obj = Parameter(attrs['name'], None, attrs['type'], attrs['linked_module'], attrs['linked_parameter'], attrs['in'], attrs['out'], None)
      self.stack.append(obj)

    # TEMPORARY CODE
    elif name=="origin" or name == "version" or name == "name" or name == "type" or name == "value" or\
    name == "required" or name == "descr_short" or name == "name" or name == "type"  or name == "description"  or name == "body" or name == "Parameter":
      pass
    else:
      print "UNTREATED! startElement name=", name, "attr=", attrs.getLength(), attrs.getNames()
      pass

  def endElement(self, name):
    #print name, "endElement"
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
    elif name == "description":
      self.stack[len(self.stack)-1].setDescription(self.getCharacters())
    elif name == "body":
      self.stack[len(self.stack)-1].setBody(self.getCharacters())
    elif name == "value":
      self.stack[len(self.stack)-1].setValue(self.getCharacters())

    #objects
    elif name=="Workflow":
      self.stack.pop()
    elif name == "StepDefinition":
      self.stack.pop()
    elif name == "StepInstance":
      self.stack.pop()
    elif name == "ModuleDefinition":
      self.stack.pop()
    elif name == "ModuleInstance":
      self.stack.pop()
    elif name == "Parameter":
      self.stack.pop()
    else:
      print "UNTREATED! endElement", name

  def getCharacters(self):
    # combine all strings and clear the list
    ret = ''.join(self.strings)
    self.clearCharacters()
    return ret

  def clearCharacters(self):
    del self.strings
    self.strings=[]

  def characters(self, content):
    self.strings.append(content)

def fromXMLString(xml_string):
  handler = WorkflowXMLHandler()
  xml.sax.parseString(xml_string, handler)
  return handler.root

def fromXMLFile(xml_file):
  handler = WorkflowXMLHandler()
  xml.sax.parse(xml_file, handler)
  return handler.root

