''' This is a comment
'''

from xml.sax.handler import ContentHandler

from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Core.Workflow.Module import ModuleInstance, ModuleDefinition
from DIRAC.Core.Workflow.Step import StepDefinition, StepInstance
from DIRAC.Core.Workflow.Workflow import Workflow


class WorkflowXMLHandler( ContentHandler ):

  def __init__( self, new_wf = None ):
    ''' If new_wf defined, it will be used as root of document '''
    # this is an attribute for the object to be created from the XML document
    self.root = new_wf  # the reference on the all document
    self.stack = None  # to keep last object
    self.strings = None  # to accumulate string object (list of strings) used to split long string

  def startDocument( self ):
    # reset the process
    # self.root=None
    self.stack = []
    self.strings = []

  def endDocument( self ):
    pass

  def startElement( self, name, attrs ):
    # print name ,"startElement", "attr=", attrs.getLength(), attrs.getNames()
    self.clearCharacters()  # clear to remove empty or nonprintable characters

    if name == "Workflow":
      if self.root == None:  # if root not defined by constractor
        self.root = Workflow()
      self.stack.append( self.root )

    elif name == "StepDefinition":
      obj = StepDefinition( "TemporaryXMLObject_StepDefinition" )
      if self.root == None:  # in case we are saving Step only
        self.root = obj
      self.stack.append( obj )

    elif name == "StepInstance":
      obj = StepInstance( "TemporaryXMLObject_StepInstance" )
      self.stack.append( obj )

    elif name == "ModuleDefinition":
      obj = ModuleDefinition( "TemporaryXMLObject_ModuleDefinition" )
      if self.root == None:  # in case we are saving Module only
        self.root = obj
      self.stack.append( obj )

    elif name == "ModuleInstance":
      obj = ModuleInstance( "TemporaryXMLObject_ModuleInstance" )
      self.stack.append( obj )

    elif name == "Parameter":
      obj = Parameter( str( attrs['name'] ), None, str( attrs['type'] ), str( attrs['linked_module'] ), str( attrs['linked_parameter'] ), str( attrs['in'] ), str( attrs['out'] ), str( attrs['description'] ) )
      self.stack.append( obj )

    # TEMPORARY CODE
    elif name == "origin" or name == "version" or name == "name" or name == "type" or name == "value" or\
    name == "required" or name == "descr_short" or name == "name" or name == "type"  or name == "description"  or name == "body":
      pass
    else:
      print "UNTREATED! startElement name=", name, "attr=", attrs.getLength(), attrs.getNames()
      pass

  def endElement( self, name ):
    # print name, "endElement"
    # attributes
    if name == "origin":
      self.stack[len( self.stack ) - 1].setOrigin( self.getCharacters() )
    elif name == "version":
      self.stack[len( self.stack ) - 1].setVersion( self.getCharacters() )
    elif name == "name":
      self.stack[len( self.stack ) - 1].setName( self.getCharacters() )
    elif name == "type":
      self.stack[len( self.stack ) - 1].setType( self.getCharacters() )
    elif name == "required":
      self.stack[len( self.stack ) - 1].setRequired( self.getCharacters() )
    elif name == "descr_short":
      self.stack[len( self.stack ) - 1].setDescrShort( self.getCharacters() )
    elif name == "name":
      self.stack[len( self.stack ) - 1].setName( self.getCharacters() )
    elif name == "type":
      self.stack[len( self.stack ) - 1].setType( self.getCharacters() )
    elif name == "description":
      self.stack[len( self.stack ) - 1].setDescription( self.getCharacters() )
    elif name == "body":
      self.stack[len( self.stack ) - 1].setBody( self.getCharacters() )
    elif name == "value":
      ch = self.getCharacters()
      # to keep compatibility with the old version
      # were """ was not used for the string
      if self.stack[len( self.stack ) - 1].isTypeString():
        self.stack[len( self.stack ) - 1].setValue( ch )
      else:
        self.stack[len( self.stack ) - 1].setValue( eval( ch ) )

    # objects
    elif name == "Workflow":
      self.stack.pop()
    elif name == "StepDefinition":
      self.root.step_definitions.append( self.stack.pop() )
    elif name == "StepInstance":
      self.root.step_instances.append( self.stack.pop() )
    elif name == "ModuleDefinition":
      self.root.addModule( self.stack.pop() )
    elif name == "ModuleInstance":
      obj = self.stack.pop()
      self.stack[len( self.stack ) - 1].module_instances.append( obj )
    elif name == "Parameter":
      obj = self.stack.pop();
      self.stack[len( self.stack ) - 1].addParameter( obj )
    else:
      print "UNTREATED! endElement", name

  def getCharacters( self ):
    # combine all strings and clear the list
    ret = ''.join( self.strings )
    self.clearCharacters()
    return str( ret )

  def clearCharacters( self ):
    del self.strings
    self.strings = []

  def characters( self, content ):
    self.strings.append( content )


