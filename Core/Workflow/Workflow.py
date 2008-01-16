# $Id: Workflow.py,v 1.16 2008/01/16 12:16:44 gkuznets Exp $
"""
    This is a comment
"""
__RCSID__ = "$Revision: 1.16 $"

import os
#try: # this part to inport as part of the DIRAC framework
from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
#  print "DIRAC.Core.Workflow.Parameter"
#except: # this part is to import code without DIRAC
#  from Parameter import *
#  from Module import *
#  from Step import *
#  print "Parameter"

class Workflow(AttributeCollection):
  def __init__(self, name=None, obj=None):
    AttributeCollection.__init__(self)

    # sort out parameters and class attributes
    if (obj == None) or isinstance(obj, ParameterCollection):
      self.setName(name)
      self.setType('')
      self.setDescrShort('')
      self.setDescription('')
      self.setOrigin('')
      self.setVersion(0.0)
      self.parameters = ParameterCollection(obj) # creating copy
      self.step_instances = InstancesPool(self)
      self.step_definitions = DefinitionsPool(self)
      self.module_definitions = DefinitionsPool(self)
    elif isinstance(obj, Workflow):
      if name == None:
        self.setName(obj.getName())
      else:
        self.setName(name)
      self.setType(obj.getType())
      self.setDescrShort(obj.getDescrShort())
      self.setDescription(obj.getDescription())
      self.setOrigin(obj.getOrigin())
      self.setVersion(obj.getVersion())
      # copy instances and definitions
      self.parameters = ParameterCollection(obj.parameters)
      self.module_definitions = DefinitionsPool(self, obj.module_definitions)
      self.step_instances = InstancesPool(self, obj.step_instances)
      self.step_definitions = DefinitionsPool(self, obj.step_definitions)
    else:
      raise TypeError('Can not create object type '+ str(type(self)) + ' from the '+ str(type(obj)))
    #self.module_definitions.setOwner(self)

  def __str__(self):
    """Creates a string representation of itself
    """
    ret = str(self.getName())+':\n'+ AttributeCollection.__str__(self) + self.parameters.__str__()
    ret = ret + str(self.step_definitions)
    ret = ret + str(self.step_instances)
    ret = ret + str(self.module_definitions)
    return ret

  def toXML(self):
    """Creates an XML representation of itself
    """
    # THIS is very importatnt that Definitions should be written before instances
    ret = '<Workflow>\n'
    ret = ret + AttributeCollection.toXML(self)
    ret = ret + self.parameters.toXML()
    ret = ret + self.module_definitions.toXML()
    ret = ret + self.step_definitions.toXML()
    ret = ret + self.step_instances.toXML()
    ret = ret + '</Workflow>\n'
    return ret

  def toXMLFile(self, outFile):
    if os.path.exists(outFile):
      os.remove(outFile)
    xmlfile = open(outFile, 'w')
    xmlfile.write(self.toXML())
    xmlfile.close()

  def addStep(self, step):
    # this is WERY importatnt piece of code
    # we have to joing all Modules definition from all added steps in the single dictionary
    # and we have to share whis dictionary between all included steps
    # we also have to check versions of the modules and instances
    for type in step.module_definitions.keys():
      if self.module_definitions.has_key(type):
        #we have the same ModuleDefinition in 2 places
        # we need to find way to synchronise it
        print "Workflow:addStep - we need to write ModuleDefinitions synchronisation code"
      else:
        # new module - just append it
        self.module_definitions.append(step.module_definitions[type])
    self.step_definitions.append(step)
    del step.module_definitions # we need to clean all unwanted definitions
    step.module_definitions=None
    return step

  def addModule(self, module):
    # KGG We need to add code to update existing modules
    self.module_definitions.append(module)
    return module

  def createStepInstance(self, type, name):
    """ Creates step instance of type 'type' with the name 'name'
    """
    if self.step_definitions.has_key(type):
      stepi = StepInstance(name, self.step_definitions[type])
      self.step_instances.append(stepi)
      return stepi
    else:
      raise KeyError('Can not find StepDefinition '+ type+' to create StepInstrance '+name)

  def removeStepInstance(self, name):
    self.instances[name].setParents(None)
    self.instances.delete(name)

  def updateParents(self):
    self.module_definitions.updateParents(self)
    self.step_instances.updateParents(self)
    self.step_definitions.updateParents(self)


  def resolveGlobalVars(self):
    """ This method will create global parameter list and then will resolve all instances of @{VARNAME}
    Be aware that parameters of that type are GLOBAL!!! are string and can not be dynamically change
    The scope: the resolution of that parameters apply from lower to upper object, for example if
    parameter use in module, then it checks module, then step, then workflow"""
    self.parameters.resolveGlobalVars()
    for inst in self.step_instances:
      inst.resolveGlobalVars(self.step_definitions, self.parameters)


  def createCode(self, combine_steps=False):
    str=''
    str=str+self.module_definitions.createCode()
    str=str+self.step_definitions.createCode()
    str=str+"\nclass job:\n"
    str=str+indent(1)+'def execute(self):\n'
    #str=str+indent(2)+'# flush self.step_instances\n'
    str=str+self.step_instances.createCode()
    # it seems we do not need it on this level
    str=str+indent(2)+'# output assignment\n'
    for v in self.parameters:
      if v.isOutput():
        str=str+v.createParameterCode(2,'self')

    str=str+'\nj=job()\n'
    str=str+self.parameters.createParametersCode(0,'j')
    str=str+'j.execute()'
    return str

  def execute(self):
    # define workflow attributes
    wf_exec_attr={} # dictianary with the WF attributes, used to resolve links to self.attrname
    for wf_parameter in self.parameters:
      # parameters shall see objects in the current scope order to resolve links
      if wf_parameter.preExecute(): # for parm which not just outputs
        if wf_parameter.isLinked():
          #print "Workflow self[",wf_parameter.getName(),']=',parameter.getLinkedModule()+'['+parameter.getLinkedParameter()+']'
          if wf_parameter.getLinkedModule() == 'self':
            # this is not supose to happen
            #print "Warning! Job attribute ", wf_parameter.getName(), "refer on the attribute of the same workflow", wf_parameter.getLinkedParameter()
            wf_exec_attr[wf_parameter.getName()] = wf_exec_attr[wf_parameter.getLinkedParameter()]
          else:
            wf_exec_attr[wf_parameter.getName()] = wf_exec_attr[wf_parameter.getLinkedModule()][wf_parameter.getLinkedParameter()]
        else:
          #print "Workflow self[",wf_parameter.getName(),']=',wf_parameter.getValue()
          wf_exec_attr[wf_parameter.getName()] = wf_parameter.getValue()

    self.module_definitions.loadCode() # loading Module classes into current python scope

    #wf_exec_steps will be dictianary of dictionaries [step instance name][parameter name]
    # used as dictionary of step instances to carry parameters
    wf_exec_steps={}
    #print 'Executing Workflow',self.getType()
    for step_inst in self.step_instances:
      step_inst_name = step_inst.getName()
      wf_exec_steps[step_inst_name] = {}
      #print "WorkflowInstance creating Step instance ",step_inst_name," of type", step_inst.getType()
      for parameter in step_inst.parameters:
        if parameter.preExecute():
          if parameter.isLinked():
            #print "StepInstance", step_inst_name+'['+parameter.getName(),']=',parameter.getLinkedModule()+'['+parameter.getLinkedParameter()+']'
            if parameter.getLinkedModule() == 'self':
              # tale value form the step_dict
              wf_exec_steps[step_inst_name][parameter.getName()] = wf_exec_attr[parameter.getLinkedParameter()]
            else:
              wf_exec_steps[step_inst_name][parameter.getName()]= wf_exec_steps[parameter.getLinkedModule()][parameter.getLinkedParameter()]
          else:
            wf_exec_steps[step_inst_name][parameter.getName()]=parameter.getValue()
            #print "StepInstance", step_inst_name+'.'+parameter.getName(),'=',parameter.getValue()
      step_inst.execute(wf_exec_steps[step_inst_name], self.step_definitions)

    # now we need to copy output values to the STEP!!! parameters
    #print "WorkflowInstance output assignment"
    for wf_parameter in self.parameters:
      if wf_parameter.isOutput():
        if wf_parameter.isLinked():
          #print "WorkflowInstance  self."+ wf_parameter.getName(),'=',wf_parameter.getLinkedModule()+'.'+wf_parameter.getLinkedParameter()
          if wf_parameter.getLinkedModule() == 'self':
            # this is not supose to happen
            #print "Warning! Workflow OUTPUT attribute ", wf_parameter.getName(), "refer on the attribute of the same workflow", wf_parameter.getLinkedParameter()
            wf_exec_attr[wf_parameter.getName()] = wf_exec_attr[wf_parameter.getLinkedParameter()]
          else:
            wf_exec_attr[wf_parameter.getName()] = wf_exec_steps[wf_parameter.getLinkedModule()][wf_parameter.getLinkedParameter()]
        else:
          # it is also does not make scence - we can produce warning
          #print "Warning! Workflow OUTPUT attribute", wf_parameter.getName(), "asigned constant", wf_parameter.getValue()
          wf_exec_attr[wf_parameter.getName()] = wf_parameter.getValue()
          #print "WorkflowInstance  self."+ wf_parameter.getName(),'=',wf_parameter.getValue()

