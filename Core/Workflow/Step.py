# $Id: Step.py,v 1.14 2007/12/10 23:59:33 gkuznets Exp $
"""
    This is a comment
"""
__RCSID__ = "$Revision: 1.14 $"

import os
#try: # this part to inport as part of the DIRAC framework
from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
#except: # this part is to import code without DIRAC
#  from Parameter import *
#  from Module import *

class StepDefinition(AttributeCollection):

    def __init__(self, type=None, obj=None, parent=None):
        AttributeCollection.__init__(self)
        self.module_instances = None
        # this object can be shared with the workflow
        # to if its =None and workflow!=None we have to
        # pass everything above
        self.module_definitions = None
        self.parent = parent

        # sort out Parameters and class attributes
        if (obj == None) or isinstance(obj, ParameterCollection):
            self.setType(type)
            self.setDescrShort('')
            self.setDescription('')
            self.setOrigin('')
            self.setVersion(0.0)
            self.parameters = ParameterCollection(obj) # creating copy
            self.module_instances = InstancesPool(self)
            self.module_definitions = DefinitionsPool(self)
        elif isinstance(obj, StepDefinition):
            if type == None:
                self.setType(obj.getType())
            else:
                self.setType(type)
            self.setDescrShort(obj.getDescrShort())
            self.setDescription(obj.getDescription())
            self.setOrigin(obj.getOrigin())
            self.setVersion(obj.getVersion())
            # copy instances and definitions
            self.parameters = ParameterCollection(self, obj.parameters)
            self.module_instances = InstancesPool(self, obj.module_instances)
            if obj.module_definitions != None:
                self.module_definitions = DefinitionsPool(self. obj.module_definitions)
        else:
            raise TypeError('Can not create object type '+ str(type(self)) + ' from the '+ str(type(obj)))

    def __str__(self):
        ret =  str(type(self))+':\n'+ AttributeCollection.__str__(self) + self.parameters.__str__()
        if self.module_definitions != None:
            ret = ret + str(self.module_definitions)
        else:
            ret = ret + 'Module definitions shared in Workflow\n'
        ret = ret + str(self.module_instances)
        return ret


    def toXML(self):
        ret = '<StepDefinition>\n'
        ret = ret + AttributeCollection.toXML(self)
        ret = ret + self.parameters.toXML()
        if self.module_definitions != None:
            ret = ret + self.module_definitions.toXML()
        ret = ret + self.module_instances.toXML()
        ret = ret + '</StepDefinition>\n'
        return ret

    def addModule(self, module):
        # KGG We need to add code to update existing modules
        if self.module_definitions == None:
            parents.module_definitions.append(module)
        else:
            self.module_definitions.append(module)
        return module

    def createModuleInstance(self, type, name):
        """ Creates module instance of type 'type' with the name 'name'
        """
        if self.module_definitions[type]:
            mi = ModuleInstance(name, self.module_definitions[type])
            self.module_instances.append(mi)
            return mi
        else:
            raise KeyError('Can not find ModuleDefinition '+ type+' to create ModuleInstrance '+name)

    def removeModuleInstance(self, name):
        self.module_instances.delete(name)

    def compare(self, s):
        ret = AttributeCollection.compare(self,s) and self.module_instances.compare(s)
        if self.module_definitions.getOwner() == self:
            ret = ret and self.module_definitions.compare(s)
        return ret

    def updateParent(self, parent):
        AttributeCollection.updateParents(self, parents)
        self.module_instances.updateParent(self)
        if( module_definitions != None ):
            module_definitions.updateParent(self)

    def createCode(self):
        str='class '+self.getType()+ ':\n'
        str=str+indent(1)+'def execute(self):\n'
        str=str+self.module_instances.createCode()
        str=str+indent(2)+'# output assignment\n'
        for v in self.parameters:
            if v.isOutput():
                str=str+v.createParameterCode(2,'self')
        return str


class StepInstance(AttributeCollection):

    def __init__(self, name, obj=None, parent=None):
        AttributeCollection.__init__(self)
        self.parent = None

        if obj == None:
          self.parameters = ParameterCollection()
        elif isinstance(obj, StepInstance) or isinstance(obj, StepDefinition):
            if name == None:
                self.setName(obj.getName())
            else:
                self.setName(name)
            self.setType(obj.getType())
            self.setDescrShort(obj.getDescrShort())
            self.parameters = ParameterCollection(obj.parameters)
        elif (obj == None) or isinstance(obj, ParameterCollection):
            # set attributes
            self.setName(name)
            self.setType("")
            self.setDescrShort("")
            self.parameters = ParameterCollection(obj)
        elif coll != None:
            raise TypeError('Can not create object type '+ str(type(self)) + ' from the '+ str(type(opt)))


    def resolveGlobalVars(self, step_definitions, wf_parameters):
        self.parameters.resolveGlobalVars(wf_parameters)
        for inst in step_definitions[self.getType()].module_instances:
            inst.resolveGlobalVars(wf_parameters, self.parameters)

    def createCode(self, ind=2):
        str=indent(ind)+self.getName()+' = '+self.getType()+ '()\n'
        str=str+self.parameters.createParametersCode(ind, self.getName())
        str=str+indent(ind)+self.getName()+'.execute()\n\n'
        return str

    def __str__(self):
        return str(type(self))+':\n'+ AttributeCollection.__str__(self) + self.parameters.__str__()

    def toXML(self):
        ret = '<StepInstance>\n'
        ret = ret + AttributeCollection.toXML(self)
        ret = ret + self.parameters.toXML()
        ret = ret + '</StepInstance>\n'
        return ret

    def toXMLFile(self, outFile):
        if os.path.exists(outFile):
          os.remove(outFile)
        xmlfile = open(outFile, 'w')
        xmlfile.write(self.toXML())
        xmlfile.close()

    def execute(self, step_exec_attr, definitions):
        """step_exec_attr is array to hold parameters belong to this Step, filled above """
        #print 'Executing StepInstance',self.getName(),'of type',self.getType(), definitions.keys()
        step_def = definitions[self.getType()]
        step_exec_modules={}
        for mod_inst in step_def.module_instances:
            mod_inst_name = mod_inst.getName()
            print "StepInstance creating module instance ",mod_inst_name," of type", mod_inst.getType()
            # since during execution Step is inside Workflow the  step_def.module_definitions == None
            #step_exec_modules[mod_inst_name] = step_def.module_definitions[mod_inst.getType()].main_class_obj() # creating instance
            step_exec_modules[mod_inst_name] = step_def.parent.module_definitions[mod_inst.getType()].main_class_obj() # creating instance

            # add some mandatory attributes to the instance
            setattr(step_exec_modules[mod_inst_name], 'MODULE' , mod_inst)
            setattr(step_exec_modules[mod_inst_name], 'MODULE_INSTANCE_NAME' , mod_inst_name)
            setattr(step_exec_modules[mod_inst_name], 'MODULE_DEFINITION_NAME' , mod_inst.getType())
            setattr(step_exec_modules[mod_inst_name], 'STEP' , self)
            setattr(step_exec_modules[mod_inst_name], 'STEP_INSTANCE_NAME' , self.getName())
            setattr(step_exec_modules[mod_inst_name], 'STEP_DEFINITION_NAME' , self.getType())
            #setattr(step_exec_modules[mod_inst_name], 'WF' , self.parent)
            #setattr(step_exec_modules[mod_inst_name], 'WF_NAME' , self.parent.getName())

            for parameter in mod_inst.parameters:
                if parameter.preExecute():
                    if parameter.isLinked():
                        #print "ModuleInstance",mod_inst_name+'.'+parameter.getName(),'=',parameter.getLinkedModule()+'.'+parameter.getLinkedParameter()
                        if parameter.getLinkedModule() == 'self':
                            # tale value form the step_dict
                            setattr(step_exec_modules[mod_inst_name], parameter.getName(), step_exec_attr[parameter.getLinkedParameter()])
                        else:
                            setattr(step_exec_modules[mod_inst_name], parameter.getName(), getattr(step_exec_modules[parameter.getLinkedModule()], parameter.getLinkedParameter()))
                    else:
                        setattr(step_exec_modules[mod_inst_name], parameter.getName(), parameter.getValue())
                        #print "ModuleInstance", mod_inst_name+'.'+parameter.getName(),'=',parameter.getValue()
            # Execution
            step_exec_modules[mod_inst_name].execute()

        # now we need to copy output values to the STEP!!! parameters
        #print "output assignment"
        for st_parameter in self.parameters:
            if st_parameter.isOutput():
                if st_parameter.isLinked():
                    #print "StepInstance this."+ st_parameter.getName(),'=',st_parameter.getLinkedModule()+'.'+st_parameter.getLinkedParameter()
                    if st_parameter.getLinkedModule() == 'self':
                        # this is not supose to happen
                        print "Warning! Step OUTPUT attribute", st_parameter.getName(), "refer on the attribute of the same step", st_parameter.getLinkedParameter()
                        step_exec_attr[st_parameter.getName()] = step_exec_attr[st_parameter.getLinkedParameter()]
                    else:
                        step_exec_attr[st_parameter.getName()] = getattr(step_exec_modules[st_parameter.getLinkedModule()], st_parameter.getLinkedParameter())
                else:
                    # it is also does not make scence - we can produce wanging
                    print "Warning! Step OUTPUT attribute ", st_parameter.getName(), "asigned constant", st_parameter.getValue()
                    step_exec_attr[st_parameter.getName()] = st_parameter.getValue()
                    #print "StepInstance this."+ st_parameter.getName(),'=',st_parameter.getValue()

