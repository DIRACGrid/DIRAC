# $Id: Step.py,v 1.25 2008/06/09 13:36:43 atsareg Exp $
"""
    This is a comment
"""
__RCSID__ = "$Revision: 1.25 $"

import os, time, types, traceback, sys
#try: # this part to inport as part of the DIRAC framework
from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC import S_OK, S_ERROR
#except: # this part is to import code without DIRAC
#  from Parameter import *
#  from Module import *

class StepDefinition(AttributeCollection):

    def __init__(self, step_type=None, obj=None, parent=None):
        AttributeCollection.__init__(self)
        self.module_instances = None
        # this object can be shared with the workflow
        # to if its =None and workflow!=None we have to
        # pass everything above
        self.module_definitions = None
        self.parent = parent

        # sort out Parameters and class attributes
        if (obj == None) or isinstance(obj, ParameterCollection):
            self.setType('notgiven')
            self.setDescrShort('')
            self.setDescription('')
            self.setOrigin('')
            self.setVersion(0.0)
            self.parameters = ParameterCollection(obj) # creating copy
            self.module_instances = InstancesPool(self)
            self.module_definitions = DefinitionsPool(self)
        elif isinstance(obj, StepDefinition):
            self.setType(obj.getType())
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
        if step_type :
          self.setType(step_type)

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

    def toXMLFile(self, outFile):
        if os.path.exists(outFile):
          os.remove(outFile)
        xmlfile = open(outFile, 'w')
        xmlfile.write(self.toXML())
        xmlfile.close()

    def addModule(self, module):
        # KGG We need to add code to update existing modules
        if self.module_definitions == None:
            parents.module_definitions.append(module)
        else:
            self.module_definitions.append(module)
        return module

    def createModuleInstance(self, module_type, name):
        """ Creates module instance of type 'type' with the name 'name'
        """

        if self.module_definitions[module_type]:
            mi = ModuleInstance(name, self.module_definitions[module_type])
            self.module_instances.append(mi)
            return mi
        else:
            raise KeyError('Can not find ModuleDefinition '+ module_type+' to create ModuleInstrance '+name)

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

        self.step_commons = {}
        self.stepStatus = S_OK()

    def resolveGlobalVars(self, step_definitions, wf_parameters):
        self.parameters.resolveGlobalVars(wf_parameters)
        module_instance_number=0
        for inst in step_definitions[self.getType()].module_instances:
            module_instance_number=module_instance_number+1
            if not inst.parameters.find("MODULE_NUMBER"):
              inst.parameters.append(Parameter("MODULE_NUMBER","%s"%module_instance_number,"string","","",True,False,"Number of the ModuleInstance within the Step"))
            if not inst.parameters.find("MODULE_INSTANCE_NAME"):
              inst.parameters.append(Parameter("MODULE_INSTANCE_NAME",inst.getName(),"string","","",True,False,"Name of the ModuleInstance within the Step"))
            if not inst.parameters.find("MODULE_DEFINITION_NAME"):
              inst.parameters.append(Parameter("MODULE_DEFINITION_NAME",inst.getType(),"string","","",True,False,"Type of the ModuleInstance within the Step"))
            if not inst.parameters.find("JOB_ID"):
              inst.parameters.append(Parameter("JOB_ID","","string","self","JOB_ID",True,False,"Type of the ModuleInstance within the Step"))
            if not inst.parameters.find("PRODUCTION_ID"):
              inst.parameters.append(Parameter("PRODUCTION_ID","","string","self","PRODUCTION_ID",True,False,"Type of the ModuleInstance within the Step"))
            if not inst.parameters.find("STEP_NUMBER"):
              inst.parameters.append(Parameter("STEP_NUMBER","","string","self","STEP_NUMBER",True,False,"Type of the ModuleInstance within the Step"))
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

    def setWorkflowCommons(self,wf):
      """ Add reference to the collection of the common tools
      """

      self.workflow_commons = wf

    def execute(self, step_exec_attr, definitions):
        """step_exec_attr is array to hold parameters belong to this Step, filled above """
        print 'Executing StepInstance',self.getName(),'of type',self.getType(), definitions.keys()
        if self.workflow_commons.has_key('JobReport'):
          result = self.workflow_commons['JobReport'].setApplicationStatus('Executing '+self.getName())
        self.step_commons['StartTime'] = time.time()
        self.step_commons['StartStats'] = os.times()
        step_def = definitions[self.getType()]
        step_exec_modules={}
        error_message = ''
        for mod_inst in step_def.module_instances:
            mod_inst_name = mod_inst.getName()
            mod_inst_type = mod_inst.getType()

            print "StepInstance creating module instance ",mod_inst_name," of type", mod_inst.getType()
            # since during execution Step is inside Workflow the  step_def.module_definitions == None
            #step_exec_modules[mod_inst_name] = step_def.module_definitions[mod_inst_type].main_class_obj() # creating instance
            step_exec_modules[mod_inst_name] = step_def.parent.module_definitions[mod_inst_type].main_class_obj() # creating instance

            # add some mandatory attributes to the instance
            # moved to the resolveGlobalVars
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

            # Set reference to the workflow and step common tools
            setattr(step_exec_modules[mod_inst_name], 'workflow_commons', self.parent.workflow_commons)
            setattr(step_exec_modules[mod_inst_name], 'step_commons', self.step_commons)
            setattr(step_exec_modules[mod_inst_name], 'stepStatus', self.stepStatus)
            setattr(step_exec_modules[mod_inst_name], 'workflowStatus', self.parent.workflowStatus)

            try:
              result = step_exec_modules[mod_inst_name].execute()
              if not result['OK']:
                if self.stepStatus['OK']:
                  error_message = result['Message']
                  if self.workflow_commons.has_key('JobReport'):
                    result = self.workflow_commons['JobReport'].setApplicationStatus(error_message)
                self.stepStatus = S_ERROR(result['Message'])
              else:
                # Get output values to the step_commons dictionary
                for key in result.keys():
                  if key != "OK":
                    if key != "Value":
                      self.step_commons[key] = result[key]
                    elif type(result['Value']) == types.DictType:
                      for vkey in result['Value'].keys():
                        self.step_commons[key] = result['Value'][key]

            except Exception, x:
              print "Exception while module execution"
              print "Module",mod_inst_name,mod_inst.getType()
              print str(x)
              exc = sys.exc_info()
              exc_type = exc[0]
              value = exc[1]
              print "== EXCEPTION ==\n%s: %s\n\n%s===============" % (
                         exc_type,
                         value,
                         "\n".join(traceback.format_tb(exc[2])))

              if self.stepStatus['OK']:
                # This is the error that caused the workflow disruption
                # report it to the WMS
                error_message = 'Exception while %s module execution: %s' % (mod_inst_name,str(x))
                if self.workflow_commons.has_key('JobReport'):
                  result = self.workflow_commons['JobReport'].setApplicationStatus(error_message)

              self.stepStatus = S_ERROR(error_message)

        # now we need to copy output values to the STEP!!! parameters
        #print "output assignment"
        for st_parameter in self.parameters:
            if st_parameter.isOutput():
                if st_parameter.isLinked():
                    #print "StepInstance this."+ st_parameter.getName(),'=',st_parameter.getLinkedModule()+'.'+st_parameter.getLinkedParameter()
                    if st_parameter.getLinkedModule() == 'self':
                        # this is not supposed to happen
                        print "Warning! Step OUTPUT attribute", st_parameter.getName(), "refer on the attribute of the same step", st_parameter.getLinkedParameter()
                        step_exec_attr[st_parameter.getName()] = step_exec_attr[st_parameter.getLinkedParameter()]
                    else:
                        step_exec_attr[st_parameter.getName()] = getattr(step_exec_modules[st_parameter.getLinkedModule()], st_parameter.getLinkedParameter())
                else:
                    # it is also does not make scence - we can produce wanging
                    print "Warning! Step OUTPUT attribute ", st_parameter.getName(), "asigned constant", st_parameter.getValue()
                    step_exec_attr[st_parameter.getName()] = st_parameter.getValue()
                    #print "StepInstance this."+ st_parameter.getName(),'=',st_parameter.getValue()

        # Return the result of the first failed module or S_OK
        if not self.stepStatus['OK']:
          return S_ERROR(error_message)
        else:
          return S_OK(result['Value'])
