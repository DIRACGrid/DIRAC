""" Implementation of a step
"""
# pylint: disable=unused-wildcard-import,wildcard-import

import os
import time
import traceback
import sys

from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC import S_OK, S_ERROR


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
        if (obj is None) or isinstance(obj, ParameterCollection):
            self.setType("notgiven")
            self.setDescrShort("")
            self.setDescription("")
            self.setOrigin("")
            self.setVersion(0.0)
            self.parameters = ParameterCollection(obj)  # creating copy
            self.module_instances = InstancesPool(self)
            self.module_definitions = DefinitionsPool(self)
        elif isinstance(obj, StepDefinition):
            self.setType(obj.getType())
            self.setDescrShort(obj.getDescrShort())
            self.setDescription(obj.getDescription())
            self.setOrigin(obj.getOrigin())
            self.setVersion(obj.getVersion())
            # copy instances and definitions
            self.parameters = ParameterCollection(obj.parameters)
            self.module_instances = InstancesPool(self, obj.module_instances)
            if obj.module_definitions is not None:
                self.module_definitions = DefinitionsPool(obj.module_definitions)
        else:
            raise TypeError("Can not create object type " + str(type(self)) + " from the " + str(type(obj)))
        if step_type:
            self.setType(step_type)

    def __str__(self):
        ret = str(type(self)) + ":\n" + AttributeCollection.__str__(self) + self.parameters.__str__()
        if self.module_definitions is not None:
            ret = ret + str(self.module_definitions)
        else:
            ret = ret + "Module definitions shared in Workflow\n"
        ret = ret + str(self.module_instances)
        return ret

    def toXML(self):
        ret = "<StepDefinition>\n"
        ret = ret + AttributeCollection.toXML(self)
        ret = ret + self.parameters.toXML()
        if self.module_definitions is not None:
            ret = ret + self.module_definitions.toXML()
        ret = ret + self.module_instances.toXML()
        ret = ret + "</StepDefinition>\n"
        return ret

    def toXMLFile(self, outFile):
        if os.path.exists(outFile):
            os.remove(outFile)
        with open(outFile, "w") as xmlfile:
            xmlfile.write(self.toXML())

    def addModule(self, module):
        # KGG We need to add code to update existing modules
        if self.module_definitions is None:
            self.parent.module_definitions.append(module)
        else:
            self.module_definitions.append(module)
        return module

    def createModuleInstance(self, module_type, name):
        """Creates module instance of type 'type' with the name 'name'"""

        if self.module_definitions[module_type]:
            mi = ModuleInstance(name, self.module_definitions[module_type])
            self.module_instances.append(mi)
            return mi
        else:
            raise KeyError("Can not find ModuleDefinition " + module_type + " to create ModuleInstrance " + name)

    def removeModuleInstance(self, name):
        """Remove module instance specified by its name"""
        self.module_instances.delete(name)

    def compare(self, s):
        """Custom Step comparison operation"""
        return AttributeCollection.compare(self, s) and self.module_instances.compare(s)

    def updateParent(self, parent):
        """ """
        # FIXME: no updateParents for AttributeCollection, What should this be?
        AttributeCollection.updateParents(self, parent)  # pylint: disable=no-member
        self.module_instances.updateParents(self)
        if self.module_definitions is not None:
            self.module_definitions.updateParents(self)

    def createCode(self):
        """Create Step code"""

        str = "class " + self.getType() + ":\n"
        str = str + indent(1) + "def execute(self):\n"
        str = str + self.module_instances.createCode()
        str = str + indent(2) + "# output assignment\n"
        for v in self.parameters:
            if v.isOutput():
                str = str + v.createParameterCode(2, "self")
        str += "\n"
        return str


class StepInstance(AttributeCollection):
    def __init__(self, name, obj=None, parent=None):
        AttributeCollection.__init__(self)
        self.parent = None

        if obj is None:
            self.parameters = ParameterCollection()
        elif isinstance(obj, StepInstance) or isinstance(obj, StepDefinition):
            if name is None:
                self.setName(obj.getName())
            else:
                self.setName(name)
            self.setType(obj.getType())
            self.setDescrShort(obj.getDescrShort())
            self.parameters = ParameterCollection(obj.parameters)
        elif (obj is None) or isinstance(obj, ParameterCollection):
            # set attributes
            self.setName(name)
            self.setType("")
            self.setDescrShort("")
            self.parameters = ParameterCollection(obj)
        elif obj is not None:
            raise TypeError("Can not create object type " + str(type(self)) + " from the " + str(type(obj)))

        self.step_commons = {}
        self.stepStatus = S_OK()

    def resolveGlobalVars(self, step_definitions, wf_parameters):
        """Resolve parameter values defined in the @{<variable>} form"""
        self.parameters.resolveGlobalVars(wf_parameters)
        module_instance_number = 0
        for inst in step_definitions[self.getType()].module_instances:
            module_instance_number = module_instance_number + 1
            if not inst.parameters.find("MODULE_NUMBER"):
                inst.parameters.append(
                    Parameter(
                        "MODULE_NUMBER",
                        f"{module_instance_number}",
                        "string",
                        "",
                        "",
                        True,
                        False,
                        "ModuleInstance number within the Step",
                    )
                )
            if not inst.parameters.find("MODULE_INSTANCE_NAME"):
                inst.parameters.append(
                    Parameter(
                        "MODULE_INSTANCE_NAME",
                        inst.getName(),
                        "string",
                        "",
                        "",
                        True,
                        False,
                        "Name of the ModuleInstance within the Step",
                    )
                )
            if not inst.parameters.find("MODULE_DEFINITION_NAME"):
                inst.parameters.append(
                    Parameter(
                        "MODULE_DEFINITION_NAME",
                        inst.getType(),
                        "string",
                        "",
                        "",
                        True,
                        False,
                        "Type of the ModuleInstance within the Step",
                    )
                )
            if not inst.parameters.find("JOB_ID"):
                inst.parameters.append(
                    Parameter(
                        "JOB_ID", "", "string", "self", "JOB_ID", True, False, "Job ID within a Production as a string"
                    )
                )
            if not inst.parameters.find("PRODUCTION_ID"):
                inst.parameters.append(
                    Parameter(
                        "PRODUCTION_ID", "", "string", "self", "PRODUCTION_ID", True, False, "Production ID as a string"
                    )
                )
            if not inst.parameters.find("STEP_NUMBER"):
                inst.parameters.append(
                    Parameter(
                        "STEP_NUMBER",
                        "",
                        "string",
                        "self",
                        "STEP_NUMBER",
                        True,
                        False,
                        "Step instance number within the Workflow",
                    )
                )
            if not inst.parameters.find("STEP_ID"):
                inst.parameters.append(
                    Parameter(
                        "STEP_ID", "", "string", "self", "STEP_NUMBER", True, False, "Step ID within the Workflow"
                    )
                )
            inst.resolveGlobalVars(wf_parameters, self.parameters)

    def createCode(self, ind=2):
        """Create the Step code"""
        str = indent(ind) + self.getName() + " = " + self.getType() + "()\n"
        str = str + self.parameters.createParametersCode(ind, self.getName())
        str = str + indent(ind) + self.getName() + ".execute()\n\n"
        return str

    def __str__(self):
        """Step string representation"""
        return str(type(self)) + ":\n" + AttributeCollection.__str__(self) + self.parameters.__str__()

    def toXML(self):
        """Generate the Step XML representation"""
        ret = "<StepInstance>\n"
        ret = ret + AttributeCollection.toXML(self)
        ret = ret + self.parameters.toXML()
        ret = ret + "</StepInstance>\n"
        return ret

    def setWorkflowCommons(self, wf):
        """Add reference to the collection of the common tools"""

        self.workflow_commons = wf

    def execute(self, step_exec_attr, definitions):
        """Step execution method. step_exec_attr is array to hold parameters belong to this Step,
        filled above in the workflow
        """
        print("Executing StepInstance", self.getName(), "of type", self.getType(), list(definitions))
        # Report the Application state if the corresponding tool is supplied
        if "JobReport" in self.workflow_commons:
            if self.parent.workflowStatus["OK"]:
                self.workflow_commons["JobReport"].setApplicationStatus("Executing " + self.getName())

        # Prepare Step statistics evaluation
        self.step_commons["StartTime"] = time.time()
        self.step_commons["StartStats"] = os.times()

        step_def = definitions[self.getType()]
        step_exec_modules = {}
        error_message = ""
        error_code = 0
        for mod_inst in step_def.module_instances:
            mod_inst_name = mod_inst.getName()
            mod_inst_type = mod_inst.getType()

            # print "StepInstance creating module instance ", mod_inst_name, " of type", mod_inst.getType()
            step_exec_modules[mod_inst_name] = step_def.parent.module_definitions[
                mod_inst_type
            ].main_class_obj()  # creating instance

            # Resolve all the linked parameter values
            for parameter in mod_inst.parameters:
                if parameter.preExecute():
                    # print '>>>> Input', parameter
                    if parameter.isLinked():
                        # print ">>>> ModuleInstance", mod_inst_name + '.' + parameter.getName(),
                        # '=', parameter.getLinkedModule() + '.' + parameter.getLinkedParameter()
                        if parameter.getLinkedModule() == "self":
                            # tale value form the step_dict
                            setattr(
                                step_exec_modules[mod_inst_name],
                                parameter.getName(),
                                step_exec_attr[parameter.getLinkedParameter()],
                            )
                        else:
                            setattr(
                                step_exec_modules[mod_inst_name],
                                parameter.getName(),
                                getattr(step_exec_modules[parameter.getLinkedModule()], parameter.getLinkedParameter()),
                            )
                    else:
                        # print ">>>> ModuleInstance", mod_inst_name + '.' + parameter.getName(), '=', parameter.getValue()
                        setattr(step_exec_modules[mod_inst_name], parameter.getName(), parameter.getValue())
                    # print 'Step Input Parameter:', parameter.getName(), getattr(
                    # step_exec_modules[mod_inst_name], parameter.getName() )

            # Set reference to the workflow and step common tools
            setattr(step_exec_modules[mod_inst_name], "workflow_commons", self.parent.workflow_commons)
            setattr(step_exec_modules[mod_inst_name], "step_commons", self.step_commons)
            setattr(step_exec_modules[mod_inst_name], "stepStatus", self.stepStatus)
            setattr(step_exec_modules[mod_inst_name], "workflowStatus", self.parent.workflowStatus)

            try:
                result = step_exec_modules[mod_inst_name].execute()
                if not result["OK"]:
                    if self.stepStatus["OK"]:
                        error_message = result["Message"]
                        error_code = result["Errno"]
                        if "JobReport" in self.workflow_commons:
                            if self.parent.workflowStatus["OK"]:
                                self.workflow_commons["JobReport"].setApplicationStatus(error_message)
                    self.stepStatus = S_ERROR(result["Message"])
                else:
                    for parameter in mod_inst.parameters:
                        if parameter.isOutput():
                            # print '<<<< Output', parameter
                            if parameter.isLinked():
                                # print "ModuleInstance self ." + parameter.getName(), '=',
                                # parameter.getLinkedModule() + '.' + parameter.getLinkedParameter()
                                if parameter.getLinkedModule() == "self":
                                    # this is not supposed to happen
                                    print("Warning! Module OUTPUT attribute", parameter.getName(), end=" ")
                                    print(
                                        "refer to the attribute of the same module",
                                        parameter.getLinkedParameter(),
                                        "=",
                                        getattr(step_exec_modules[mod_inst_name], parameter.getName()),
                                    )
                                    step_exec_attr[parameter.getName()] = getattr(
                                        step_exec_modules[mod_inst_name],
                                        parameter.getLinkedParameter(),
                                        parameter.getValue(),
                                    )
                                    # print "                 OUT", parameter.getLinkedParameter(), '=',
                                    # getattr( step_exec_modules[mod_inst_name], parameter.getName(),
                                    # parameter.getValue() )
                                else:
                                    # print 'Output step_exec_attr', st_parameter.getName(),
                                    # step_exec_modules[st_parameter.getLinkedModule()],
                                    # parameter.getLinkedParameter()
                                    step_exec_attr[parameter.getName()] = getattr(
                                        step_exec_modules[parameter.getLinkedModule()], parameter.getLinkedParameter()
                                    )
                            else:
                                # This also does not make sense - we can give a warning
                                print("Warning! Module OUTPUT attribute ", parameter.getName(), end=" ")
                                print("assigned constant", parameter.getValue())
                                # print "StepInstance self." + parameter.getName(), '=', parameter.getValue()
                                step_exec_attr[parameter.getName()] = parameter.getValue()

                            # print 'Module Output Parameter:', parameter.getName(), step_exec_attr[parameter.getName()]

                    # Get output values to the step_commons dictionary
                    for key in result:
                        if key != "OK":
                            if key != "Value":
                                self.step_commons[key] = result[key]
                            elif isinstance(result["Value"], dict):
                                for vkey in result["Value"].keys():
                                    self.step_commons[vkey] = result["Value"][vkey]

            except Exception as x:
                print("Exception while module execution")
                print("Module", mod_inst_name, mod_inst.getType())
                print(str(x))
                exc = sys.exc_info()
                exc_type = exc[0]
                value = exc[1]
                print(
                    "== EXCEPTION ==\n%s: %s\n\n%s==============="
                    % (exc_type, value, "\n".join(traceback.format_tb(exc[2])))
                )

                print("Step status: ", self.stepStatus)
                print("Workflow status: ", self.parent.workflowStatus)
                if self.stepStatus["OK"]:
                    # This is the error that caused the workflow disruption
                    # report it to the WMS
                    error_message = f"Exception while {mod_inst_name} module execution: {str(x)}"
                    error_code = 0
                    if "JobReport" in self.workflow_commons:
                        if self.parent.workflowStatus["OK"]:
                            self.workflow_commons["JobReport"].setApplicationStatus(
                                f"Exception in {mod_inst_name} module"
                            )

                self.stepStatus = S_ERROR(error_code, error_message)

        # now we need to copy output values to the STEP!!! parameters
        for st_parameter in self.parameters:
            if st_parameter.isOutput():
                # print '<< Output', st_parameter
                if st_parameter.isLinked():
                    # print "StepInstance self." + st_parameter.getName(), '=',
                    # st_parameter.getLinkedModule() + '.' + st_parameter.getLinkedParameter()
                    if st_parameter.getLinkedModule() == "self":
                        # this is not supposed to happen
                        print("Warning! Step OUTPUT attribute", st_parameter.getName(), end=" ")
                        print(
                            "refer to the attribute of the same step",
                            st_parameter.getLinkedParameter(),
                            step_exec_attr[st_parameter.getLinkedParameter()],
                        )
                        step_exec_attr[st_parameter.getName()] = step_exec_attr[st_parameter.getLinkedParameter()]

                    else:
                        # print 'Output step_exec_attr', st_parameter.getName(),
                        # step_exec_modules[st_parameter.getLinkedModule()],
                        # st_parameter.getLinkedParameter()
                        step_exec_attr[st_parameter.getName()] = getattr(
                            step_exec_modules[st_parameter.getLinkedModule()], st_parameter.getLinkedParameter()
                        )
                    setattr(self, st_parameter.getName(), step_exec_attr[st_parameter.getName()])
                else:
                    # This also does not make sense - we can give a warning
                    print("Warning! Step OUTPUT attribute ", st_parameter.getName(), end=" ")
                    print("assigned constant", st_parameter.getValue())
                    # print "StepInstance self." + st_parameter.getName(), '=', st_parameter.getValue()
                    step_exec_attr[st_parameter.getName()] = st_parameter.getValue()

                print("Step Output", st_parameter.getName(), "=", step_exec_attr[st_parameter.getName()])

        # Return the result of the first failed module or S_OK
        if not self.stepStatus["OK"]:
            return S_ERROR(error_code, error_message)
        else:
            return S_OK(result["Value"])
