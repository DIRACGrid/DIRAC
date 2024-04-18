"""
    Workflow class is the main container of Steps and Modules
"""
import os
import xml.sax

from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Utility import *
from DIRAC import S_OK, S_ERROR


class Workflow(AttributeCollection):
    def __init__(self, obj=None, name=None):
        """Be aware that 1-st param is an obj not a name!!!!
        obj can me a string with XML representation or with filename
        also obj can be a Workflow or ParameterCollections
        """
        AttributeCollection.__init__(self)
        if (obj is None) or isinstance(obj, ParameterCollection):
            self.setName("notgiven")
            self.setType("")
            self.setDescrShort("")
            self.setDescription("")
            self.setOrigin("")
            self.setVersion(0.0)
            self.parameters = ParameterCollection(obj)
            self.step_instances = InstancesPool(self)
            self.step_definitions = DefinitionsPool(self)
            self.module_definitions = DefinitionsPool(self)

        elif isinstance(obj, Workflow):
            self.fromWorkflow(obj)
        elif isinstance(obj, str):
            self.parameters = ParameterCollection(None)
            self.step_instances = InstancesPool(self)
            self.step_definitions = DefinitionsPool(self)
            self.module_definitions = DefinitionsPool(self)
            # if obj is an XML string
            if obj.startswith("<"):
                fromXMLString(obj, self)
            else:
                fromXMLFile(obj, self)
        elif obj is not None:
            raise TypeError("Can not create object type " + str(type(self)) + " from the " + str(type(obj)))

        if name:
            self.setName(name)
        self.workflow_commons = {}
        self.workflowStatus = S_OK()

    def fromWorkflow(self, obj):
        self.setName(obj.getName())
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

    def __str__(self):
        """Creates a string representation of itself"""
        ret = str(self.getName()) + ":\n" + AttributeCollection.__str__(self) + self.parameters.__str__()
        ret = ret + str(self.step_definitions)
        ret = ret + str(self.step_instances)
        ret = ret + str(self.module_definitions)
        return ret

    def toXML(self):
        """Creates an XML representation of itself"""
        # THIS is very important that Definitions should be written before instances
        ret = "<Workflow>\n"
        ret = ret + AttributeCollection.toXML(self)
        ret = ret + self.parameters.toXML()
        ret = ret + self.module_definitions.toXML()
        ret = ret + self.step_definitions.toXML()
        ret = ret + self.step_instances.toXML()
        ret = ret + "</Workflow>\n"
        return ret

    def toXMLFile(self, outFile):
        if os.path.exists(outFile):
            os.remove(outFile)
        with open(outFile, "w") as xmlfile:
            xmlfile.write(self.toXML())

    def addTool(self, name, tool):
        """Add an object that will be available in all the modules to perform some operations.
        For example, a state reporting facility.
        """
        self.workflow_commons[name] = tool

    def addStep(self, step):
        # this is a VERY important piece of code
        # we have to join all Modules definition from all added steps in the single dictionary
        # and we have to share this dictionary between all included steps
        # we also have to check versions of the modules and instances
        for type_o in step.module_definitions:
            if type_o not in self.module_definitions:
                self.module_definitions.append(step.module_definitions[type_o])
        self.step_definitions.append(step)
        del step.module_definitions  # we need to clean up all unwanted definitions
        step.module_definitions = None
        return step

    def addModule(self, module):
        # KGG We need to add code to update existing modules
        self.module_definitions.append(module)
        return module

    def createStepInstance(self, type_o, name):
        """Creates step instance of type 'type_o' with the name 'name'"""
        if type_o in self.step_definitions:
            stepi = StepInstance(name, self.step_definitions[type_o])
            self.step_instances.append(stepi)
            return stepi
        else:
            raise KeyError("Can not find StepDefinition " + type + " to create StepInstrance " + name)

    def removeStepInstance(self, name):
        self.step_instances[name].setParent(None)
        self.step_instances.delete(name)

    def updateParents(self):
        self.module_definitions.updateParents(self)
        self.step_instances.updateParents(self)
        self.step_definitions.updateParents(self)

    def resolveGlobalVars(self):
        """This method will create global parameter list and then will resolve all instances of @{VARNAME}
        Be aware that parameters of that type are GLOBAL!!! are string and can not be dynamically change
        The scope: the resolution of that parameters apply from lower to upper object, for example if
        parameter use in module, then it checks module, then step, then workflow

        Comment: If varible linked it should not be used in a global list"""

        # reenforced global parameters on the level of Workflow
        if not self.parameters.find("PRODUCTION_ID"):
            self.parameters.append(
                Parameter(
                    "PRODUCTION_ID",
                    "00000000",
                    "string",
                    "",
                    "",
                    True,
                    False,
                    "Transformation ID taken from the ProductionManager",
                )
            )
        if not self.parameters.find("JOB_ID"):
            self.parameters.append(
                Parameter(
                    "JOB_ID",
                    "00000000",
                    "string",
                    "",
                    "",
                    True,
                    False,
                    "Job ID within Tranformation taken from the Transformation Manager",
                )
            )

        self.parameters.resolveGlobalVars()
        step_instance_number = 0
        for inst in self.step_instances:
            # for each step instance we can define STEP_NUMBER
            step_instance_number = step_instance_number + 1
            if not inst.parameters.find("STEP_NUMBER"):
                inst.parameters.append(
                    Parameter(
                        "STEP_NUMBER",
                        f"{step_instance_number}",
                        "string",
                        "",
                        "",
                        True,
                        False,
                        "Number of the StepInstance within the Workflow",
                    )
                )
            if not inst.parameters.find("STEP_ID"):
                prod_ID = self.parameters.find("PRODUCTION_ID").getValue()
                job_ID = self.parameters.find("JOB_ID").getValue()
                inst.parameters.append(
                    Parameter(
                        "STEP_ID",
                        "%s_%s_%d" % (prod_ID, job_ID, step_instance_number),
                        "string",
                        "",
                        "",
                        True,
                        False,
                        "Step instance ID",
                    )
                )
            if not inst.parameters.find("STEP_INSTANCE_NAME"):
                inst.parameters.append(
                    Parameter(
                        "STEP_INSTANCE_NAME",
                        inst.getName(),
                        "string",
                        "",
                        "",
                        True,
                        False,
                        "Name of the StepInstance within the Workflow",
                    )
                )
            if not inst.parameters.find("STEP_DEFINITION_NAME"):
                inst.parameters.append(
                    Parameter(
                        "STEP_DEFINITION_NAME",
                        inst.getType(),
                        "string",
                        "",
                        "",
                        True,
                        False,
                        "Type of the StepInstance within the Workflow",
                    )
                )
            if not inst.parameters.find("JOB_ID"):
                inst.parameters.append(
                    Parameter(
                        "JOB_ID",
                        "",
                        "string",
                        "self",
                        "JOB_ID",
                        True,
                        False,
                        "Job ID within Tranformation taken from the Transformation Manager",
                    )
                )
            if not inst.parameters.find("PRODUCTION_ID"):
                inst.parameters.append(
                    Parameter(
                        "PRODUCTION_ID",
                        "",
                        "string",
                        "self",
                        "PRODUCTION_ID",
                        True,
                        False,
                        "Type of the StepInstance within the Workflow",
                    )
                )
            inst.resolveGlobalVars(self.step_definitions, self.parameters)

    def createCode(self, combine_steps=False):
        self.resolveGlobalVars()
        str = ""
        str = str + self.module_definitions.createCode()
        str = str + self.step_definitions.createCode()
        str = str + "\nclass job:\n"
        str = str + indent(1) + "def execute(self):\n"
        # str=str+indent(2)+'# flush self.step_instances\n'
        str = str + self.step_instances.createCode()
        # it seems we do not need it on this level
        str = str + indent(2) + "# output assignment\n"
        for v in self.parameters:
            if v.isOutput():
                str = str + v.createParameterCode(2, "self")

        str = str + "\nj=job()\n"
        str = str + self.parameters.createParametersCode(0, "j")
        str = str + "j.execute()"
        return str

    def execute(self):
        self.resolveGlobalVars()
        # define workflow attributes
        wf_exec_attr = {}  # dictionary with the WF attributes, used to resolve links to self.attrname
        for wf_parameter in self.parameters:
            # parameters shall see objects in the current scope order to resolve links
            if wf_parameter.preExecute():  # for parm which not just outputs
                # print 'Input', wf_parameter
                if wf_parameter.isLinked():
                    # print "Workflow self." + wf_parameter.getName(), '=',
                    # wf_parameter.getLinkedModule() + '.' + wf_parameter.getLinkedParameter()
                    if wf_parameter.getLinkedModule() == "self":
                        # this is not suppose to happen
                        print(
                            "Warning! Job attribute ",
                            wf_parameter.getName(),
                            "refers to the attribute of the same workflow",
                            wf_parameter.getLinkedParameter(),
                        )
                        wf_exec_attr[wf_parameter.getName()] = wf_exec_attr[wf_parameter.getLinkedParameter()]
                    else:
                        wf_exec_attr[wf_parameter.getName()] = wf_exec_attr[wf_parameter.getLinkedModule()][
                            wf_parameter.getLinkedParameter()
                        ]
                else:
                    # print "Workflow self." + wf_parameter.getName(), '=', wf_parameter.getValue()
                    wf_exec_attr[wf_parameter.getName()] = wf_parameter.getValue()

            # Put all the workflow parameters into the workflow_commons dictionary
            self.workflow_commons[wf_parameter.getName()] = wf_parameter.getValue()

        self.module_definitions.loadCode()  # loading Module classes into current python scope

        # wf_exec_steps will be dictionary of dictionaries [step instance name][parameter name]
        # used as dictionary of step instances to carry parameters
        wf_exec_steps = {}
        # print 'Executing Workflow',self.getType()
        error_message = ""
        step_result = ""
        for step_inst in self.step_instances:
            step_inst_name = step_inst.getName()
            step_inst_type = step_inst.getType()
            wf_exec_steps[step_inst_name] = {}
            # print "WorkflowInstance creating Step instance ",step_inst_name," of type", step_inst_type
            for parameter in step_inst.parameters:
                if parameter.preExecute():
                    # print '>> Input', parameter
                    if parameter.isLinked():
                        # print ">> StepInstance", step_inst_name + '.' + parameter.getName(),
                        # '=', parameter.getLinkedModule() + '.' + parameter.getLinkedParameter()
                        if parameter.getLinkedModule() == "self":
                            # tale value form the step_dict
                            wf_exec_steps[step_inst_name][parameter.getName()] = wf_exec_attr[
                                parameter.getLinkedParameter()
                            ]
                        else:
                            # print wf_exec_steps[parameter.getLinkedModule()].keys()
                            wf_exec_steps[step_inst_name][parameter.getName()] = wf_exec_steps[
                                parameter.getLinkedModule()
                            ][parameter.getLinkedParameter()]
                    else:
                        # print ">> StepInstance", step_inst_name + '.' + parameter.getName(), '=', parameter.getValue()
                        wf_exec_steps[step_inst_name][parameter.getName()] = parameter.getValue()

                # In the step_commons all parameters are added, both Input and Output ones.
                step_inst.step_commons[parameter.getName()] = parameter.getValue()

            resolveVariables(wf_exec_steps[step_inst_name])
            # Set proper values for all Input Parameters
            for key, value in wf_exec_steps[step_inst_name].items():
                step_inst.step_commons[key] = value

            step_inst.setParent(self)
            step_inst.setWorkflowCommons(self.workflow_commons)

            result = step_inst.execute(wf_exec_steps[step_inst_name], self.step_definitions)
            if not result["OK"]:
                if self.workflowStatus["OK"]:
                    error_message = result["Message"]
                self.workflowStatus = S_ERROR(result["Message"])
                self.workflowStatus["Errno"] = result["Errno"]

            step_result = result.get("Value", step_result)

        # now we need to copy output values to the STEP!!! parameters
        # print "WorkflowInstance output assignment"
        for wf_parameter in self.parameters:
            if wf_parameter.isOutput():
                if wf_parameter.isLinked():
                    # print "WorkflowInstance  self." + wf_parameter.getName(), '=',
                    # wf_parameter.getLinkedModule() + '.' + wf_parameter.getLinkedParameter()
                    if wf_parameter.getLinkedModule() == "self":
                        # this is not suppose to happen
                        print(
                            "Warning! Workflow OUTPUT attribute ",
                            wf_parameter.getName(),
                            "refer on the attribute of the same workflow",
                            wf_parameter.getLinkedParameter(),
                        )
                        wf_exec_attr[wf_parameter.getName()] = wf_exec_attr[wf_parameter.getLinkedParameter()]
                    else:
                        wf_exec_attr[wf_parameter.getName()] = wf_exec_steps[wf_parameter.getLinkedModule()][
                            wf_parameter.getLinkedParameter()
                        ]
                else:
                    # it is also does not make sense - we can produce warning
                    print(
                        "Warning! Workflow OUTPUT attribute",
                        wf_parameter.getName(),
                        "assigned constant",
                        wf_parameter.getValue(),
                    )
                    # print "WorkflowInstance  self."+ wf_parameter.getName(),'=',wf_parameter.getValue()
                    wf_exec_attr[wf_parameter.getName()] = wf_parameter.getValue()
                setattr(self, wf_parameter.getName(), wf_exec_attr[wf_parameter.getName()])
        # Return the result of the first failed step or S_OK
        if not self.workflowStatus["OK"]:
            # return S_ERROR( error_message )
            return self.workflowStatus
        return S_OK(step_result)


def fromXMLString(xml_string, obj=None):
    # prevent circular import in WorkflowReader
    from DIRAC.Core.Workflow.WorkflowReader import WorkflowXMLHandler

    # KGG !!! We need to reset Workflow if it exists
    handler = WorkflowXMLHandler(obj)
    xml.sax.parseString(xml_string, handler)
    return handler.root


def fromXMLFile(xml_file, obj=None):
    # prevent circular import in WorkflowReader
    from DIRAC.Core.Workflow.WorkflowReader import WorkflowXMLHandler

    # KGG !!! We need to reset Workflow if it exists
    handler = WorkflowXMLHandler(obj)
    xml.sax.parse(xml_file, handler)
    return handler.root
