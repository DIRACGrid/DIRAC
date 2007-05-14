from Parameter import *
from Module import *
from Step import *

class Workflow(AttributeCollection):
    def __init__(self, type=None, obj=None):
        AttributeCollection.__init__(self)

        # sort out parameters and class attributes
        if (obj == None) or isinstance(obj, ParameterCollection):
            self.setType(type)
            self.setDescrShort('')
            self.setDescription('')
            self.setOrigin('')
            self.setVersion(0.0)
            self.parameters = ParameterCollection(obj) # creating copy
            self.step_instances = InstancesPool(self)
            self.step_definitions = DefinitionsPool(self)
            self.module_definitions = DefinitionsPool(self)
        elif isinstance(obj, Workflow):
            if type == None:
                self.setType(obj.getType())
            else:
                self.setType(type)
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
        ret = str(type(self))+':\n'+ AttributeCollection.__str__(self) + self.parameters.__str__()
        ret = ret + str(self.step_definitions)
        ret = ret + str(self.step_instances)
        ret = ret + str(self.module_definitions)
        return ret

    def toXMLString(self):
        ret = '<Workflow>\n'
        ret = ret + AttributeCollection.toXMLString(self)+self.parameters.toXMLString()
        ret = ret + self.step_definitions.toXMLString()
        ret = ret + self.step_instances.toXMLString()
        ret = ret + self.module_definitions.toXMLString()
        return ret+'</Workflow>\n'

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
                # we have not that ModuleDefinition so we can easyly move it
                self.module_definitions.append(step.module_definitions[type])
        self.step_definitions.append(step)
        del step.module_definitions # we need to clean all unwanted definitions
        step.module_definitions=self.module_definitions
        return step

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


#============================================================================
# test section
#============================================================================
if __name__ == "__main__":
    from WFSamples import *
    #print w1
    w1.resolveGlobalVars()
    #print "# ================ CODE ========================"
    print w1.createCode()
    #print "------------------- result of the evaluation -------------"
    eval(compile(w1.createCode(),'<string>','exec'))
    #print " ================== Interpretation ======================="
    #w1.execute()
    #print w1.toXMLString()
    #import pickle
    #output = open('D:\gennady\workspace\Workflow\wf.pkl', 'wb')
    #pickle.dump(w1, output, 2)
    #output = open('D:\gennady\workspace\Workflow\wf.xml', 'wb')
    #output.write(w1.toXMLString())
    #output.close()

    #wf_file = open('D:\gennady\workspace\Workflow\wf.pkl', 'rb')
    #w2 = pickle.load(wf_file)
    #wf_file = open('D:\gennady\workspace\Workflow\wf.xml', 'rb')
    #s2 = wf_file.read()
    #print s2
    #w2.updateParents()
    #print w2.createCode()
    #eval(compile(w2.createCode(),'<string>','exec'))

    #from PyQt4 import QtCore, QtGui
    #from editors.ModuleEditor import *
    #app = QtGui.QApplication(sys.argv)
    #mainWin = ModuleEditor(md1)
    #mainWin.show()
    #sys.exit(app.exec_())



# ======================================================================

