from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *

bodyTestApp = """class TestAppModule:

    def __init__(self):
        pass

    def initialize(self,name,version,parameters):
        print( 'Test application initialization %s %s' % ( name, str( version ) ) )
        return 1

    def execute(self):
        if not self.enable:
            print 'Module ',str(type(self)),'is disabled'
            return 1
        print( 'Test application execution' )
        print 'Module MODULE.getDescrShort() =', self.MODULE.getDescrShort()
        print 'Module MODULE_DEFINITION_NAME =', self.MODULE_DEFINITION_NAME
        print 'Module MODULE_INSTANCE_NAME =', self.MODULE_INSTANCE_NAME
        print 'Module STEP.getDescrShort() =', self.STEP.getDescrShort()
        print 'Module STEP_DEFINITION_NAME =', self.STEP_DEFINITION_NAME
        print 'Module STEP_INSTANCE_NAME =', self.STEP_INSTANCE_NAME

        return 1"""

md1 = ModuleDefinition("TestAppModule")
md1.addParameter(Parameter("enable", "True", "bool", "", "", True, False, "If False execution is disabled"))
md1.setBody(bodyTestApp)
md1.setDescrShort("Empty Module")
md1.setDescription("Empty Module to do some testing")
md1.setRequired("")
md1.setOrigin("")
md1.setVersion(0.1)

sd1 = StepDefinition("TestAppStep")
sd1.addModule(md1)
mi1 = sd1.createModuleInstance("TestAppModule", "testappmod1")
wf1 = Workflow("TestAppWF")
wf1.addStep(sd1)
si1 = wf1.createStepInstance("TestAppStep", "testappstep1")
print(wf1.createCode())
# eval(compile(wf1.createCode(),'<string>','exec'))
wf1.execute()
