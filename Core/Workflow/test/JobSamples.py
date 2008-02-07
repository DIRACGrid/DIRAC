# $Id: JobSamples.py,v 1.6 2008/02/07 16:01:25 gkuznets Exp $
"""
    This is a comment
"""
__RCSID__ = "$Revision: 1.6 $"

# $Source: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Workflow/test/JobSamples.py,v $

from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *

#define Module 1
module1 = ModuleDefinition('GaudiApplication')#during constraction class creates duplicating copies of the params
module1.setDescription('Gaudi Application module')
module1.setBody('from WorkflowLib.Module.GaudiApplication import GaudiApplication\n')

# we add empty parameters but linked up as default
module1.appendParameter(Parameter("appName","","string","self","appName",True, False, "Application Name"))
module1.appendParameter(Parameter("appVersion","","string","self","appVersion",True, False, "Application Version"))
module1.appendParameter(Parameter("appType","","string","self","appType",True,False,"Application Version"))
module1.appendParameter(Parameter("appLog","","string","self","appLog",True,False,"list of logfile"))
module1.appendParameter(Parameter("inputData","","jdl","self","inputData",True, False, "Application Name"))
module1.appendParameter(Parameter("optionsFile","","string","self","optionsFile",True,False,"Options File"))
module1.appendParameter(Parameter("optionsLine","","string","self","optionsLine",True,False,"Number of Event","option"))
module1.appendParameter(Parameter("systemConfig","","string","self","systemConfig",True,False,"Job Platform"))
module1.appendParameter(Parameter("poolXMLCatName","","string","self","poolXMLCatName",True,False,"POOL XML slice"))
module1.appendParameter(Parameter("outputData",'',"string","self","outputData",True,False,"list of output data"))

#define module 2
module2 = ModuleDefinition('LogChecker')#during constraction class creates duplicating copies of the params
module2.setDescription('Check LogFile module')
module2.setBody('from WorkflowLib.Module.LogChecker import *\n')

# we add parameters and link them to the level of step
module2.appendParameter(Parameter("appName","","string","self","appName",True, False, "Application Name"))
module2.appendParameter(Parameter("appVersion","","string","self","appVersion",True, False, "Application Version"))
module2.appendParameter(Parameter("appType","","string","self","appType",True,False,"Application Version"))
module2.appendParameter(Parameter("appLog","","string","self","appLog",True,False,"list of logfile"))
module2.appendParameter(Parameter("inputData","","string","self","inputData",True,False,"InputData"))
module2.appendParameter(Parameter("OUTPUT_MAX","","string","self","OUTPUT_MAX",True,False,"nb max of output to keep"))
# this parameter is static so we define it here
module2.appendParameter(Parameter("EMAIL","@{EMAILNAME}","string","","",True,False,"EMAIL adress"))

###############   STEPS ##################################
#### step 1 we are creating step definition
step1 = StepDefinition('Gaudi_App_Step')
step1.addModule(module1) # Creating instance of the module 'Gaudi_App_Step'
moduleInstance1 = step1.createModuleInstance('GaudiApplication', 'Module1')

step1.addModule(module2) # Creating instance of the module 'LogChecker'
moduleInstance2 = step1.createModuleInstance('LogChecker', 'Module2')
# in principle we can link parameters of moduleInstance2 with moduleInstance1 but
# in this case we going to use link with the step

# now we can add parameters for the STEP but instead of typing them we can just use old one from modules
step1.appendParameterCopyLinked(module2.parameters)
step1.appendParameterCopyLinked(module1.parameters)

# and we can add additional parameter which will be used as a global
step1.appendParameter(Parameter("STEP_ID","@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}","string","","",True, False, "Temporary fix"))
step1.setValue("appLog","@{appName}_@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}.log")
step1.unlinkParameter(["appLog","appName", "appType"])



##############  WORKFLOW #################################
workflow1 = Workflow(name='main')
workflow1.setDescription('Workflow of GaudiApplication')

workflow1.addStep(step1)
step1_prefix="step1_"
stepInstance1 = workflow1.createStepInstance('Gaudi_App_Step', 'Step1')
# lets link all parameters them up with the level of workflow
stepInstance1.linkParameterUp(stepInstance1.parameters, step1_prefix)
stepInstance1.setLink("systemConfig","self", "systemConfig") # correct link as we have onlu one system config
# except "STEP_ID", "appLog"
stepInstance1.unlinkParameter(["STEP_ID", "appLog","appName", "appType", "outputData"])
stepInstance1.setValue("appName", "Boole")
stepInstance1.setValue("appType", "digi")
stepInstance1.setValue("outputData","@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}.@{appType}")

step2_prefix="step2_"
stepInstance2 = workflow1.createStepInstance('Gaudi_App_Step', 'Step2')
# lets link all parameters them up with the level of workflow
stepInstance2.linkParameterUp(stepInstance2.parameters, step2_prefix)
stepInstance2.setLink("systemConfig","self", "systemConfig") # correct link as we have onlu one system config
# except "STEP_ID", "appLog"
stepInstance2.unlinkParameter(["STEP_ID", "appLog","appName", "appType", "outputData"])
stepInstance2.setValue("appName", "Brunel")
stepInstance2.setValue("appType", "dst")
stepInstance2.setLink("inputData", stepInstance1.getName(), "outputData")
stepInstance2.setValue("outputData","@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}.@{appType}")

# Now lets define parameters on the top
indata = "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_1.sim;LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_2.sim;LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_3.sim"
#indata = "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_1.sim"
# lets specify parameters on the level of workflow
workflow1.appendParameterCopyLinked(step1.parameters, step1_prefix)
# and finally we can unlink them because we inherit them linked
workflow1.unlinkParameter(workflow1.parameters)

workflow1.setValue(step1_prefix+"appVersion", "v14r8")
workflow1.setValue(step1_prefix+"optionsFile", "v200601.opts")
workflow1.setValue(step1_prefix+"optionsLine","ApplicationMgr.EvtMax = 1")
workflow1.setValue(step1_prefix+"poolXMLCatName","pool_xml_catalog.xml")
workflow1.setValue(step1_prefix+"inputData",indata)
workflow1.setValue(step1_prefix+"OUTPUT_MAX","10")
# remove unwanted
workflow1.removeParameter(step1_prefix+"outputData")
workflow1.removeParameter(step1_prefix+"systemConfig")
#add syspem config which common for all modules
workflow1.appendParameter(Parameter("systemConfig","slc4_amd64_gcc34","string","","",True, False, "Application Name"))


workflow1.appendParameterCopyLinked(step1.parameters, step2_prefix)
# and finally we can unlink them because we inherit them linked
workflow1.unlinkParameter(workflow1.parameters)

workflow1.setValue(step2_prefix+"appVersion", "v32r2")
workflow1.setValue(step2_prefix+"optionsFile", "v200601.opts")
workflow1.setValue(step2_prefix+"optionsLine","ApplicationMgr.EvtMax = -1")
workflow1.setValue(step2_prefix+"poolXMLCatName","pool_xml_catalog.xml")
workflow1.setValue(step2_prefix+"OUTPUT_MAX","10")
# remove unwanted
workflow1.removeParameter(step2_prefix+"inputData")
workflow1.removeParameter(step2_prefix+"outputData")
workflow1.removeParameter(step2_prefix+"systemConfig")


workflow1.appendParameter(Parameter("PRODUCTION_ID","00003033","string","","",True, False, "Temporary fix"))
workflow1.appendParameter(Parameter("JOB_ID","00000010","string","","",True, False, "Temporary fix"))
workflow1.appendParameter(Parameter("EMAILNAME","joel.closier@cern.ch","string","","",True, False, "Email to send a report from the LogCheck module"))

#workflow1.toXMLFile('/afs/cern.ch/user/g/gkuznets/test.xml')
#w4 = fromXMLFile("/afs/cern.ch/user/g/gkuznets/test1.xml")
#print 'Creating code for the workflow'
print workflow1.createCode()
eval(compile(workflow1.createCode(),'<string>','exec'))
#workflow1.execute()
