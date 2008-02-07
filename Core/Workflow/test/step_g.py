from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *

#define Module 2
module2 = ModuleDefinition('GaudiApplication')#during constraction class creates duplicating copies of the params
module2.setDescription('Gaudi Application module')
module2.setBody('from WorkflowLib.Module.GaudiApplication import GaudiApplication\n')

# we add empty parameters but linked up as default
module2.appendParameter(Parameter("CONFIG_NAME","","string","self","CONFIG_NAME",True, False, "Configuration Name"))
module2.appendParameter(Parameter("CONFIG_VERSION","","string","self","CONFIG_VERSION",True, False, "Configuration Version"))
module2.appendParameter(Parameter("appName","","string","self","appName",True, False, "Application Name"))
module2.appendParameter(Parameter("appVersion","","string","self","appVersion",True, False, "Application Version"))
module2.appendParameter(Parameter("appType","","string","self","appType",True,False,"Application Version"))
module2.appendParameter(Parameter("appLog","","string","self","appLog",True,False,"list of logfile"))
module2.appendParameter(Parameter("inputData","","jdl","self","inputData",True, False, "List of InputData"))
module2.appendParameter(Parameter("inputDataType","","string","self","inputDataType",True, False, "Input Data Type"))
module2.appendParameter(Parameter("optionsFile","","string","self","optionsFile",True,False,"Options File"))
module2.appendParameter(Parameter("optionsLine","","string","self","optionsLine",True,False,"Number of Event","option"))
module2.appendParameter(Parameter("systemConfig","","string","self","systemConfig",True,False,"Job Platform"))
module2.appendParameter(Parameter("poolXMLCatName","","string","self","poolXMLCatName",True,False,"POOL XML slice"))
module2.appendParameter(Parameter("outputData",'',"string","self","outputData",True,False,"list of output data"))

#define module 3
module3 = ModuleDefinition('LogChecker')#during constraction class creates duplicating copies of the params
module3.setDescription('Check LogFile module')
module3.setBody('from WorkflowLib.Module.LogChecker import *\n')

# we add parameters and link them to the level of step
module3.appendParameter(Parameter("CONFIG_NAME","","string","self","CONFIG_NAME",True, False, "Configuration Name"))
module3.appendParameter(Parameter("CONFIG_VERSION","","string","self","CONFIG_VERSION",True, False, "Configuration Version"))
module3.appendParameter(Parameter("appName","","string","self","appName",True, False, "Application Name"))
module3.appendParameter(Parameter("appVersion","","string","self","appVersion",True, False, "Application Version"))
module3.appendParameter(Parameter("appType","","string","self","appType",True,False,"Application Version"))
module3.appendParameter(Parameter("appLog","","string","self","appLog",True,False,"list of logfile"))
module3.appendParameter(Parameter("inputData","","string","self","inputData",True,False,"InputData"))
module3.appendParameter(Parameter("OUTPUT_MAX","","string","self","OUTPUT_MAX",True,False,"nb max of output to keep"))
# this parameter is static so we define it here
module3.appendParameter(Parameter("EMAIL","@{EMAILNAME}","string","","",True,False,"EMAIL adress"))


#define module 4
module4 = ModuleDefinition('BookkeepingReport')
module4.setDescription('Bookkeeping Report module')
module4.setBody('from WorkflowLib.Module.BookkeepingReport import * \n')
module4.appendParameter(Parameter("STEP_ID","","string","self","STEP_ID",True,False,"EMAIL adress"))
module4.appendParameter(Parameter("CONFIG_NAME","","string","self","CONFIG_NAME",True, False, "Configuration Name"))
module4.appendParameter(Parameter("CONFIG_VERSION","","string","self","CONFIG_VERSION",True, False, "Configuration Version"))
module4.appendParameter(Parameter("appName","","string","self","appName",True, False, "Application Name"))
module4.appendParameter(Parameter("appVersion","","string","self","appVersion",True, False, "Application Version"))
module4.appendParameter(Parameter("inputData","","string","self","inputData",True,False,"InputData"))


###############   STEPS ##################################
#### step 1 we are creating step definition
step1 = StepDefinition('Gaudi_App_Step')
step1.addModule(module2) # Creating instance of the module 'Gaudi_App_Step'
moduleInstance2 = step1.createModuleInstance('GaudiApplication', 'module2')

step1.addModule(module3) # Creating instance of the module 'LogChecker'
moduleInstance3 = step1.createModuleInstance('LogChecker', 'module3')

step1.addModule(module4) # Creating instance of the module 'LogChecker'
moduleInstance4 = step1.createModuleInstance('BookkeepingReport', 'module4')
# in principle we can link parameters of moduleInstance2 with moduleInstance1 but
# in this case we going to use link with the step

# now we can add parameters for the STEP but instead of typing them we can just use old one from modules
step1.appendParameterCopyLinked(module3.parameters)
step1.appendParameterCopyLinked(module2.parameters)

# and we can add additional parameter which will be used as a global
step1.appendParameter(Parameter("STEP_ID","@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}","string","","",True, False, "Temporary fix"))
step1.setValue("appLog","@{appName}_@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}.log")
step1.unlinkParameter(["appLog","appName", "appType"])
step1.unlinkParameter(["CONFIG_NAME","CONFIG_VERSION"])


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
stepInstance1.setValue("appName", "Brunel")
stepInstance1.setValue("appType", "dst")
stepInstance1.setValue("outputData","@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}.@{appType}")
stepInstance1.linkParameterUp("CONFIG_NAME")
stepInstance1.linkParameterUp("CONFIG_VERSION")

# Now lets define parameters on the top
#indata = "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_1.sim;LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_2.sim;LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_3.sim"
indata = "LFN:/lhcb/production/CCRC08/v0/00002090/RAW/0000/00002090_00002534_1.raw"
# lets specify parameters on the level of workflow
workflow1.appendParameterCopyLinked(step1.parameters, step1_prefix)
# and finally we can unlink them because we inherit them linked
workflow1.unlinkParameter(workflow1.parameters)

workflow1.setValue(step1_prefix+"appVersion", "v32r3")
#JCworkflow1.setValue(step1_prefix+"CONFIG_NAME", "CCRC08")
#JCworkflow1.setValue(step1_prefix+"CONFIG_VERSION", "v0")
workflow1.setValue(step1_prefix+"optionsFile", "RealDataDst.opts")
workflow1.setValue(step1_prefix+"optionsLine","ApplicationMgr.EvtMax = 1")
workflow1.setValue(step1_prefix+"poolXMLCatName","pool_xml_catalog.xml")
workflow1.setValue(step1_prefix+"inputData",indata)
workflow1.setValue(step1_prefix+"inputDataType","MDF")
#workflow1.setValue(step1_prefix+"outputData","@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}.@{appType}")
workflow1.setValue(step1_prefix+"OUTPUT_MAX","10")
# remove unwanted
workflow1.removeParameter(step1_prefix+"outputData")
workflow1.removeParameter(step1_prefix+"systemConfig")
#add syspem config which common for all modules
workflow1.appendParameter(Parameter("systemConfig","slc4_amd64_gcc34","string","","",True, False, "Application Name"))


# and finally we can unlink them because we inherit them linked
workflow1.unlinkParameter(workflow1.parameters)

workflow1.appendParameter(Parameter("PRODUCTION_ID","00003033","string","","",True, False, "Temporary fix"))
workflow1.appendParameter(Parameter("JOB_ID","00000010","string","","",True, False, "Temporary fix"))
workflow1.appendParameter(Parameter("EMAILNAME","joel.closier@cern.ch","string","","",True, False, "Email to send a report from the LogCheck module"))
workflow1.appendParameter(Parameter("CONFIG_NAME","CCRC08","string","","",True, False, "Configuration Name"))
workflow1.appendParameter(Parameter("CONFIG_VERSION","v0","string","","",True, False, "Configuration Version"))
workflow1.toXMLFile('/afs/cern.ch/user/g/gkuznets/test.xml')


from DIRAC.Interfaces.API.Job                            import Job
from DIRAC.Interfaces.API.Dirac                            import Dirac


j = Job("/afs/cern.ch/user/j/joel/public/wkf_CCRC.xml")
d= Dirac()
print j._toXML()
d.submit(j,mode='local')

#w4 = fromXMLFile("/afs/cern.ch/user/j/joel/public/wkf_CCRC.xml")
#w4 = Workflow("/afs/cern.ch/user/j/joel/public/wkf_CCRC.xml")
#w4.toXMLFile('/afs/cern.ch/user/g/gkuznets/test_.xml')
#print 'Creating code for the workflow'
#print w4.toXML()
#print w4.createCode()
#eval(compile(w4.createCode(),'<string>','exec'))
#workflow1.execute()
