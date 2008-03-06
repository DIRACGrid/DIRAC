from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *

#define Module 2
module2 = ModuleDefinition('GaudiApplication')#during constraction class creates duplicating copies of the params
module2.setDescription('Gaudi Application module')
module2.setBody('from WorkflowLib.Module.GaudiApplication import GaudiApplication\n')

# we add empty parameters but linked up as default
module2.appendParameter(Parameter("DataType","","string","self","DataType",True, False, "data type"))
module2.appendParameter(Parameter("CONFIG_NAME","","string","self","CONFIG_NAME",True, False, "Configuration Name"))
module2.appendParameter(Parameter("CONFIG_VERSION","","string","self","CONFIG_VERSION",True, False, "Configuration Version"))
module2.appendParameter(Parameter("EVENTTYPE","","string","self","EVENTTYPE",True, False, "Event Type"))
module2.appendParameter(Parameter("appName","","string","self","appName",True, False, "Application Name"))
module2.appendParameter(Parameter("appVersion","","string","self","appVersion",True, False, "Application Version"))
module2.appendParameter(Parameter("appType","","string","self","appType",True,False,"Application Version"))
module2.appendParameter(Parameter("appLog","","string","self","appLog",True,False,"list of logfile"))
module2.appendParameter(Parameter("inputData","","jdl","self","inputData",True, False, "List of InputData"))
module2.appendParameter(Parameter("inputDataType","","string","self","inputDataType",True, False, "Input Data Type"))
module2.appendParameter(Parameter("nb_events_input","","string","self","nb_events_input",True,False,"number of events as input"))
module2.appendParameter(Parameter("NUMBER_OF_EVENTS","","string","self","NUMBER_OF_EVENTS",True, False, "number of events requested"))
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
module3.appendParameter(Parameter("DataType","","string","self","DataType",True, False, "data type"))
module3.appendParameter(Parameter("CONFIG_NAME","","string","self","CONFIG_NAME",True, False, "Configuration Name"))
module3.appendParameter(Parameter("CONFIG_VERSION","","string","self","CONFIG_VERSION",True, False, "Configuration Version"))
module3.appendParameter(Parameter("NUMBER_OF_EVENTS","","string","self","NUMBER_OF_EVENTS",True, False, "number of events requested"))
module3.appendParameter(Parameter("nb_events_input","","string","self","nb_events_input",True,False,"number of events as input"))
module3.appendParameter(Parameter("appName","","string","self","appName",True, False, "Application Name"))
module3.appendParameter(Parameter("appVersion","","string","self","appVersion",True, False, "Application Version"))
module3.appendParameter(Parameter("appType","","string","self","appType",True,False,"Application Version"))
module3.appendParameter(Parameter("appLog","","string","self","appLog",True,False,"list of logfile"))
module3.appendParameter(Parameter("poolXMLCatName","","string","self","poolXMLCatName",True,False,"POOL XML slice"))
module3.appendParameter(Parameter("inputData","","string","self","inputData",True,False,"InputData"))
module3.appendParameter(Parameter("OUTPUT_MAX","","string","self","OUTPUT_MAX",True,False,"nb max of output to keep"))
# this parameter is static so we define it here
module3.appendParameter(Parameter("EMAIL","@{EMAILNAME}","string","","",True,False,"EMAIL adress"))
module3.appendParameter(Parameter("outputDataSE","Tier1-RDST","string","","",True,False,"SE of output data"))


#define module 4
module4 = ModuleDefinition('BookkeepingReport')
module4.setDescription('Bookkeeping Report module')
module4.setBody('from WorkflowLib.Module.BookkeepingReport import * \n')
module4.appendParameter(Parameter("STEP_ID","","string","self","STEP_ID",True,False,"EMAIL adress"))
module4.appendParameter(Parameter("nb_events_input","","string","self","nb_events_input",True,False,"number of events as input"))
module4.appendParameter(Parameter("NUMBER_OF_EVENTS","","string","self","NUMBER_OF_EVENTS",True, False, "number of events requested"))
module4.appendParameter(Parameter("DataType","","string","self","DataType",True, False, "data type"))
module4.appendParameter(Parameter("CONFIG_NAME","","string","self","CONFIG_NAME",True, False, "Configuration Name"))
module4.appendParameter(Parameter("CONFIG_VERSION","","string","self","CONFIG_VERSION",True, False, "Configuration Version"))
module4.appendParameter(Parameter("appName","","string","self","appName",True, False, "Application Name"))
module4.appendParameter(Parameter("appVersion","","string","self","appVersion",True, False, "Application Version"))
module4.appendParameter(Parameter("inputData","","string","self","inputData",True,False,"InputData"))
module4.appendParameter(Parameter("inputDataType","","string","self","inputDataType",True, False, "Input Data Type"))
module4.appendParameter(Parameter("EVENTTYPE","","string","self","EVENTTYPE",True, False, "Event Type"))
module4.appendParameter(Parameter("outputData",'',"string","self","outputData",True,False,"list of output data"))
module4.appendParameter(Parameter("appType","","string","self","appType",True,False,"Application Version"))
module4.appendParameter(Parameter("poolXMLCatName","","string","self","poolXMLCatName",True,False,"POOL XML slice"))
module4.appendParameter(Parameter("appLog","","string","self","appLog",True,False,"list of logfile"))


#define module 5
module5 = ModuleDefinition('JobFinalization')
module5.setDescription('Job Finalization module')
module5.setBody('from WorkflowLib.Module.JobFinalization import * \n')

# commented by KGG
#module5.appendParameter(Parameter("outputDataSE","Tier1-RDST","string","","",True,False,"SE of output data"))
#module5.appendParameter(Parameter("outputData",'',"string","self","outputData",True,False,"list of output data"))
module5.appendParameter(Parameter("poolXMLCatName","","string","self","poolXMLCatName",True,False,"POOL XML slice"))
#module5.appendParameter(Parameter("inputData","","string","self","inputData",True,False,"InputData"))
#module5.appendParameter(Parameter("appType","","string","self","appType",True,False,"Application Version"))


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
#step1.addModule(module5)
#moduleInstance5 = step1.createModuleInstance('JobFinalization','module5')

# now we can add parameters for the STEP but instead of typing them we can just use old one from modules
step1.appendParameterCopyLinked(module3.parameters)
step1.appendParameterCopyLinked(module2.parameters)

# and we can add additional parameter which will be used as a global
step1.appendParameter(Parameter("STEP_ID","@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}","string","","",True, False, "Temporary fix"))
step1.appendParameter(Parameter("EVENTTYPE","30000000","string","","",True, False, "Event Type"))
step1.setValue("appLog","@{appName}_@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}.log")
step1.unlinkParameter(["appLog","appName", "appType"])
step1.unlinkParameter(["DataType", "CONFIG_NAME","CONFIG_VERSION","NUMBER_OF_EVENTS"])

step3 = StepDefinition('Job_Finalization')
step3.addModule(module5)
moduleInstance5 = step3.createModuleInstance('JobFinalization','module5')

# adding data input
moduleInstance5.appendParameter(Parameter("outputData_1",'',"string","self","outputData_1",True,False,"list of output data"))
moduleInstance5.appendParameter(Parameter("appType_1","","string","self","appType_1",True,False,"Application Version"))
moduleInstance5.appendParameter(Parameter("outputDataSE_1","","string","self","outputDataSE_1",True,False,"SE of output data"))

moduleInstance5.appendParameter(Parameter("outputData_2",'',"string","self","outputData_2",True,False,"list of output data"))
moduleInstance5.appendParameter(Parameter("appType_2","","string","self","appType_2",True,False,"Application Version"))
moduleInstance5.appendParameter(Parameter("outputDataSE_2","","string","self","outputDataSE_2",True,False,"SE of output data"))


##############  WORKFLOW #################################
workflow1 = Workflow(name='CCRC-joel-test')
workflow1.setDescription('Workflow of GaudiApplication')

workflow1.addStep(step1)
step1_prefix="step1_"
stepInstance1 = workflow1.createStepInstance('Gaudi_App_Step', 'Step1')
# lets link all parameters them up with the level of workflow
stepInstance1.linkParameterUp(stepInstance1.parameters, step1_prefix)
stepInstance1.setLink("systemConfig","self", "SystemConfig") # capital letter corrected
# except "STEP_ID", "appLog"
stepInstance1.unlinkParameter(["STEP_ID", "appLog","appName", "appType", "outputData", "EVENTTYPE"])
stepInstance1.setValue("appName", "Brunel")
stepInstance1.setValue("appType", "rdst")
stepInstance1.setValue("outputData","@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}.@{appType}")
stepInstance1.linkParameterUp("CONFIG_NAME")
stepInstance1.linkParameterUp("CONFIG_VERSION")
stepInstance1.linkParameterUp("DataType")
stepInstance1.linkParameterUp("NUMBER_OF_EVENTS")
stepInstance1.setLink("inputData","self", "InputData") # KGG linked with InputData of the Workflow

step2_prefix="step2_"
stepInstance2 = workflow1.createStepInstance('Gaudi_App_Step', 'Step2')
# lets link all parameters them up with the level of workflow
stepInstance2.linkParameterUp(stepInstance2.parameters, step2_prefix)
stepInstance2.setLink("systemConfig","self", "SystemConfig") # capital letter corrected
# except "STEP_ID", "appLog"
stepInstance2.unlinkParameter(["STEP_ID", "appLog","appName", "appType", "outputData", "EVENTTYPE"])
stepInstance2.setValue("appName", "DaVinci")
stepInstance2.setValue("appType", "etc")
stepInstance2.setValue("outputData","@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}.@{appType}")
stepInstance2.linkParameterUp("CONFIG_NAME")
stepInstance2.linkParameterUp("CONFIG_VERSION")
stepInstance2.linkParameterUp("DataType")
stepInstance2.linkParameterUp("NUMBER_OF_EVENTS")
stepInstance2.setLink("inputData",stepInstance1.getName(),"outputData")

workflow1.addStep(step3)
step3_prefix="step3_"
stepInstance3 = workflow1.createStepInstance('Job_Finalization', 'Step3')
stepInstance3.appendParameterCopyLinked(moduleInstance5.parameters)
stepInstance3.setLink("outputData_1",stepInstance1.getName(),"outputData")
stepInstance3.setLink("appType_1",stepInstance1.getName(),"appType")
stepInstance3.setLink("outputDataSE_1",stepInstance1.getName(),"outputDataSE")

stepInstance3.setLink("outputData_2",stepInstance2.getName(),"outputData")
stepInstance3.setLink("appType_2",stepInstance2.getName(),"appType")
stepInstance3.setLink("outputDataSE_2",stepInstance2.getName(),"outputDataSE")
#stepInstance3.linkParameterUp(stepInstance3.parameters, step3_prefix)

# Now lets define parameters on the top
#indata = "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_1.sim;LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_2.sim;LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_3.sim"
indata = "LFN:/lhcb/production/CCRC08/v0/00002090/RAW/0000/00002090_00002534_1.raw"
etcf = "self.outputData"
opt_dav = "EvtTupleSvc.Output = {}"
opt_dav = opt_dav+";ApplicationMgr.OutStream -= {'DstWriter'}"
opt_dav = opt_dav+";ApplicationMgr.OutStream = {'Sequencer/SeqWriteTag'}"
opt_dav = opt_dav+";ApplicationMgr.TopAlg -= { \"GaudiSequencer/SeqPreselHWZ2bbl\" }"
opt_dav = opt_dav+";MessageSvc.Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc.timeFormat = '%Y-%m-%d %H:%M:%S UTC'"
opt_dav = opt_dav+";WR.Output = \"Collection=\'EVTTAGS/TagCreator/1\' ADDRESS=\'/Event\' DATAFILE=\'"+etcf+"\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\""
#indata = "LFN:/lhcb/data/CCRC08/RAW/LHCb/CCRC/402154/402154_0000047096.raw;LFN:/lhcb/data/CCRC08/RAW/LHCb/CCRC/402154/402154_0000047097.raw"
# lets specify parameters on the level of workflow
workflow1.appendParameterCopyLinked(step1.parameters, step1_prefix)
workflow1.appendParameterCopyLinked(step1.parameters, step2_prefix)
# and finally we can unlink them because we inherit them linked
workflow1.unlinkParameter(workflow1.parameters)

workflow1.setValue(step1_prefix+"appVersion", "v32r3p1")
workflow1.setValue(step2_prefix+"appVersion", "v19r10")
workflow1.setValue(step1_prefix+"nb_events_input", "@{NUMBER_OF_EVENTS}")
workflow1.setValue(step2_prefix+"nb_events_input", "-1")
workflow1.setValue(step1_prefix+"optionsFile", "RealDataRdst.opts")
#workflow1.setValue(step2_prefix+"optionsFile", "RealDataRdst.opts")
workflow1.setValue(step2_prefix+"optionsFile", "DVOfficialStrippingFile.opts")
workflow1.setValue(step2_prefix+"optionsLine",opt_dav)
#workflow1.setValue(step2_prefix+"optionsLine","""WR.Output = \"Collection=\'EVTTAGS/TagCreator/1\' ADDRESS=\'/Event\' DATAFILE=\'etcf\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\" """)
workflow1.setValue(step1_prefix+"optionsLine","#include \"$BRUNELOPTS/SuppressWarnings.opts\";MessageSvc.Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc.timeFormat = '%Y-%m-%d %H:%M:%S UTC';EventLoopMgr.OutputLevel = 3")
workflow1.setValue(step1_prefix+"poolXMLCatName","pool_xml_catalog.xml")
workflow1.setValue(step2_prefix+"poolXMLCatName","pool_xml_catalog.xml")
#workflow1.setValue(step1_prefix+"inputData",indata)
workflow1.removeParameter(step1_prefix+"inputData") # KGG wrong parameter
workflow1.setValue(step1_prefix+"inputDataType","MDF")
workflow1.setValue(step1_prefix+"OUTPUT_MAX","20")
# remove unwanted
workflow1.removeParameter(step1_prefix+"outputData")
workflow1.removeParameter(step1_prefix+"systemConfig")
workflow1.removeParameter(step2_prefix+"outputData")
workflow1.removeParameter(step2_prefix+"systemConfig")
#add syspem config which common for all modules
#workflow1.appendParameter(Parameter("systemConfig","slc4_ia32_gcc34","string","","",True, False, "Application Name"))
workflow1.appendParameter(Parameter("SystemConfig","slc4_ia32_gcc34","JDLReqt","","",True, False, "Application Name"))

workflow1.appendParameter(Parameter("InputData",indata,"JDL","","",True, False, "Application Name"))
workflow1.appendParameter(Parameter("JobType","test","JDL","","",True, False, "Job TYpe"))
workflow1.appendParameter(Parameter("Owner","joel","JDL","","",True, False, "user Name"))
workflow1.appendParameter(Parameter("StdError","std.err","JDL","","",True, False, "user Name"))
workflow1.appendParameter(Parameter("StdOutput","std.out","JDL","","",True, False, "user Name"))
workflow1.appendParameter(Parameter("SoftwarePackages","Brunel.v32r3p1","JDL","","",True, False, "software"))

workflow1.appendParameter(Parameter("MaxCPUTime",20000,"JDLReqt","","",True, False, "Application Name"))
#workflow1.appendParameter(Parameter("Site","LCG.CERN.ch","JDLReqt","","",True, False, "Site"))
workflow1.appendParameter(Parameter("Platform","gLite","JDLReqt","","",True, False, "platform"))

# and finally we can unlink them because we inherit them linked
workflow1.unlinkParameter(workflow1.parameters)

workflow1.appendParameter(Parameter("PRODUCTION_ID","00003033","string","","",True, False, "Temporary fix"))
workflow1.appendParameter(Parameter("JOB_ID","00000011","string","","",True, False, "Temporary fix"))
workflow1.appendParameter(Parameter("EMAILNAME","joel.closier@cern.ch","string","","",True, False, "Email to send a report from the LogCheck module"))
workflow1.appendParameter(Parameter("DataType","DATA","string","","",True, False, "type of Datatype"))
workflow1.appendParameter(Parameter("CONFIG_NAME","LHCb","string","","",True, False, "Configuration Name"))
workflow1.appendParameter(Parameter("CONFIG_VERSION","CCRC08","string","","",True, False, "Configuration Version"))
workflow1.appendParameter(Parameter("NUMBER_OF_EVENTS","5","string","","",True, False, "number of events requested"))
workflow1.toXMLFile('wkf_CCRC_3.xml')
#w4 = fromXMLFile("/afs/cern.ch/user/g/gkuznets/test1.xml")
#print 'Creating code for the workflow'
#print workflow1.createCode()
print workflow1.showCode()
#eval(compile(workflow1.createCode(),'<string>','exec'))
#workflow1.execute()
