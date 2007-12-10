# $Id: JobSamples.py,v 1.2 2007/12/10 23:59:34 gkuznets Exp $
"""
    This is a comment
"""
__RCSID__ = "$Revision: 1.2 $"

# $Source: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Workflow/test/JobSamples.py,v $

from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *

""" Collection of objects for the testing"""


# general module params
moduleParams = ParameterCollection()
enable = Parameter("enable","True","bool","","",True, False, "if False execution disabled")
moduleParams.append(enable)
loglevel = Parameter("debug", "False", "bool", "", "", True, False, "allows to print additional information")
moduleParams.append(loglevel)
result = Parameter("result","Error",'string','','',False,True,"Result should be dictionary")
moduleParams.append(result)
logfile = Parameter('logfile','None','string','','',False,True,'Log file')
moduleParams.append(logfile)

#Gaudi Application specific
gaudiParams = ParameterCollection()
gaudiParams.append(Parameter("appName","Brunel","string","","",True, False, "Application Name"))
gaudiParams.append(Parameter("appVersion","v32r1","string","","",True, False, "Application Version"))
gaudiParams.append(Parameter("optionsFile","v200601.opts","string","","",True,False,"Options File"))
#gaudiParams.append(Parameter("optionsFile","$L0DUOPTS/L0DUConfig_alg1_set0_lumi2.opts","string","","",True,False,"Options File"))
gaudiParams.append(Parameter('optionsLine','ApplicationMgr.EvtMax = 5','string','','',True,False,'Number of Event','option'))
#gaudiParams.append(Parameter('optionsLine','ApplicationMgr().EvtMax = 5;PoolDbCacheSvc().Catalog = [\"xmlcatalog_file:joel.xml\"]','string','','',True,False,'Number of Event','option'))
gaudiParams.append(Parameter("systemConfig","slc4_ia32_gcc34","string","","",True,False,"Job Platform"))

module1 = ModuleDefinition('GaudiApplication', moduleParams )#during constraction class creates duplicating copies of the params
module1.appendParameterCopy(gaudiParams)#again we creating duplicates
module1.setDescription('Gaudi Application module')
module1.setBody('from WorkflowLib.Module.GaudiApplication import GaudiApplication\n')

step1 = StepDefinition('Gaudi_App_Step',gaudiParams) # again creating deep copy of the parameters
step1.addModule(module1)

#for p in gaudiParams:
#  step1.appendParameter(Parameter(parameter=p))
#step1.appendParameter(Parameter(parameter=p)

moduleInstance = step1.createModuleInstance('GaudiApplication', 'Module1')

# in this instance we want to connect parm belong to ModuleInstance with the Step perams
for p in gaudiParams:
  moduleInstance.findParameter(p.getName()).link('self',p.getName()) # input params


step1.appendParameterCopy(result)
step1.appendParameterCopy(logfile)
step1.findParameter('result').link(moduleInstance.getName(),'result') # output param
step1.findParameter('logfile').link(moduleInstance.getName(),'logfile')

workflow1 = Workflow('main')
workflow1.setDescription('Workflow of GaudiApplication')
workflow1.appendParameterCopy(result)
workflow1.appendParameterCopy(logfile)

workflow1.addStep(step1)
stepInstance1 = workflow1.createStepInstance('Gaudi_App_Step', 'Step1')

# there is a problem here
# we might have a several steps to lets change the names of the variables
step_prefix="step1_"
for p in gaudiParams:
  newparm = Parameter(parameter=p)
  newparm.setName(step_prefix+p.getName())
  workflow1.appendParameter(newparm)
  stepInstance1.findParameter(p.getName()).link('self',step_prefix+p.getName())


workflow1.findParameter('result').link(stepInstance1.getName(),'result')
workflow1.findParameter('logfile').link(stepInstance1.getName(),'logfile')
#workflow1.findParameter('command').setValue('ls -l')

#we have a choise, we can change defaul values in the gaudiParams for the new step
# or we can change values after the creation
# there is 1st approach
gaudiParams.find("appName").setValue("Boole")
gaudiParams.find("appVersion").setValue("v8r16")

#let as to create another step instance
stepInstance2 = workflow1.createStepInstance('Gaudi_App_Step', 'Step2')
step_prefix="step2_"
for p in gaudiParams:
  newparm = Parameter(parameter=p)
  newparm.setName(step_prefix+p.getName())
  workflow1.appendParameter(newparm)
  stepInstance2.findParameter(p.getName()).link('self',step_prefix+p.getName())

# There is a second solution (see above)
#workflow1.findParameter(step_prefix+"appName").setValue("Boole")
#workflow1.findParameter(step_prefix+"appVersion").setValue("v8r16")
testFile = '/opt/DIRAC3/DIRAC/Core/Workflow/test/jobDescription.xml'
#testFile = '/opt/DIRAC3/DIRAC/Core/Workflow/test/testjobxml.xml'
#workflow1.toXMLFile(testFile)
#print workflow1.createCode()
w4 = fromXMLFile(testFile)
#print 'Creating code for the workflow'
#print w4.createCode()
w4.execute()
#print w4.createCode()
#w4.execute()
