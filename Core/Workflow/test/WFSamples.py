from Parameter import *
from Module import *
from Step import *
from Workflow import *

""" Collection of objects for the testing"""

body1 = """class PrintOutput(object):
  # Warning!!
  # class name MUST me the same as a Type field
  # and MUST have method execute
  class_var = 0 # static class parameter

  def __init__(self):
    # constructor code
    self.enable = 1 #local class parameter
    self.version = 0
    self.debug = False
    self.message = 'empty message'

  def execute(self):
    # main execution function
    if self.enable :
      if self.debug:
        print 'Executing Module = ',str(type(self))
      print self.message
    else:
      print 'Type1 - pass'


  def __del__(self):
    pass
"""

body2 = """class Summ(object):
  # Warning!!
  # class name MUST me the same as a Type field
  # and MUST have method execute
  class_var = 0 # static class parameter

  def __init__(self):
    # constructor code
    self.enable = 1 #local class parameter
    self.version = 0
    self.debug = False
    self.input1 = 0
    self.input2 = 0
    self.result = 0

  def execute(self):
    # main execution function
    if self.enable :
      if self.debug:
        print 'Executing Module = ',str(type(self))
      self.result=self.input1+self.input2
      if self.debug:
        print 'inputs are', self.input1, self.input2
        print 'Result is',self.result
    else:
      print str(type(self)), 'pass'


  def __del__(self):
    pass
"""

body3 = "from calendar import Calendar\n"


op1 = Parameter("enable","True","bool","","",True, False, "if False execution disabled")
op2 = Parameter("version","1.25","float","","",False, True, "we can get version of the module")
#op3 = Parameter("message","\'this is ugly module\'","string","","",False,False,"message for the printing")
op3 = Parameter("message","@{inparam4}","string","","",False,False,"message for the printing")
op4 = Parameter("debug", "False", "bool", "", "", True, False, "allows to print additional information")
op5 = Parameter("input1","2","int","","",True,False,"argument for addition")
op6 = Parameter("input2","5","int","","",True,False,"argument for addition")
op7 = Parameter("result","0","int","","",False,True,"argument for addition")

md1 = ModuleDefinition('PrintOutput')
md1.appendParameter( op1 )
md1.appendParameter( op2 )
md1.appendParameter( op3 )
md1.appendParameter( op4 )
md1.setDescription('Module to print imput messsage')
md1.setBody(body1)

md2 = ModuleDefinition('Summ')
md2.setBody(body2)
md2.appendParameter( op1 )
md2.appendParameter( op2 )
md2.appendParameter( op4 )
md2.appendParameter( op5 )
md2.appendParameter( op6 )
md2.appendParameter( op7 )

md3 = ModuleDefinition('PrintOutput')
md3.appendParameter( op1 )
md3.appendParameter( op2 )
md3.appendParameter( op3 )
md3.appendParameter( op4 )
md3.setDescription('Module to print imput messsage')
md3.setBody(body1)

sd1 = StepDefinition('TotalSumm')
sd1.addModule(md3)
sd1.addModule(md1)
sd1.addModule(md2)
sd1.appendParameter(Parameter("enable_inst1","True","bool","","",True, True, "enabling instance 1"))
sd1.appendParameter(Parameter("enable_inst2","True","bool","","",True, True, "enabling instance 2"))
sd1.appendParameter(Parameter("enable_inst3","True","bool","","",True, True, "enabling instance 3"))
sd1.appendParameter(Parameter("debug","True","bool","","",True, True, "enabling additional printing"))
sd1.appendParameter(Parameter("input1","3.8","float","","",True, False, "input slot"))
sd1.appendParameter(Parameter("input2","8.2","float","","",True, False, "input slot"))
sd1.appendParameter(Parameter("input3","2.0","float","","",True, False, "input slot"))
sd1.appendParameter(Parameter("result","0.0","float","","",False, True, "output"))
#sd1.append(Parameter("message","empty message","string","","",True, False, "output"))
sd1.appendParameter(Parameter("message","@{inparam4}","string","","",True, False, "output"))

mi1 = sd1.createModuleInstance('Summ', 'mi1')
mi2 = sd1.createModuleInstance('PrintOutput', 'mi2')
mi3 = sd1.createModuleInstance('Summ', 'mi3')
mi4 = sd1.createModuleInstance('PrintOutput', 'mi4')
mi5 = sd1.createModuleInstance('Summ', 'mi5')
mi6 = sd1.createModuleInstance('PrintOutput', 'mi6')

mi1.findParameter('enable').link('self','enable_inst1')
mi1.findParameter('debug').link('self','debug')
mi1.findParameter('input1').link('self','input1')
mi1.findParameter('input2').link('self','input2')

mi2.findParameter('enable').link('mi1','enable')
mi2.findParameter('debug').link('self','debug')
#mi2.findParameter('message').link('mi1','result') # taken from the level of step

mi3.findParameter('enable').link('self','enable_inst2')
mi3.findParameter('debug').link('self','debug')
mi3.findParameter('input1').link('self','input2')
mi3.findParameter('input2').link('self','input3')

mi4.findParameter('enable').link('mi3','enable')
mi4.findParameter('debug').link('self','debug')
#mi4.findParameter('message').link('mi3','result') # taken from the previouse instance
mi4.findParameter('message').link('mi3','result') # taken from the previouse instance

mi5.findParameter('enable').link('self','enable_inst3')
mi5.findParameter('debug').link('self','debug')
mi5.findParameter('input1').link('mi1','result')
mi5.findParameter('input2').link('mi3','result')

mi6.findParameter('enable').link('mi5','enable')
mi6.findParameter('debug').link('self','debug')
mi6.findParameter('message').link('mi5','result') # taken from the previouse instance (chain propagation)
sd1.findParameter('result').link('mi5','result')
#sd1.findParameter('message').link('self','inparam4') # taken from the level of step

w1 = Workflow('main')
w1.addStep(sd1)

w1.appendParameter(Parameter("final","0","float","","",False, True, "Final result"))
w1.appendParameter(Parameter("debug","True","bool","","",True, False, "Debug switch"))
w1.appendParameter(Parameter("message","vv@{inparam4}jj@{inpar2}ge","string","","",True, False, ""))
w1.appendParameter(Parameter("inparam4","VER","string","","",True, False, ""))
w1.appendParameter(Parameter("inpar2","SORTIE@{inparam4}","PARAM","","",True, False, ""))
si1 = w1.createStepInstance('TotalSumm', 'si1')
si2 = w1.createStepInstance('TotalSumm', 'si2')

si1.findParameter('debug').link('self','debug')
si2.findParameter('debug').link('self','debug')
si2.findParameter('input1').link('si1','result') # linking the results
w1.findParameter('final').link('si2','result')

