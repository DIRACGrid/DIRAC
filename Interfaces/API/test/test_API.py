import unittest
from mock import Mock

from DIRAC.Interfaces.API.Job import Job

class APITestCase( unittest.TestCase ):
  """ Base class for the Modules test cases
  """
  def setUp( self ):
    self.job = Job()

  def tearDown( self ):
    pass

class APISuccess( APITestCase ):

  def test_basicJob( self ):
    self.job.setOwner( 'ownerName' )
    self.job.setOwnerGroup( 'ownerGroup' )
    self.job.setName( 'jobName' )
    self.job.setJobGroup( 'jobGroup' )
    self.job.setExecutable( 'someExe' )
    self.job.setType( 'jobType' )
    self.job.setDestination( 'DIRAC.someSite.ch' )

    xml = self.job._toXML()

    expected = '''<Workflow>
<origin></origin>
<description><![CDATA[]]></description>
<descr_short></descr_short>
<version>0.0</version>
<type></type>
<name>jobName</name>
<Parameter name="JobType" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified type"><value><![CDATA[jobType]]></value></Parameter>
<Parameter name="Priority" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User Job Priority"><value><![CDATA[1]]></value></Parameter>
<Parameter name="JobGroup" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified job group"><value><![CDATA[jobGroup]]></value></Parameter>
<Parameter name="JobName" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified name"><value><![CDATA[jobName]]></value></Parameter>
<Parameter name="Site" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified destination site"><value><![CDATA[DIRAC.someSite.ch]]></value></Parameter>
<Parameter name="Origin" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Origin of client"><value><![CDATA[DIRAC]]></value></Parameter>
<Parameter name="StdOutput" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard output file"><value><![CDATA[std.out]]></value></Parameter>
<Parameter name="StdError" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard error file"><value><![CDATA[std.err]]></value></Parameter>
<Parameter name="InputData" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Default null input data value"><value><![CDATA[]]></value></Parameter>
<Parameter name="LogLevel" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Job Logging Level"><value><![CDATA[info]]></value></Parameter>
<Parameter name="ParametricInputData" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input data value"><value><![CDATA[]]></value></Parameter>
<Parameter name="ParametricInputSandbox" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input sandbox value"><value><![CDATA[]]></value></Parameter>
<Parameter name="ParametricParameters" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input parameters value"><value><![CDATA[]]></value></Parameter>
<Parameter name="Owner" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified ID"><value><![CDATA[ownerName]]></value></Parameter>
<Parameter name="OwnerGroup" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified owner group."><value><![CDATA[ownerGroup]]></value></Parameter>
<ModuleDefinition>
<body><![CDATA[
from DIRAC.Workflow.Modules.Script import Script
]]></body>
<origin></origin>
<description><![CDATA[ The Script class provides a simple way for users to specify an executable
    or file to run (and is also a simple example of a workflow module).
]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>Script</type>
</ModuleDefinition>
<StepDefinition>
<origin></origin>
<version>0.0</version>
<type>ScriptStep1</type>
<description><![CDATA[]]></description>
<descr_short></descr_short>
<Parameter name="executable" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Executable Script"><value><![CDATA[]]></value></Parameter>
<Parameter name="arguments" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Arguments for executable Script"><value><![CDATA[]]></value></Parameter>
<Parameter name="applicationLog" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Log file name"><value><![CDATA[]]></value></Parameter>
<ModuleInstance>
<type>Script</type>
<name>Script</name>
<descr_short></descr_short>
</ModuleInstance>
</StepDefinition>
<StepInstance>
<type>ScriptStep1</type>
<name>RunScriptStep1</name>
<descr_short></descr_short>
<Parameter name="executable" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Executable Script"><value><![CDATA[someExe]]></value></Parameter>
<Parameter name="arguments" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Arguments for executable Script"><value><![CDATA[]]></value></Parameter>
<Parameter name="applicationLog" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Log file name"><value><![CDATA[CodeOutput.log]]></value></Parameter>
</StepInstance>
</Workflow>
'''

    self.assertEqual( xml, expected )



#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( APITestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( APISuccess ) )
