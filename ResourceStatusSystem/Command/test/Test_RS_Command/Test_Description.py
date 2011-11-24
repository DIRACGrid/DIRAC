################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Command.test.Test_RS_Command import fixtures
_fixture = fixtures.DescriptionFixture

################################################################################

'''
Add test cases to the suite
'''
import DIRAC.ResourceStatusSystem.Command.test.Test_RS_Command.TestCase_Description as tcd

Test_Description_RSPeriods_Command            = tcd.TestCase_Description_RSPeriods_Command
Test_Description_ServiceStats_Command         = tcd.TestCase_Description_ServiceStats_Command
Test_Description_ResourceStats_Command        = tcd.TestCase_Description_ResourceStats_Command
Test_Description_StorageElementsStats_Command = tcd.TestCase_Description_StorageElementsStats_Command
Test_Description_MonitoredStatus_Command      = tcd.TestCase_Description_MonitoredStatus_Command

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 