################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Command.test.Test_Jobs_Command import fixtures
_fixture = fixtures.DescriptionFixture

################################################################################

'''
Add test cases to the suite
'''
import DIRAC.ResourceStatusSystem.Command.test.Test_Jobs_Command.TestCase_Description as tcd

Test_Description_JobsStats_Command           = tcd.TestCase_Description_JobsStats_Command
Test_Description_JobsEff_Command             = tcd.TestCase_Description_JobsEff_Command
Test_Description_SystemCharge_Command        = tcd.TestCase_Description_SystemCharge_Command
Test_Description_JobsEffSimple_Command       = tcd.TestCase_Description_JobsEffSimple_Command
Test_Description_JobsEffSimpleCached_Command = tcd.TestCase_Description_JobsEffSimpleCached_Command

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 