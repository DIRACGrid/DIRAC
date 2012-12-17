################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Command.test.Test_ClientsCache_Command import fixtures
_fixture = fixtures.DescriptionFixture

################################################################################

'''
Add test cases to the suite
'''
import DIRAC.ResourceStatusSystem.Command.test.Test_ClientsCache_Command.TestCase_Description as tcd

Test_Description_JobsEffSimpleEveryOne_Command     = tcd.TestCase_Description_JobsEffSimpleEveryOne_Command
Test_Description_PilotsEffSimpleEverySites_Command = tcd.TestCase_Description_PilotsEffSimpleEverySites_Command
Test_Description_TransferQualityEverySEs_Command   = tcd.TestCase_Description_TransferQualityEverySEs_Command
Test_Description_DTEverySites_Command              = tcd.TestCase_Description_DTEverySites_Command
Test_Description_DTEveryResources_Command          = tcd.TestCase_Description_DTEveryResources_Command

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 