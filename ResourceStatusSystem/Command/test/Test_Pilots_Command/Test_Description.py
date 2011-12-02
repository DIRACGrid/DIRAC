################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Command.test.Test_Pilots_Command import fixtures
_fixture = fixtures.DescriptionFixture

################################################################################

'''
Add test cases to the suite
'''
import DIRAC.ResourceStatusSystem.Command.test.Test_Pilots_Command.TestCase_Description as tcd

Test_Description_PilotsStats_Command           = tcd.TestCase_Description_PilotsStats_Command
Test_Description_PilotsEff_Command             = tcd.TestCase_Description_PilotsEff_Command
Test_Description_PilotsEffSimple_Command       = tcd.TestCase_Description_PilotsEffSimple_Command
Test_Description_PilotsEffSimpleCached_Command = tcd.TestCase_Description_PilotsEffSimpleCached_Command

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 