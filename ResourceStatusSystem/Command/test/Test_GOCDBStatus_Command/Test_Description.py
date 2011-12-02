################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Command.test.Test_GOCDBStatus_Command import fixtures
_fixture = fixtures.DescriptionFixture

################################################################################

'''
Add test cases to the suite
'''
import DIRAC.ResourceStatusSystem.Command.test.Test_GOCDBStatus_Command.TestCase_Description as tcd

Test_Description_GOCDBStatus_Command   = tcd.TestCase_Description_GOCDBStatus_Command
Test_Description_DTCached_Command      = tcd.TestCase_Description_DTCached_Command
Test_Description_DTInfo_Cached_Command = tcd.TestCase_Description_DTInfo_Cached_Command

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 