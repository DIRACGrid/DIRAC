################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Command.test.Test_GGUSTickets_Command import fixtures
_fixture = fixtures.DescriptionFixture

################################################################################

'''
Add test cases to the suite
'''
import DIRAC.ResourceStatusSystem.Command.test.Test_GGUSTickets_Command.TestCase_Description as tcd

Test_Description_GGUSTickets_Open = tcd.TestCase_Description_GGUSTickets_Open
Test_Description_GGUSTickets_Link = tcd.TestCase_Description_GGUSTickets_Link
Test_Description_GGUSTickets_Info = tcd.TestCase_Description_GGUSTickets_Info

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 