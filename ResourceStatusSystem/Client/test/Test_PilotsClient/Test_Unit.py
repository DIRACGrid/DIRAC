################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Client.test.Test_PilotsClient import fixtures
_fixture = fixtures.DescriptionFixture

################################################################################

'''
Add test cases to the suite
'''
from DIRAC.ResourceStatusSystem.Client.test.Test_PilotsClient.TestCase_Unit import TestCase_Unit

Test_Unit = TestCase_Unit

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 