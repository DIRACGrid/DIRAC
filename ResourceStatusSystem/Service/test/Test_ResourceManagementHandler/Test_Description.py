################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Service.test.Test_ResourceManagementHandler import fixtures
_fixture = fixtures.DescriptionFixture

################################################################################

'''
Add test cases to the suite
'''
from DIRAC.ResourceStatusSystem.Service.test.Test_ResourceManagementHandler.TestCase_Description import TestCase_Description

Test_Description = TestCase_Description

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 