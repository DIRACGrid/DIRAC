################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.DB.test.Test_ResourceManagementDB import fixtures
_fixture = fixtures.DescriptionFixture

################################################################################

'''
Add test cases to the suite
'''
#import Unit
from DIRAC.ResourceStatusSystem.DB.test.Test_ResourceManagementDB.TestCase_Unit import TestCase_Unit

Test_Unit = TestCase_Unit

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 