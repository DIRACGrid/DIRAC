################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Client.test.Test_ResourceStatusClient import fixtures
_fixture = fixtures.Description_withHandler

################################################################################

'''
Add test cases to the suite
'''
from DIRAC.ResourceStatusSystem.Client.test.Test_ResourceStatusClient.TestCase_Unit import TestCase_Unit

Test_Unit_withHandler = TestCase_Unit

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 