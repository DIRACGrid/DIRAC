################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.PolicySystem.test.Test_PEP import fixtures
_fixture = fixtures.UnitFixture

################################################################################

'''
Add test cases to the suite
'''
#import Unit
from DIRAC.ResourceStatusSystem.PolicySystem.test.Test_PEP.TestCase_Description import TestCase_Description

Test_Description = TestCase_Description

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 