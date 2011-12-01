################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Agent.test.Test_RSInspectorAgent import fixtures
_fixture = fixtures.UnitFixture

################################################################################

'''
Add test cases to the suite
'''
from DIRAC.ResourceStatusSystem.Agent.test.Test_RSInspectorAgent.TestCase_Unit import TestCase_Unit

Test_Unit = TestCase_Unit

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 