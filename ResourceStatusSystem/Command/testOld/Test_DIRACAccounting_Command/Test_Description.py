################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Command.test.Test_DIRACAccounting_Command import fixtures
_fixture = fixtures.DescriptionFixture

################################################################################

'''
Add test cases to the suite
'''
import DIRAC.ResourceStatusSystem.Command.test.Test_DIRACAccounting_Command.TestCase_Description as tcd

Test_Description_DIRACAccounting_Command               = tcd.TestCase_Description_DIRACAccounting_Command
Test_Description_TransferQuality_Command               = tcd.TestCase_Description_TransferQuality_Command
Test_Description_TransferQualityCached_Command         = tcd.TestCase_Description_TransferQualityCached_Command
Test_Description_CachedPlot_Command                    = tcd.TestCase_Description_CachedPlot_Command
Test_Description_TransferQualityFromCachedPlot_Command = tcd.TestCase_Description_TransferQualityFromCachedPlot_Command

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 