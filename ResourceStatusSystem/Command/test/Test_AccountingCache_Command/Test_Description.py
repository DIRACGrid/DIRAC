################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Set the fixture needed to run this test.
'''
from DIRAC.ResourceStatusSystem.Command.test.Test_AccountingCache_Command import fixtures
_fixture = fixtures.DescriptionFixture

################################################################################

'''
Add test cases to the suite
'''
import DIRAC.ResourceStatusSystem.Command.test.Test_AccountingCache_Command.TestCase_Description as tcd

Test_Description_TransferQualityByDestSplitted_Command       = tcd.TestCase_Description_TransferQualityByDestSplitted_Command
Test_Description_TransferQualityByDestSplittedSite_Command   = tcd.TestCase_Description_TransferQualityByDestSplittedSite_Command
Test_Description_TransferQualityBySourceSplittedSite_Command = tcd.TestCase_Description_TransferQualityBySourceSplittedSite_Command
Test_Description_FailedTransfersBySourceSplitted_Command     = tcd.TestCase_Description_FailedTransfersBySourceSplitted_Command
Test_Description_SuccessfullJobsBySiteSplitted_Command       = tcd.TestCase_Description_SuccessfullJobsBySiteSplitted_Command
Test_Description_FailedJobsBySiteSplitted_Command            = tcd.TestCase_Description_FailedJobsBySiteSplitted_Command
Test_Description_SuccessfullPilotsBySiteSplitted_Command     = tcd.TestCase_Description_SuccessfullPilotsBySiteSplitted_Command
Test_Description_FailedPilotsBySiteSplitted_Command          = tcd.TestCase_Description_FailedPilotsBySiteSplitted_Command
Test_Description_SuccessfullPilotsByCESplitted_Command       = tcd.TestCase_Description_SuccessfullPilotsByCESplitted_Command
Test_Description_FailedPilotsByCESplitted_Command            = tcd.TestCase_Description_FailedPilotsByCESplitted_Command
Test_Description_RunningJobsBySiteSplitted_Command           = tcd.TestCase_Description_RunningJobsBySiteSplitted_Command

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 