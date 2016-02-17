''' This is a test of the chain
    ResourceManagementClient -> ResourceManagementHandler -> ResourceManagementDB
    It supposes that the DB is present, and that the service is running

    this is pytest!
'''

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

gLogger.setLevel('DEBUG')

rsClient = ResourceManagementClient()

def test_addAndRemove():

  # TEST addOrModifyAccountingCache
  # ...............................................................................

  res = rsClient.addOrModifyAccountingCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectAccountingCache('TestName1234')
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234'
  assert res['Value'][0][0] == 'TestName1234'


  # TEST deleteAccountingCache
  # ...............................................................................
  res = rsClient.deleteAccountingCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectAccountingCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']



  # TEST addOrModifyGGUSTicketsCache
  # ...............................................................................

  res = rsClient.addOrModifyGGUSTicketsCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectGGUSTicketsCache('TestName1234')
  assert res['OK'] == True
  assert res['Value'][0][2] == 'TestName1234'


  # TEST deleteGGUSTicketsCache
  # ...............................................................................

  res = rsClient.deleteGGUSTicketsCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectGGUSTicketsCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST addOrModifyDowntimeCache
  # ...............................................................................

  res = rsClient.addOrModifyDowntimeCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectDowntimeCache('TestName1234')
  assert res['OK'] == True
  assert res['Value'][0][3] == 'TestName1234'


  # TEST deleteDowntimeCache
  # ...............................................................................

  res = rsClient.deleteDowntimeCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectDowntimeCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST addOrModifyJobCache
  # ...............................................................................

  res = rsClient.addOrModifyJobCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectJobCache('TestName1234')
  assert res['OK'] == True
  assert res['Value'][0][3] == 'TestName1234'


  # TEST deleteJobCache
  # ...............................................................................

  res = rsClient.deleteJobCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectJobCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST addOrModifyTransferCache
  # ...............................................................................

  res = rsClient.addOrModifyTransferCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectTransferCache('TestName1234')
  assert res['OK'] == True
  assert res['Value'][0][0] == 'TestName1234'


  # TEST deleteTransferCache
  # ...............................................................................

  res = rsClient.deleteTransferCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectTransferCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST addOrModifyPilotCache
  # ...............................................................................

  res = rsClient.addOrModifyPilotCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectPilotCache('TestName1234')
  assert res['OK'] == True
  assert res['Value'][0][2] == 'TestName1234'


  # TEST deletePilotCache
  # ...............................................................................

  res = rsClient.deletePilotCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectPilotCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST addOrModifyPolicyResult
  # ...............................................................................

  res = rsClient.addOrModifyPolicyResult('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectPolicyResult('TestName1234')
  assert res['OK'] == True
  assert res['Value'][0][7] == 'TestName1234'


  # TEST deletePolicyResult
  # ...............................................................................

  res = rsClient.deletePolicyResult('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectPolicyResult('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST addOrModifyPolicyResultLog
  # ...............................................................................

  res = rsClient.addOrModifyPolicyResultLog('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectPolicyResultLog('TestName1234')
  assert res['OK'] == True
  assert res['Value'][0][8] == 'TestName1234'


  # TEST deletePolicyResultLog
  # ...............................................................................

  res = rsClient.deletePolicyResultLog('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectPolicyResultLog('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST addOrModifySpaceTokenOccupancyCache
  # ...............................................................................

  res = rsClient.addOrModifySpaceTokenOccupancyCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectSpaceTokenOccupancyCache('TestName1234')
  assert res['OK'] == True
  assert res['Value'][0][0] == 'TestName1234'


  # TEST deleteSpaceTokenOccupancyCache
  # ...............................................................................

  res = rsClient.deleteSpaceTokenOccupancyCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectSpaceTokenOccupancyCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST addOrModifyUserRegistryCache
  # ...............................................................................

  res = rsClient.addOrModifyUserRegistryCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectUserRegistryCache('TestName1234')
  assert res['OK'] == True
  assert res['Value'][0][0] == 'TestName1234'


  # TEST deleteUserRegistryCache
  # ...............................................................................

  res = rsClient.deleteUserRegistryCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectUserRegistryCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST addOrModifyVOBOXCache
  # ...............................................................................

  res = rsClient.addOrModifyVOBOXCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectVOBOXCache('TestName1234')
  assert res['OK'] == True
  assert res['Value'][0][2] == 'TestName1234'


  # TEST deleteVOBOXCache
  # ...............................................................................

  res = rsClient.deleteVOBOXCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectVOBOXCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST insertErrorReportBuffer
  # ...............................................................................

  res = rsClient.insertErrorReportBuffer('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectErrorReportBuffer('TestName1234')
  assert res['OK'] == True
  assert res['Value'][0][1] == 'TestName1234'


  # TEST deleteErrorReportBuffer
  # ...............................................................................

  res = rsClient.deleteErrorReportBuffer('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectErrorReportBuffer('TestName1234')
  assert res['OK'] == True
  assert not res['Value']

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF


