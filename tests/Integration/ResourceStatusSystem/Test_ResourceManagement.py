''' This is a test of the chain
    ResourceManagementClient -> ResourceManagementHandler -> ResourceManagementDB
    It supposes that the DB is present, and that the service is running

    this is pytest!
'''

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import datetime
from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

gLogger.setLevel('DEBUG')

rsClient = ResourceManagementClient()

dateEffective = datetime.datetime.now()
lastCheckTime = datetime.datetime.now()

def test_addAndRemove():

  rsClient.deleteAccountingCache('TestName12345')

  # TEST addOrModifyAccountingCache
  # ...............................................................................

  res = rsClient.addOrModifyAccountingCache('TestName12345', 'plotType', 'plotName', 'result', dateEffective, lastCheckTime)
  assert res['OK'] == True

  res = rsClient.selectAccountingCache('TestName12345')
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName12345'
  assert res['Value'][0][0] == 'TestName12345'

  res = rsClient.addOrModifyAccountingCache('TestName12345', 'plotType', 'plotName', 'changedresult', dateEffective, lastCheckTime)
  assert res['OK'] == True

  res = rsClient.selectAccountingCache('TestName12345')
  #check if the result has changed
  assert res['Value'][0][3] == 'changedresult'


  # TEST deleteAccountingCache
  # ...............................................................................
  res = rsClient.deleteAccountingCache('TestName12345')
  assert res['OK'] == True

  res = rsClient.selectAccountingCache('TestName12345')
  assert res['OK'] == True
  assert not res['Value']


# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF


