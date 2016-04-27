''' Test_RSS_PolicySystem_EmailAction

    requires a 'work/ResourceStatus/' directory in 'DIRAC' environment variable path

    this is pytest!
'''

import os
import sys
import json
import tempfile
from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.EmailAction import EmailAction
from DIRAC.ResourceStatusSystem.Agent.EmailAgent                 import EmailAgent

gLogger.setLevel('DEBUG')

test_name = "ElementIsBanned"
test_decisionParams = {'status': 'Active', 'reason': None, 'tokenOwner': None, 'active': 'Active', 'name': 'SE1', 'element': 'Resource', 'elementType': 'StorageElement', 'statusType': 'ReadAccess'}
test_enforcementResult = {'Status': 'Banned', 'Reason': 'AlwaysBanned ###', 'PolicyAction': [('LogPolicyResultAction', 'LogPolicyResultAction'), ('ElementIsBanned', 'EmailAction'), ('LogStatusAction', 'LogStatusAction')]}
test_singlePolicyResults = [{'Status': 'Banned', 'Policy': {'command': None, 'name': 'AlwaysBannedForSE1SE2', 'args': None, 'type': 'AlwaysBanned', 'module': 'AlwaysBannedPolicy', 'description': 'A Policy that always returns Banned'}, 'Reason': 'AlwaysBanned'}, {'Status': 'Active', 'Policy': {'command': None, 'name': 'AlwaysActiveForResource', 'args': None, 'type': 'AlwaysActive', 'module': 'AlwaysActivePolicy', 'description': 'A Policy that always returns Active'}, 'Reason': 'AlwaysActive'}]

action = EmailAction(test_name, test_decisionParams, test_enforcementResult, test_singlePolicyResults)
agent  = EmailAgent("ResourceStatus/EmailAgent", "ResourceStatus/EmailAgent")

#The 'DIRAC' path is used by EmailAction's and EmailAgent's functions
#if 'DIRAC' path does not exists set it to /tmp
if not os.environ.get('DIRAC'):
  os.environ['DIRAC'] = tempfile.gettempdir() + "/"
#if the '/work/ResourceStatus/ subdirectory does not exists create it
if not os.path.isdir(os.getenv('DIRAC') + 'work/ResourceStatus/'):
  os.makedirs(os.getenv('DIRAC') + 'work/ResourceStatus/')

test_cacheFile = os.getenv('DIRAC') + 'work/ResourceStatus/' + 'cache.json'
test_siteName                   = 'LCG.test1234.ch'
test_status                     = 'Banned'
test_previousStatus             = 'Active'
test_statusType                 = 'ReadAccess'
test_name                       = 'SE1'
test_time                       = '2016-03-07 11:28:28'
test_dict                       = {"status": test_status, "previousStatus": test_previousStatus, "statusType": test_statusType, "name": test_name, "time": test_time}

def test_addAndRemove():

  # TEST addtoJSON
  # ...............................................................................

  result = action._addtoJSON(test_siteName, test_dict)
  #check if there was any errors
  assert result['OK'] == True
  #check if the file is really there
  assert os.path.isfile(test_cacheFile) == True
  #ensure that the file is not empty
  assert (os.stat(test_cacheFile).st_size > 0) == True

  with open(test_cacheFile) as f:
        loaded_test_dict = json.load(f)

  assert ( test_siteName in loaded_test_dict ) == True

  for data in loaded_test_dict[test_siteName]:
    assert data['statusType'] == test_statusType
    assert data['name'] == test_name
    assert data['status'] == test_status
    assert data['time'] == test_time
    assert data['previousStatus'] == test_previousStatus

  # TEST emailBodyGenerator
  # ...............................................................................

  result = agent._emailBodyGenerator(loaded_test_dict, test_siteName)
  assert result == test_statusType + ' of ' + test_name + ' has been ' + test_status + ' since ' + test_time + ' (Previous status: ' + test_previousStatus + ')\n'

  # TEST deleteCacheFile
  # ...............................................................................

  result = agent._deleteCacheFile()
  assert result['OK'] == True
  #the file must not be there now
  assert os.path.isfile(test_cacheFile) == False

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
