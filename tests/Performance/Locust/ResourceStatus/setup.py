"""
Pretest setup script before running the actual load test.

NOTE: Change to the directory of the config.yaml before starting the test using bzt
"""

import datetime
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient


rssClient = ResourceStatusClient()

# number of test rows to be inserted in the RSSDB
num_elements = 10

# clear the DB table before the test
res = rssClient.delete('ResourceStatus')
assert res['OK']

# insert dummy data into the table
for i in range(num_elements):
  current_time = datetime.datetime.utcnow()
  resource_name = "Res" + str(i)
  res = rssClient.insertStatusElement('Resource', 'Status', resource_name, 'statusType',
                                      'Active', 'elementType', 'reason', current_time,
                                      current_time, 'tokenOwner', current_time)
  assert res['OK']
