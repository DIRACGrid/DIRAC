"""
Teardown script after running the load test.

Deletes all the rows in the resource status table, which were added as a part of the test

NOTE: Change to the directory of the config.yaml before starting the test using bzt
"""

from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient


rssClient = ResourceStatusClient()

# clear the DB table
res = rssClient.delete('ResourceStatus')
assert res['OK']
