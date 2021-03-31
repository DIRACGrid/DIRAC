""" This tests only need the ElasticJobParametersDB, and connects directly to it
"""

import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.ElasticJobParametersDB import ElasticJobParametersDB

#  Add a time delay to allow updating the modified index before querying it.
SLEEP_DELAY = 2

gLogger.setLevel('DEBUG')
elasticJobParametersDB = ElasticJobParametersDB()


def test_setAndGetJobFromDB():
  res = elasticJobParametersDB.setJobParameter(100, 'DIRAC', 'dirac@cern')
  assert res['OK']
  time.sleep(SLEEP_DELAY)

  res = elasticJobParametersDB.getJobParameters(100)
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern'

  # update it
  res = elasticJobParametersDB.setJobParameter(100, 'DIRAC', 'dirac@cern.cern')
  assert res['OK']
  time.sleep(SLEEP_DELAY)
  res = elasticJobParametersDB.getJobParameters(100)
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  res = elasticJobParametersDB.getJobParameters(100, ['DIRAC'])
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  res = elasticJobParametersDB.getJobParameters(100, 'DIRAC')
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'

  # add one
  res = elasticJobParametersDB.setJobParameter(100, 'someKey', 'someValue')
  assert res['OK']
  time.sleep(SLEEP_DELAY)

  # now search
  res = elasticJobParametersDB.getJobParameters(100)
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  assert res['Value'][100]['someKey'] == 'someValue'
  res = elasticJobParametersDB.getJobParameters(100, ['DIRAC', 'someKey'])
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  assert res['Value'][100]['someKey'] == 'someValue'
  res = elasticJobParametersDB.getJobParameters(100, 'DIRAC, someKey')
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  assert res['Value'][100]['someKey'] == 'someValue'

  # another one + search
  res = elasticJobParametersDB.setJobParameter(100, 'someOtherKey', 'someOtherValue')
  assert res['OK']
  time.sleep(SLEEP_DELAY)
  res = elasticJobParametersDB.getJobParameters(100)
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  assert res['Value'][100]['someKey'] == 'someValue'
  assert res['Value'][100]['someOtherKey'] == 'someOtherValue'
  res = elasticJobParametersDB.getJobParameters(100, ['DIRAC', 'someKey', 'someOtherKey'])
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  assert res['Value'][100]['someKey'] == 'someValue'
  assert res['Value'][100]['someOtherKey'] == 'someOtherValue'

  # another job
  res = elasticJobParametersDB.setJobParameter(101, 'DIRAC', 'dirac@cern')
  assert res['OK']
  res = elasticJobParametersDB.setJobParameter(101, 'key101', 'value101')
  assert res['OK']
  res = elasticJobParametersDB.setJobParameter(101, 'someKey', 'value101')
  assert res['OK']
  res = elasticJobParametersDB.setJobParameter(101, 'key101', 'someValue')
  assert res['OK']
  time.sleep(SLEEP_DELAY)
  res = elasticJobParametersDB.getJobParameters(100)
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  assert res['Value'][100]['someKey'] == 'someValue'
  assert res['Value'][100]['someOtherKey'] == 'someOtherValue'
  assert len(res['Value']) == 1
  assert len(res['Value'][100]) == 3
  res = elasticJobParametersDB.getJobParameters(101)
  assert res['OK']
  assert res['Value'][101]['DIRAC'] == 'dirac@cern'
  assert res['Value'][101]['key101'] == 'someValue'
  assert res['Value'][101]['someKey'] == 'value101'
  assert len(res['Value']) == 1
  assert len(res['Value'][101]) == 3
  res = elasticJobParametersDB.setJobParameters(101, [('k', 'v'), ('k1', 'v1'), ('k2', 'v2')])
  assert res['OK']
  time.sleep(SLEEP_DELAY)
  res = elasticJobParametersDB.getJobParameters(101)
  assert res['OK']
  assert res['Value'][101]['DIRAC'] == 'dirac@cern'
  assert res['Value'][101]['k'] == 'v'
  assert res['Value'][101]['k2'] == 'v2'

  # deleting
  res = elasticJobParametersDB.deleteJobParameters(100)
  assert res['OK']
  time.sleep(SLEEP_DELAY)
  res = elasticJobParametersDB.getJobParameters(100)
  assert res['OK']
  assert len(res['Value'][100]) == 0

  res = elasticJobParametersDB.deleteJobParameters(101, 'someKey')
  assert res['OK']
  time.sleep(SLEEP_DELAY)
  res = elasticJobParametersDB.getJobParameters(101)
  assert res['OK']
  assert len(res['Value'][101]) == 5
  res = elasticJobParametersDB.deleteJobParameters(101, 'someKey,key101')  # someKey is already deleted
  assert res['OK']
  time.sleep(SLEEP_DELAY)
  res = elasticJobParametersDB.getJobParameters(101)
  assert res['OK']
  assert len(res['Value'][101]) == 4
  res = elasticJobParametersDB.deleteJobParameters(101, 'nonExistingKey')
  assert res['OK']
  time.sleep(SLEEP_DELAY)
  res = elasticJobParametersDB.getJobParameters(101)
  assert res['OK']
  assert len(res['Value'][101]) == 4

  # delete the index
  res = elasticJobParametersDB.deleteIndex(elasticJobParametersDB.indexName)
