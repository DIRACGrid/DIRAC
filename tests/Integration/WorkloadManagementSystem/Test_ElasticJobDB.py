""" This tests only need the JobElasticDB, and connects directly to it
"""

import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.ElasticJobDB import ElasticJobDB

#  Add a time delay to allow updating the modified index before querying it.
SLEEP_DELAY = 2

gLogger.setLevel('DEBUG')
elasticJobDB = ElasticJobDB()


def test_setAndGetJobFromDB():
  res = elasticJobDB.setJobParameter(100, 'DIRAC', 'dirac@cern')
  assert res['OK']
  time.sleep(SLEEP_DELAY)

  res = elasticJobDB.getJobParameters(100)
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern'

  # update it
  res = elasticJobDB.setJobParameter(100, 'DIRAC', 'dirac@cern.cern')
  assert res['OK']
  time.sleep(SLEEP_DELAY)
  res = elasticJobDB.getJobParameters(100)
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  res = elasticJobDB.getJobParameters(100, ['DIRAC'])
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  res = elasticJobDB.getJobParameters(100, 'DIRAC')
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'

  # add one
  res = elasticJobDB.setJobParameter(100, 'someKey', 'someValue')
  assert res['OK']
  time.sleep(SLEEP_DELAY)
  res = elasticJobDB.getJobParameters(100)
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  assert res['Value'][100]['someKey'] == 'someValue'
  res = elasticJobDB.getJobParameters(100, ['DIRAC', 'someKey'])
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  assert res['Value'][100]['someKey'] == 'someValue'
  res = elasticJobDB.getJobParameters(100, 'DIRAC, someKey')
  assert res['OK']
  assert res['Value'][100]['DIRAC'] == 'dirac@cern.cern'
  assert res['Value'][100]['someKey'] == 'someValue'
