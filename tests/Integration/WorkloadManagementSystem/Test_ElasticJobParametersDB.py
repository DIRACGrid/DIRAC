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
