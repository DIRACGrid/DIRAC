""" This integration test only need the TaskQueueDB
    (which should of course be properly defined also in the configuration),
    and connects directly to it


    Run this test with::

        "python -m pytest tests/Integration/WorkloadManagementSystem/Test_TaskQueueDB.py"
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB


gLogger.setLevel('DEBUG')
tqDB = TaskQueueDB()


def test_basicChain():
  """ a basic put - remove
  """
  tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 50000}
  result = tqDB.insertJob(123, tqDefDict, 10)
  assert result['OK'] is True
  result = tqDB.getTaskQueueForJobs([123])
  assert result['OK'] is True
  assert 123 in result['Value']
  tq = result['Value'][123]
  result = tqDB.deleteJob(123)
  assert result['OK'] is True
  result = tqDB.cleanOrphanedTaskQueues()
  assert result['OK'] is True
  result = tqDB.deleteTaskQueueIfEmpty(tq)
  assert result['OK'] is True


def test_chainWithParameter():
  """ put - remove with parameters
  """
  tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 50000}

  # first job
  result = tqDB.insertJob(123, tqDefDict, 10)
  assert result['OK'] is True
  result = tqDB.getTaskQueueForJobs([123])
  assert result['OK'] is True
  tq = result['Value'][123]
  result = tqDB.deleteTaskQueue(tq)
  assert result['OK'] is False  # This will fail because of the foreign key
  result = tqDB.cleanOrphanedTaskQueues()
  assert result['OK'] is True
  result = tqDB.deleteTaskQueueIfEmpty(tq)  # this won't delete anything
  assert result['OK'] is True

  # second job
  result = tqDB.insertJob(125, tqDefDict, 10)
  assert result['OK'] is True
  result = tqDB.getTaskQueueForJobs([125])
  tq = result['Value'][125]
  result = tqDB.deleteTaskQueue(tq)
  assert result['OK'] is False  # This will fail because of the foreign key
  result = tqDB.deleteTaskQueueIfEmpty(tq)  # this won't delete anything, as both 123 and 125 are in
  assert result['OK'] is True  # but still it won't fail
  assert result['Value'] is False
  result = tqDB.retrieveTaskQueues()
  assert result['OK'] is True
  assert result['Value'].values()[0] == {'OwnerDN': '/my/DN', 'Jobs': 2, 'OwnerGroup': 'myGroup',
                                         'Setup': 'aSetup', 'CPUTime': 86400, 'Priority': 1.0}

  # now we will try to delete
  result = tqDB.deleteJob(123)
  assert result['OK'] is True
  result = tqDB.deleteJob(125)
  assert result['OK'] is True
  result = tqDB.deleteTaskQueueIfEmpty(tq)  # this should now delete tq
  assert result['OK'] is True
  result = tqDB.retrieveTaskQueues()
  assert result['OK'] is True
  assert result['Value'] == {}


def test_chainWithParametersComplex():
  """ put - remove with parameters
  """
  tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 50000,
               'BannedSites': ['LCG.CERN.ch', 'CLOUD.IN2P3.fr']}
  result = tqDB.insertJob(127, tqDefDict, 10)
  assert result['OK'] is True
  result = tqDB.getTaskQueueForJobs([127])
  tq = result['Value'][127]
  result = tqDB.deleteTaskQueueIfEmpty(tq)  # this won't delete anything, as 127 is in
  assert result['OK'] is True  # but still it won't fail
  assert result['Value'] is False
  result = tqDB.deleteJob(127)
  assert result['OK'] is True
  result = tqDB.deleteTaskQueueIfEmpty(tq)  # this should now delete tq
  assert result['OK'] is True
  result = tqDB.retrieveTaskQueues()
  assert result['OK'] is True
  assert result['Value'] == {}


""" Various other tests
"""


def test_TQ():
  """ test of various functions
  """
  tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 50000}
  tqDB.insertJob(123, tqDefDict, 10)

  result = tqDB.getNumTaskQueues()
  assert result['OK'] is True
  assert result['Value'] == 1
  result = tqDB.retrieveTaskQueues()
  assert result['OK'] is True
  assert result['Value'].values()[0] == {'OwnerDN': '/my/DN', 'Jobs': 1, 'OwnerGroup': 'myGroup',
                                         'Setup': 'aSetup', 'CPUTime': 86400, 'Priority': 1.0}
  result = tqDB.findOrphanJobs()
  assert result['OK'] is True
  result = tqDB.recalculateTQSharesForAll()
  assert result['OK'] is True

  # this will also remove the job
  result = tqDB.matchAndGetJob({'Setup': 'aSetup', 'CPUTime': 300000})
  assert result['OK'] is True
  assert result['Value']['matchFound'] is True
  assert result['Value']['jobId'] in [123, 125]
  tq = result['Value']['taskQueueId']

  result = tqDB.deleteTaskQueueIfEmpty(tq)
  assert result['OK'] is True
