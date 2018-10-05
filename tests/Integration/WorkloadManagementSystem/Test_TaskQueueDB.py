""" This integration test only need the TaskQueueDB
    (which should of course be properly defined also in the configuration),
    and connects directly to it


    Run this test with::

        "python -m pytest tests/Integration/WorkloadManagementSystem/Test_TaskQueueDB.py"
"""

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


def test_chainWithPlatforms():
  """ put - remove with parameters including a platform
  """

  # We'll try the following case
  #
  # possible platforms: slc5, slc6, centos7, debian, ubuntu
  # where:
  #  - centos7 > slc6 > slc5
  #  - ubuntu > debian
  # and of course what runs on rhel family does not run on debian family

  tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 5000,
               'Platforms': ['centos7']}
  result = tqDB.insertJob(1, tqDefDict, 10)
  assert result['OK'] is True
  result = tqDB.getTaskQueueForJobs([1])
  tq_job1 = result['Value'][1]
  assert tq_job1 > 0

  result = tqDB.insertJob(2, tqDefDict, 10)
  assert result['OK'] is True
  result = tqDB.getTaskQueueForJobs([2])
  tq_job2 = result['Value'][2]
  assert tq_job1 == tq_job2

  tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 5000,
               'Platforms': ['ubuntu']}
  result = tqDB.insertJob(3, tqDefDict, 10)
  assert result['OK'] is True
  result = tqDB.getTaskQueueForJobs([3])
  tq_job3 = result['Value'][3]
  assert tq_job3 == tq_job1 + 1

  tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 5000,
               'Platforms': ['centos7', 'slc6']}
  result = tqDB.insertJob(4, tqDefDict, 10)
  assert result['OK'] is True
  result = tqDB.getTaskQueueForJobs([4])
  tq_job4 = result['Value'][4]
  assert tq_job4 == tq_job3 + 1

  tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 5000,
               'Platforms': ['debian', 'ubuntu']}
  result = tqDB.insertJob(5, tqDefDict, 10)
  assert result['OK'] is True
  result = tqDB.getTaskQueueForJobs([5])
  tq_job5 = result['Value'][5]
  assert tq_job5 == tq_job4 + 1

  # We should be in this situation (TQIds are obviously invented):
  #
  # select TQId, JobId FROM `tq_Jobs`
  # +--------+---------+
  # |   TQId |   JobId |
  # +--------+---------|
  # |    101 |       1 |
  # |    101 |       2 |
  # |    102 |       3 |
  # |    103 |       4 |
  # |    104 |       5 |
  # +--------+---------+
  #
  # select * FROM `tq_TQToPlatforms`
  # +--------+---------+
  # |   TQId | Value   |
  # |--------+---------|
  # |    101 | centos7 |
  # |    102 | ubuntu  |
  # |    103 | centos7 |
  # |    103 | slc6    |
  # |    104 | debian  |
  # |    104 | ubuntu  |
  # +--------+---------+

  # strict matching

  # centos7
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'centos7'},
                                     numQueuesToGet=4)
  assert result['OK'] is True
  # this should match one in [tq_job1, tq_job2, tq_job4]
  assert int(result['Value'][0][0]) in [tq_job1, tq_job2, tq_job4]
  assert int(result['Value'][0][0]) not in [tq_job3, tq_job5]
  assert len(result['Value']) == 2

  # ubuntu
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'ubuntu'},
                                     numQueuesToGet=4)
  assert result['OK'] is True
  # this should match one in [tq_job3, tq_job5]
  assert int(result['Value'][0][0]) in [tq_job3, tq_job5]
  assert int(result['Value'][0][0]) not in [tq_job1, tq_job2, tq_job4]
  assert len(result['Value']) == 2

  # slc6
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'slc6'},
                                     numQueuesToGet=4)
  assert result['OK'] is True
  # this should match only tq_job4, as this is the only one that can run on slc6
  assert int(result['Value'][0][0]) == tq_job4
  assert len(result['Value']) == 1

  # slc5
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'slc5'},
                                     numQueuesToGet=4)
  assert result['OK'] is True
  # this should not match anything
  assert result['Value'] == []

  # compatibility matching

  # ANY
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'ANY'},
                                     numQueuesToGet=4)
  assert result['OK'] is True
  # this should match whatever
  assert int(result['Value'][0][0]) in [tq_job1, tq_job2, tq_job3,
                                        tq_job4, tq_job5]
  assert len(result['Value']) == 4

  # Now we insert a TQ without platform

  tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 5000}
  result = tqDB.insertJob(6, tqDefDict, 10)
  assert result['OK'] is True
  result = tqDB.getTaskQueueForJobs([6])
  tq_job6 = result['Value'][6]
  assert tq_job6 == tq_job5 + 1

  # matching for this one

  # ANY
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'ANY'},
                                     numQueuesToGet=5)
  assert result['OK'] is True
  # this should match whatever
  assert int(result['Value'][0][0]) in [tq_job1, tq_job2, tq_job3,
                                        tq_job4, tq_job5, tq_job6]
  assert len(result['Value']) == 5

  # slc5 -- this time it should match 1 (the one without specified platform)
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'slc5'},
                                     numQueuesToGet=5)
  assert result['OK'] is True
  assert int(result['Value'][0][0]) == tq_job6
  assert len(result['Value']) == 1

  # slc6
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'slc6'},
                                     numQueuesToGet=5)
  assert result['OK'] is True
  assert int(result['Value'][0][0]) in [tq_job4, tq_job6]
  assert len(result['Value']) == 2

  # Now we insert a TQ with platform "ANY" (same as no platform)

  tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 5000,
               'Platform': 'ANY'}
  result = tqDB.insertJob(7, tqDefDict, 10)
  assert result['OK'] is True
  result = tqDB.getTaskQueueForJobs([7])
  tq_job7 = result['Value'][7]
  assert tq_job7 == tq_job6  # would be inserted in the same TQ

  # matching for this one

  # ANY
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'ANY'},
                                     numQueuesToGet=6)
  assert result['OK'] is True
  # this should match whatever
  assert int(result['Value'][0][0]) in [tq_job1, tq_job2, tq_job3,
                                        tq_job4, tq_job5, tq_job6, tq_job7]
  assert len(result['Value']) == 5

  # slc5 -- this time it should match 2
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'slc5'},
                                     numQueuesToGet=6)
  assert result['OK'] is True
  assert int(result['Value'][0][0]) in [tq_job6, tq_job7]
  assert len(result['Value']) == 1

  # slc6
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'slc6'},
                                     numQueuesToGet=6)
  assert result['OK'] is True
  assert int(result['Value'][0][0]) in [tq_job4, tq_job6, tq_job7]
  assert len(result['Value']) == 2

  # new platform appears
  # centos8 (> centos7)
  result = tqDB.matchAndGetTaskQueue({'Setup': 'aSetup', 'CPUTime': 50000,
                                      'Platform': 'centos8'},
                                     numQueuesToGet=5)
  assert result['OK'] is True
  # FIXME: here, I would like to see 3 TQs matched: those for slc6 + centos7 + ANY
  # assert len(result['Value']) == 2
  # but here it returns only 1 (those for ANY), by construction
  # so, this is to be improved

  for jobId in xrange(1, 8):
    result = tqDB.deleteJob(jobId)
    assert result['OK'] is True

  for tqId in [tq_job1, tq_job2, tq_job3,
               tq_job4, tq_job5, tq_job6, tq_job7]:
    result = tqDB.deleteTaskQueueIfEmpty(tqId)
    assert result['OK'] is True


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
