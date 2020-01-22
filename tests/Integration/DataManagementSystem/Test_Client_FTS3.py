""" This is a test of the chain
    FTS3Client -> FTS3ManagerHandler -> FTS3DB

    It supposes that the DB is present, and that the service is running
"""

from __future__ import print_function
import unittest
import time
import sys

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.DataManagementSystem.Client.FTS3Client import FTS3Client
from DIRAC.DataManagementSystem.Client.FTS3Operation import FTS3Operation, FTS3TransferOperation, \
    FTS3StagingOperation
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job

from DIRAC.DataManagementSystem.DB.FTS3DB import FTS3DB


class TestClientFTS3(unittest.TestCase):

  def setUp(self):
    self.client = FTS3Client()
    self.db = FTS3DB()
    self.fileCounter = 0

  def generateOperation(self, opType, nbFiles, dests, sources=None):
    """ Generate one FTS3Operation object with FTS3Files in it"""
    op = None
    if opType == 'Transfer':
      op = FTS3TransferOperation()
    elif opType == 'Staging':
      op = FTS3StagingOperation()
    proxyInfo = getProxyInfo()['Value']
    op.username = proxyInfo['username']
    op.userGroup = proxyInfo['group']
    op.sourceSEs = sources
    for _i in xrange(nbFiles * len(dests)):
      self.fileCounter += 1
      for dest in dests:
        ftsFile = FTS3File()
        ftsFile.lfn = 'lfn%s' % self.fileCounter
        ftsFile.targetSE = dest
        op.ftsFiles.append(ftsFile)

    return op

  def test_01_operation(self):

    op = self.generateOperation('Transfer', 3, ['Target1', 'Target2'], sources=['Source1', 'Source2'])
    self.assertTrue(not op.isTotallyProcessed())

    res = self.client.persistOperation(op)
    self.assertTrue(res['OK'], res)
    opID = res['Value']

    res = self.client.getOperation(opID)
    self.assertTrue(res['OK'])

    op2 = res['Value']

    self.assertTrue(isinstance(op2, FTS3TransferOperation))
    self.assertTrue(not op2.isTotallyProcessed())

    for attr in ['username', 'userGroup', 'sourceSEs']:
      self.assertTrue(getattr(op, attr) == getattr(op2, attr))

    self.assertTrue(len(op.ftsFiles) == len(op2.ftsFiles))

    self.assertTrue(op2.status == FTS3Operation.INIT_STATE)

    fileIds = []
    for ftsFile in op2.ftsFiles:
      fileIds.append(ftsFile.fileID)
      self.assertTrue(ftsFile.status == FTS3File.INIT_STATE)

    # # Testing the limit feature
    # res = self.client.getOperationsWithFilesToSubmit( limit = 0 )
    #
    # self.assertTrue( res['OK'], res )
    # self.assertTrue( not res['Value'] )
    #
    # res = self.client.getOperationsWithFilesToSubmit()
    # self.assertTrue(res['OK'])
    # self.assertTrue( len( res['Value'] ) == 1 )

    # Testing updating the status and error
    fileStatusDict = {}
    for fId in fileIds:
      fileStatusDict[fId] = {'status': 'Finished' if fId % 2 else 'Failed',
                             'error': '' if fId % 2 else 'Tough luck'}

    res = self.db.updateFileStatus(fileStatusDict)
    self.assertTrue(res['OK'], res['Message'])

    res = self.client.getOperation(opID)
    self.assertTrue(res['OK'], res['Message'])
    op3 = res['Value']

    self.assertTrue(op3.ftsFiles)
    for ftsFile in op3.ftsFiles:
      if ftsFile.fileID % 2:
        self.assertTrue(ftsFile.status == 'Finished')
        self.assertTrue(not ftsFile.error)
      else:
        self.assertTrue(ftsFile.status == 'Failed')
        self.assertTrue(ftsFile.error == 'Tough luck')

    self.assertTrue(not op3.isTotallyProcessed())

    # # The operation still should be considered as having files to submit
    # res = self.client.getOperationsWithFilesToSubmit()
    # self.assertTrue(res['OK'])
    # self.assertTrue( len( res['Value'] ) == 1 )

    # Testing updating only the status and to final states
    fileStatusDict = {}
    nbFinalStates = len(FTS3File.FINAL_STATES)
    for fId in fileIds:
      fileStatusDict[fId] = {'status': FTS3File.FINAL_STATES[fId % nbFinalStates]}

    res = self.db.updateFileStatus(fileStatusDict)
    self.assertTrue(res['OK'])

    res = self.client.getOperation(opID)
    self.assertTrue(res['OK'], res['Message'])
    op4 = res['Value']

    self.assertTrue(op4.ftsFiles)
    for ftsFile in op4.ftsFiles:
      if ftsFile.fileID % 2:
        # Files to finished cannot be changed
        self.assertTrue(ftsFile.status == 'Finished')
        self.assertTrue(not ftsFile.error)
      else:
        self.assertTrue(ftsFile.status == FTS3File.FINAL_STATES[ftsFile.fileID % nbFinalStates])
        self.assertTrue(ftsFile.error == 'Tough luck')

    # Now it should be considered as totally processed
    self.assertTrue(op4.isTotallyProcessed())
    res = self.client.persistOperation(op4)

    # # The operation still should not be considered anymore
    # res = self.client.getOperationsWithFilesToSubmit()
    # self.assertTrue(res['OK'])
    # self.assertTrue( not res['Value'] )

    # # The operation should be in Processed state in the DB
    # # testing limit 0
    # res = self.client.getProcessedOperations( limit = 0 )
    # self.assertTrue(res['OK'])
    # self.assertTrue( not res['Value'] )
    #
    # res = self.client.getProcessedOperations()
    # self.assertTrue(res['OK'])
    # self.assertTrue( len( res['Value'] ) == 1 )

  def test_02_job(self):
    op = self.generateOperation('Transfer', 3, ['Target1', 'Target2'], sources=['Source1', 'Source2'])

    job1 = FTS3Job()
    job1.ftsGUID = 'a-random-guid'
    job1.ftsServer = 'fts3'

    job1.username = op.username
    job1.userGroup = op.userGroup

    op.ftsJobs.append(job1)

    res = self.client.persistOperation(op)
    self.assertTrue(res['OK'], res)
    opID = res['Value']

    res = self.client.getOperation(opID)
    self.assertTrue(res['OK'])

    op2 = res['Value']
    self.assertTrue(len(op2.ftsJobs) == 1)
    job2 = op2.ftsJobs[0]
    self.assertTrue(job2.operationID == opID)

    for attr in ['ftsGUID', 'ftsServer', 'username', 'userGroup']:
      self.assertTrue(getattr(job1, attr) == getattr(job2, attr))

  def test_03_job_monitoring_racecondition(self):
    """ We used to have a race condition resulting in duplicated transfers for a file.
        This test reproduces the race condition.

        The scenario is as follow. Operation has two files File1 and File2.
        Job1 is submitted for File1 and File2.
        File1 fails, File2 is still ongoing.
        We submit Job2 for File1.
        Job1 is monitored again, and we update again File1 to failed (because it is so in Job1)
        A Job3 would be created for File1, despite Job2 still running on it.
    """
    op = self.generateOperation('Transfer', 2, ['Target1'])

    job1 = FTS3Job()
    job1.ftsGUID = '03-racecondition-job1'
    job1.ftsServer = 'fts3'

    job1.username = op.username
    job1.userGroup = op.userGroup

    op.ftsJobs.append(job1)

    res = self.client.persistOperation(op)
    self.assertTrue(res['OK'], res['Message'])
    opID = res['Value']

    # Get back the operation to update all the IDs
    res = self.client.getOperation(opID)
    self.assertTrue(res['OK'], res['Message'])
    op = res['Value']

    fileIds = []
    for ftsFile in op.ftsFiles:
      fileIds.append(ftsFile.fileID)

    file1ID = min(fileIds)
    file2ID = max(fileIds)

    # Now we monitor Job1, and find that the first file has failed, the second is still ongoing
    fileStatusDict = {file1ID: {'status': 'Failed', 'error': 'Someone made a boo-boo'},
                      file2ID: {'status': 'Staging'}
                      }

    res = self.db.updateFileStatus(fileStatusDict)
    self.assertTrue(res['OK'])

    # We would then submit a second job
    job2 = FTS3Job()
    job2.ftsGUID = '03-racecondition-job2'
    job2.ftsServer = 'fts3'

    job2.username = op.username
    job2.userGroup = op.userGroup

    op.ftsJobs.append(job2)
    res = self.client.persistOperation(op)

    # Now we monitor Job2 & Job1 (in this order)
    fileStatusDictJob2 = {file1ID: {'status': 'Staging'},
                          }
    res = self.db.updateFileStatus(fileStatusDictJob2)
    self.assertTrue(res['OK'])

    # And in Job1, File1 is (and will remain) failed, while File2 is still ongoing
    fileStatusDictJob1 = {file1ID: {'status': 'Failed', 'error': 'Someone made a boo-boo'},
                          file2ID: {'status': 'Staging'}
                          }
    res = self.db.updateFileStatus(fileStatusDictJob1)
    self.assertTrue(res['OK'])

    # And now this is the problem, because If we check whether this operation still has
    # files to submit, it will tell me yes, while all the files are being taken care of
    res = self.client.getOperation(opID)
    self.assertTrue(res['OK'], res['Message'])
    op = res['Value']

    # isTotallyProcessed does not return S_OK struct
    filesToSubmit = op._getFilesToSubmit()
    self.assertEquals(filesToSubmit, [op.ftsFiles[0]])

  def test_04_job_monitoring_solve_racecondition(self):
    """ We used to have a race condition resulting in duplicated transfers for a file.
        This test reproduces the race condition to make sure it is fixed.
        This test makes sure that the update only happens on files concerned by the job

        The scenario is as follow. Operation has two files File1 and File2.
        Job1 is submitted for File1 and File2.
        File1 fails, File2 is still ongoing.
        We submit Job2 for File1.
        Job1 is monitored again, and we update again File1 to failed (because it is so in Job1)
        A Job3 would be created for File1, dispite Job2 still runing on it.
    """
    op = self.generateOperation('Transfer', 2, ['Target1'])

    job1 = FTS3Job()
    job1GUID = '04-racecondition-job1'
    job1.ftsGUID = job1GUID
    job1.ftsServer = 'fts3'

    job1.username = op.username
    job1.userGroup = op.userGroup

    op.ftsJobs.append(job1)

    # Now, when submitting the job, we specify the ftsGUID to which files are
    # assigned
    for ftsFile in op.ftsFiles:
      ftsFile.ftsGUID = job1GUID

    res = self.client.persistOperation(op)
    self.assertTrue(res['OK'], res['Message'])
    opID = res['Value']

    # Get back the operation to update all the IDs
    res = self.client.getOperation(opID)
    self.assertTrue(res['OK'], res['Message'])
    op = res['Value']

    fileIds = []
    for ftsFile in op.ftsFiles:
      fileIds.append(ftsFile.fileID)

    # Arbitrarilly decide that File1 has the smalled fileID
    file1ID = min(fileIds)
    file2ID = max(fileIds)

    # Now we monitor Job1, and find that the first file has failed, the second is still ongoing
    # And since File1 is in an FTS final status, we set its ftsGUID to None
    fileStatusDict = {file1ID: {'status': 'Failed', 'error': 'Someone made a boo-boo', 'ftsGUID': None},
                      file2ID: {'status': 'Staging'}
                      }

    # And when updating, take care of specifying that you are updating for a given GUID
    res = self.db.updateFileStatus(fileStatusDict, ftsGUID=job1GUID)
    self.assertTrue(res['OK'])

    # We would then submit a second job
    job2 = FTS3Job()
    job2GUID = '04-racecondition-job2'
    job2.ftsGUID = job2GUID
    job2.ftsServer = 'fts3'

    job2.username = op.username
    job2.userGroup = op.userGroup

    op.ftsJobs.append(job2)

    # And do not forget to add the new FTSGUID to File1
    # assigned
    for ftsFile in op.ftsFiles:
      if ftsFile.fileID == file1ID:
        ftsFile.ftsGUID = job2GUID

    res = self.client.persistOperation(op)

    # Now we monitor Job2 & Job1 (in this order)
    fileStatusDictJob2 = {file1ID: {'status': 'Staging'},
                          }

    # Again specify the GUID
    res = self.db.updateFileStatus(fileStatusDictJob2, ftsGUID=job2GUID)
    self.assertTrue(res['OK'])

    # And in Job1, File1 is (and will remain) failed, while File2 is still ongoing
    fileStatusDictJob1 = {file1ID: {'status': 'Failed', 'error': 'Someone made a boo-boo'},
                          file2ID: {'status': 'Staging'}
                          }

    # And thanks to specifying the job GUID, File1 should not be touched !
    res = self.db.updateFileStatus(fileStatusDictJob1, ftsGUID=job1GUID)
    self.assertTrue(res['OK'])

    # And hopefully now there shouldn't be any file to submit
    res = self.client.getOperation(opID)
    self.assertTrue(res['OK'], res['Message'])
    op = res['Value']

    # isTotallyProcessed does not return S_OK struct
    filesToSubmit = op._getFilesToSubmit()
    self.assertEquals(filesToSubmit, [])

  def test_05_cancelNotFoundJob(self):
    """ When a job disappears from the server, we need to cancel it
        and its files.

        The scenario is as follow. Operation has 4 files.
        Job1 is submitted for File1 and File2.
        Job2 is submitted for File3 and File4.
        File1 is finished, and then the job disappears.
        We need to cancel Job1 and File2.
        Job2, File3 and File4 are here to make sure we do not cancel wrongly other files
    """

    op = self.generateOperation('Transfer', 4, ['Target1'])

    job1 = FTS3Job()
    job1GUID = '05-cancelall-job1'
    job1.ftsGUID = job1GUID
    job1.ftsServer = 'fts3'

    job1.username = op.username
    job1.userGroup = op.userGroup

    # assign the GUID to the files
    op.ftsFiles[0].ftsGUID = job1GUID
    op.ftsFiles[1].ftsGUID = job1GUID

    # Pretend

    op.ftsJobs.append(job1)

    job2 = FTS3Job()
    job2GUID = '05-cancelall-job2'
    job2.ftsGUID = job2GUID
    job2.ftsServer = 'fts3'

    job2.username = op.username
    job2.userGroup = op.userGroup

    # assign the GUID to the files
    op.ftsFiles[2].ftsGUID = job2GUID
    op.ftsFiles[3].ftsGUID = job2GUID

    op.ftsJobs.append(job2)

    res = self.db.persistOperation(op)
    self.assertTrue(res['OK'], res['Message'])
    opID = res['Value']

    # Get back the operation to update all the IDs
    res = self.db.getOperation(opID)
    self.assertTrue(res['OK'], res['Message'])
    op = res['Value']

    fileIds = []
    for ftsFile in op.ftsFiles:
      fileIds.append(ftsFile.fileID)

    # Now we monitor Job1, and find that the first file has failed, the second is still ongoing
    # And since File1 is in an FTS final status, we set its ftsGUID to None
    file1ID = op.ftsFiles[0].fileID
    file2ID = op.ftsFiles[1].fileID
    fileStatusDict = {file1ID: {'status': 'Finished', 'ftsGUID': None},
                      file2ID: {'status': 'Staging'}
                      }

    # And when updating, take care of specifying that you are updating for a given GUID
    res = self.db.updateFileStatus(fileStatusDict, ftsGUID=job1GUID)
    self.assertTrue(res['OK'])

    # Now we monitor again, job one, and find out that job1 has disappeared
    # So we cancel the job and the files
    res = self.db.cancelNonExistingJob(opID, job1GUID)
    self.assertTrue(res['OK'])

    # And hopefully now File2 is Canceled, while the others are as they were
    res = self.client.getOperation(opID)
    self.assertTrue(res['OK'], res['Message'])
    op = res['Value']

    self.assertTrue(op.ftsFiles[0].status == 'Finished')
    self.assertTrue(op.ftsFiles[1].status == 'Canceled')
    self.assertTrue(op.ftsFiles[1].ftsGUID is None)
    self.assertTrue(op.ftsFiles[2].status == 'New')
    self.assertTrue(op.ftsFiles[3].status == 'New')

  def _perf(self):

    listOfIds = []

    persistStart = time.time()
    for i in xrange(1000):
      op = self.generateOperation('Transfer', i % 20 + 1, ['Dest1'])
      res = self.db.persistOperation(op)
      self.assertTrue(res['OK'])
      listOfIds.append(res['Value'])

    persistEnd = time.time()

    for opId in listOfIds:
      res = self.db.getOperation(opId)
      self.assertTrue(res['OK'])

    getEnd = time.time()

    print("Generation of 1000 operation and 10500 files %s s, retrival %s s" % (
        persistEnd - persistStart, getEnd - persistEnd))


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientFTS3)

  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
