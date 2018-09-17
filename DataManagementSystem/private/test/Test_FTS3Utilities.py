""" Test the FTS3Utilities"""
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id $"

import unittest
import mock
import datetime

from DIRAC.DataManagementSystem.private.FTS3Utilities import FTS3JSONDecoder, \
    FTS3Serializable, \
    groupFilesByTarget, \
    generatePossibleTransfersBySources, \
    selectUniqueSourceforTransfers, \
    selectUniqueRandomSource, \
    FTS3ServerPolicy


import json


class FakeClass(FTS3Serializable):
  """ Just a fake class"""
  _attrToSerialize = ['string', 'date', 'dic', 'sub']

  def __init__(self):
    self.string = ''
    self.date = None
    self.dic = {}


class TestFTS3Serialization(unittest.TestCase):
  """ Test the FTS3 JSON serialization mechanizme with FTS3JSONEncoder,
      FTS3JSONDecoder, FTS3Serializable"""

  def test_01_basic(self):
    """ Basic json transfer"""

    obj = FakeClass()
    obj.string = 'tata'
    obj.date = datetime.datetime.utcnow().replace(microsecond=0)
    obj.dic = {'a': 1}
    obj.notSerialized = 'Do not'

    obj2 = json.loads(obj.toJSON(), cls=FTS3JSONDecoder)

    self.assertTrue(obj.string == obj2.string)
    self.assertTrue(obj.date == obj2.date)
    self.assertTrue(obj.dic == obj2.dic)

    self.assertTrue(not hasattr(obj2, 'notSerialized'))

  def test_02_subobjects(self):
    """ Try setting as attribute an object """

    class NonSerializable(object):
      """ Fake class not inheriting from FTS3Serializable"""
      pass

    obj = FakeClass()
    obj.sub = NonSerializable()

    with self.assertRaises(TypeError):
      obj.toJSON()

    obj.sub = FakeClass()
    obj.sub.string = 'pipo'

    obj2 = json.loads(obj.toJSON(), cls=FTS3JSONDecoder)

    self.assertTrue(obj.sub.string == obj2.sub.string)


def mock__checkSourceReplicas(ftsFiles):
  succ = {}
  failed = {}

  for ftsFile in ftsFiles:
    if hasattr(ftsFile, 'fakeAttr_possibleSources'):
      succ[ftsFile.lfn] = dict.fromkeys(getattr(ftsFile, 'fakeAttr_possibleSources'))
    else:
      failed[ftsFile.lfn] = 'No such file or directory'

  return S_OK({'Successful': succ, 'Failed': failed})


class TestFileGrouping(unittest.TestCase):
  """ Testing all the grouping functions of FTS3Utilities
  """

  def setUp(self):
    self.f1 = FTS3File()
    self.f1.fakeAttr_possibleSources = ['Src1', 'Src2']
    self.f1.lfn = 'f1'
    self.f1.targetSE = 'target1'

    self.f2 = FTS3File()
    self.f2.fakeAttr_possibleSources = ['Src2', 'Src3']
    self.f2.lfn = 'f2'
    self.f2.targetSE = 'target2'

    self.f3 = FTS3File()
    self.f3.fakeAttr_possibleSources = ['Src4']
    self.f3.lfn = 'f3'
    self.f3.targetSE = 'target1'

    # File does not exist :-)
    self.f4 = FTS3File()
    self.f4.lfn = 'f4'
    self.f4.targetSE = 'target3'

    self.allFiles = [self.f1, self.f2, self.f3, self.f4]

  def test_01_groupFilesByTarget(self):

    # empty input
    self.assertTrue(groupFilesByTarget([])['Value'] == {})

    res = groupFilesByTarget(self.allFiles)

    self.assertTrue(res['OK'])

    groups = res['Value']

    self.assertTrue(self.f1 in groups['target1'])
    self.assertTrue(self.f2 in groups['target2'])
    self.assertTrue(self.f3 in groups['target1'])
    self.assertTrue(self.f4 in groups['target3'])

  @mock.patch(
      'DIRAC.DataManagementSystem.private.FTS3Utilities._checkSourceReplicas',
      side_effect=mock__checkSourceReplicas)
  def test_02_generatePossibleTransfersBySources(self, _mk_checkSourceReplicas):
    """ Get all the possible sources"""
    # We assume here that they all go to the same target
    res = generatePossibleTransfersBySources(self.allFiles)

    self.assertTrue(res['OK'])
    groups = res['Value']
    self.assertTrue(self.f1 in groups['Src1'])
    self.assertTrue(self.f1 in groups['Src2'])
    self.assertTrue(self.f2 in groups['Src2'])
    self.assertTrue(self.f2 in groups['Src3'])
    self.assertTrue(self.f3 in groups['Src4'])
    self.assertTrue(self.f2 in groups['Src3'])

  @mock.patch(
      'DIRAC.DataManagementSystem.private.FTS3Utilities._checkSourceReplicas',
      side_effect=mock__checkSourceReplicas)
  def test_03_selectUniqueSourceforTransfers(self, _mk_checkSourceReplicas):
    """ Suppose they all go to the same target """

    groupBySource = generatePossibleTransfersBySources(self.allFiles)['Value']

    res = selectUniqueSourceforTransfers(groupBySource)

    self.assertTrue(res['OK'])

    uniqueSources = res['Value']
    # Src1 and Src2 should not be here because f1 and f2 should be taken from Src2
    self.assertTrue(sorted(uniqueSources.keys()) == sorted(['Src2', 'Src4']))
    self.assertTrue(self.f1 in uniqueSources['Src2'])
    self.assertTrue(self.f2 in uniqueSources['Src2'])
    self.assertTrue(self.f3 in uniqueSources['Src4'])

  @mock.patch(
      'DIRAC.DataManagementSystem.private.FTS3Utilities._checkSourceReplicas',
      side_effect=mock__checkSourceReplicas)
  def test_04_selectUniqueRandomSource(self, _mk_checkSourceReplicas):
    """ Suppose they all go to the same target """

    res = selectUniqueRandomSource(self.allFiles)

    self.assertTrue(res['OK'])

    uniqueSources = res['Value']

    # There should be only f1,f2 and f3
    allReturnedFiles = []
    existingFiles = [self.f1, self.f2, self.f3]
    for srcSe, ftsFiles in uniqueSources.iteritems():
      allReturnedFiles.extend(ftsFiles)

    # No files should be duplicated and all files should be there, except the non existing one
    self.assertEqual(len(existingFiles), len(allReturnedFiles))
    self.assertEqual(set(existingFiles), set(allReturnedFiles))

    filesInSrc1 = uniqueSources.get('Src1', [])
    filesInSrc2 = uniqueSources.get('Src2', [])
    filesInSrc3 = uniqueSources.get('Src3', [])

    filesInSrc4 = uniqueSources.get('Src4', [])
    # f1
    self.assertTrue(self.f1 in filesInSrc1 + filesInSrc2)
    self.assertTrue(self.f2 in filesInSrc2 + filesInSrc3)
    self.assertTrue(self.f3 in filesInSrc4)


def mock__failoverServerPolicy(_attempt):
  return "server_0"


def mock__randomServerPolicy(_attempt):
  return "server_0"


def mock__sequenceServerPolicy(_attempt):
  return "server_0"


def mock__OKFTSServerStatus(ftsServer):
  return S_OK(ftsServer)


def mock__ErrorFTSServerStatus(ftsServer):
  return S_ERROR(ftsServer)


class TestFTS3ServerPolicy (unittest.TestCase):
  """ Testing FTS3 ServerPolicy selection """

  def setUp(self):
    self.fakeServerDict = {"server_0": "server0.cern.ch",
                           "server_1": "server1.cern.ch",
                           "server_2": "server2.cern.ch"}

  @mock.patch(
      'DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._getFTSServerStatus',
      side_effect=mock__OKFTSServerStatus)
  @mock.patch(
      'DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._sequenceServerPolicy',
      side_effect=mock__sequenceServerPolicy)
  @mock.patch(
      'DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._randomServerPolicy',
      side_effect=mock__randomServerPolicy)
  @mock.patch(
      'DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._failoverServerPolicy',
      side_effect=mock__failoverServerPolicy)
  def testCorrectServerPolicyIsUsed(
          self,
          mockFailoverFunc,
          mockRandomFunc,
          mockSequenceFunc,
          mockFTSServerStatus):
    " Test correct server policy method is called "

    obj = FTS3ServerPolicy(self.fakeServerDict, "Sequence")
    obj.chooseFTS3Server()
    self.assertTrue(mockSequenceFunc.called)

    obj = FTS3ServerPolicy(self.fakeServerDict, "Random")
    obj.chooseFTS3Server()
    self.assertTrue(mockRandomFunc.called)

    obj = FTS3ServerPolicy(self.fakeServerDict, "Failover")
    obj.chooseFTS3Server()
    self.assertTrue(mockFailoverFunc.called)

    # random policy should be selected for an invalid policy
    obj = FTS3ServerPolicy(self.fakeServerDict, "InvalidPolicy")
    obj.chooseFTS3Server()
    self.assertTrue(mockRandomFunc.called)

  @mock.patch(
      'DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._getFTSServerStatus',
      side_effect=mock__ErrorFTSServerStatus)
  def testFailoverServerPolicy(self, mockFTSServerStatus):
    """ Test if the failover server policy returns server at a given position"""

    obj = FTS3ServerPolicy(self.fakeServerDict, "Failover")
    for i in range(len(self.fakeServerDict)):
      self.assertEquals('server_%d' % i, obj._failoverServerPolicy(i))

  @mock.patch(
      'DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._getFTSServerStatus',
      side_effect=mock__ErrorFTSServerStatus)
  def testSequenceServerPolicy(self, mockFTSServerStatus):
    """ Test if the sequence server policy selects the servers Sequentially """

    obj = FTS3ServerPolicy(self.fakeServerDict, "Sequence")

    for i in range(len(self.fakeServerDict)):
      self.assertEquals('server_%d' % i, obj._sequenceServerPolicy(i))

    self.assertEquals('server_0', obj._sequenceServerPolicy(i))

  @mock.patch(
      'DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy._getFTSServerStatus',
      side_effect=mock__ErrorFTSServerStatus)
  def testRandomServerPolicy(self, mockFTSServerStatus):
    """ Test if the random server policy does not selects the same server multiple times """

    obj = FTS3ServerPolicy(self.fakeServerDict, "Random")
    serverSet = set()

    for i in range(len(self.fakeServerDict)):
      serverSet.add(obj._randomServerPolicy(i))

    self.assertEquals(len(serverSet), len(self.fakeServerDict))


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestFTS3Serialization)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestFileGrouping))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestFTS3ServerPolicy))
  unittest.TextTestRunner(verbosity=2).run(suite)
